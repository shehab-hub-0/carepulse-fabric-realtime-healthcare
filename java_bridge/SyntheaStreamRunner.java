package synthea.stream;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintStream;
import java.lang.reflect.Field;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.mitre.synthea.engine.Generator;
import org.mitre.synthea.export.CSVConstants;
import org.mitre.synthea.export.CSVExporter;
import org.mitre.synthea.export.CSVFileManager;
import org.mitre.synthea.export.Exporter;
import org.mitre.synthea.helpers.Config;
import org.mitre.synthea.world.agents.PayerManager;
import org.mitre.synthea.world.agents.Provider;

/**
 * Runs Synthea continuously in-memory and streams all 19 Synthea tables as NDJSON to stdout.
 *
 * <p>All Synthea console logs are redirected to stderr so stdout carries strictly one JSON record
 * per line with zero CSV files created on disk.
 */
public final class SyntheaStreamRunner {

    private static final List<String> EMIT_ORDER = List.of(
        "organizations",
        "providers",
        "payers",
        "patients",
        "payer_transitions",
        "encounters",
        "conditions",
        "allergies",
        "careplans",
        "medications",
        "procedures",
        "immunizations",
        "observations",
        "imaging_studies",
        "devices",
        "supplies",
        "claims",
        "claims_transactions",
        "patient_expenses"
    );

    private static final List<String> DIMENSION_TABLES = List.of(
        "organizations",
        "providers",
        "payers"
    );

    private SyntheaStreamRunner() {}

    public static void main(String[] args) throws Exception {
        RunnerConfig cfg = RunnerConfig.parse(args);

        PrintStream ndjsonOut = new PrintStream(System.out, true, StandardCharsets.UTF_8);
        System.setOut(System.err);

        Path noopDir = Files.createTempDirectory("synthea_inmem_");
        configureSynthea(noopDir, cfg.yearsOfHistory, cfg.batchSize);

        InMemoryStreamCSVFileManager streamManager = new InMemoryStreamCSVFileManager();
        installInMemoryFileManager(streamManager);
        deleteDirectoryQuietly(noopDir);

        Gson gson = new GsonBuilder().serializeNulls().disableHtmlEscaping().create();
        Map<String, String> lastDimensionHash = new HashMap<>();
        AtomicLong sequence = new AtomicLong(0);

        long batchId = 0;
        int stateIdx = 0;
        String previousState = null;
        while (cfg.maxBatches == 0 || batchId < cfg.maxBatches) {
            batchId++;
            String state = cfg.states.get(stateIdx % cfg.states.size());
            stateIdx++;

            // Reset static provider/payer registries when switching states so national payers (e.g., ID 10000) reload cleanly.
            if (previousState != null && !state.equals(previousState)) {
                Provider.clear();
                PayerManager.clear();
            }
            previousState = state;

            long seed = cfg.seed != null
                ? cfg.seed + batchId - 1
                : System.nanoTime() ^ (batchId * 0x9E3779B97F4A7C15L);

            Generator.GeneratorOptions options = new Generator.GeneratorOptions();
            options.population = cfg.batchSize;
            options.state = state;
            options.seed = seed;
            options.clinicianSeed = seed;
            options.threadPoolSize = Math.min(cfg.batchSize, Runtime.getRuntime().availableProcessors());

            Exporter.ExporterRuntimeOptions runtimeOptions = new Exporter.ExporterRuntimeOptions();
            runtimeOptions.yearsOfHistory = cfg.yearsOfHistory;

            Generator generator = new Generator(options, runtimeOptions);
            generator.run();

            Map<String, List<Map<String, String>>> batchRows = streamManager.drainBatch();
            emitBatch(ndjsonOut, gson, batchId, state, batchRows, lastDimensionHash, sequence, cfg.dedupeDimensions);
        }
    }

    private static void configureSynthea(Path noopBaseDir, int yearsOfHistory, int batchSize) {
        Config.set("exporter.baseDirectory", noopBaseDir.toAbsolutePath().toString());
        Config.set("exporter.years_of_history", Integer.toString(yearsOfHistory));
        Config.set("generate.default_population", Integer.toString(batchSize));

        Config.set("exporter.csv.export", "true");
        Config.set("exporter.csv.included_files", "");
        Config.set("exporter.csv.excluded_files", "");
        Config.set("exporter.csv.append_mode", "false");
        Config.set("exporter.csv.folder_per_run", "false");
        Config.set("exporter.csv.include_quality_of_life_observations", "true");

        Config.set("exporter.fhir.export", "false");
        Config.set("exporter.fhir_stu3.export", "false");
        Config.set("exporter.fhir_dstu2.export", "false");
        Config.set("exporter.hospital.fhir.export", "false");
        Config.set("exporter.hospital.fhir_stu3.export", "false");
        Config.set("exporter.hospital.fhir_dstu2.export", "false");
        Config.set("exporter.practitioner.fhir.export", "false");
        Config.set("exporter.practitioner.fhir_stu3.export", "false");
        Config.set("exporter.practitioner.fhir_dstu2.export", "false");
        Config.set("exporter.groups.fhir.export", "false");
        Config.set("exporter.ccda.export", "false");
        Config.set("exporter.text.export", "false");
        Config.set("exporter.clinical_note.export", "false");
        Config.set("exporter.json.export", "false");
        Config.set("exporter.metadata.export", "false");
        Config.set("exporter.bfd.export", "false");
        Config.set("exporter.cdw.export", "false");
        Config.set("exporter.symptoms.csv.export", "false");
        Config.set("exporter.symptoms.text.export", "false");
        Config.set("exporter.enable_custom_exporters", "false");
    }

    private static void installInMemoryFileManager(InMemoryStreamCSVFileManager manager) throws Exception {
        CSVExporter exporter = CSVExporter.getInstance();
        Field fmField = CSVExporter.class.getDeclaredField("fileManager");
        fmField.setAccessible(true);
        fmField.set(exporter, manager);
    }

    private static void emitBatch(
        PrintStream out,
        Gson gson,
        long batchId,
        String state,
        Map<String, List<Map<String, String>>> batchRows,
        Map<String, String> lastDimensionHash,
        AtomicLong sequence,
        boolean dedupeDimensions
    ) {
        for (String table : EMIT_ORDER) {
            List<Map<String, String>> rows = batchRows.get(table);
            if (rows == null || rows.isEmpty()) {
                continue;
            }
            boolean isDimension = DIMENSION_TABLES.contains(table);
            for (Map<String, String> row : rows) {
                if (isDimension && dedupeDimensions) {
                    String dimId = row.getOrDefault("id", "");
                    String snapshot = row.toString();
                    String key = table + ":" + dimId;
                    if (!dimId.isEmpty() && snapshot.equals(lastDimensionHash.get(key))) {
                        continue;
                    }
                    lastDimensionHash.put(key, snapshot);
                }

                Map<String, Object> envelope = new LinkedHashMap<>();
                envelope.put("table", table);
                envelope.put("batch_id", batchId);
                envelope.put("seq", sequence.incrementAndGet());
                envelope.put("state", state);
                envelope.put("data", row);
                out.println(gson.toJson(envelope));
            }
        }
        out.flush();
    }

    private static void deleteDirectoryQuietly(Path dir) {
        try {
            if (Files.exists(dir)) {
                try (var walk = Files.walk(dir)) {
                    walk.sorted(Comparator.reverseOrder()).forEach(path -> {
                        try {
                            Files.deleteIfExists(path);
                        } catch (IOException ignored) {
                        }
                    });
                }
            }
        } catch (IOException ignored) {
        }
    }

    private static final class RunnerConfig {
        final List<String> states;
        final int batchSize;
        final long maxBatches;
        final int yearsOfHistory;
        final Long seed;
        final boolean dedupeDimensions;

        private RunnerConfig(
            List<String> states,
            int batchSize,
            long maxBatches,
            int yearsOfHistory,
            Long seed,
            boolean dedupeDimensions
        ) {
            this.states = states;
            this.batchSize = batchSize;
            this.maxBatches = maxBatches;
            this.yearsOfHistory = yearsOfHistory;
            this.seed = seed;
            this.dedupeDimensions = dedupeDimensions;
        }

        static RunnerConfig parse(String[] args) {
            List<String> states = List.of("Massachusetts");
            int batchSize = 1;
            long maxBatches = 0;
            int yearsOfHistory = 10;
            Long seed = null;
            boolean dedupeDimensions = true;

            for (int i = 0; i < args.length; i++) {
                String arg = args[i];
                if ("--states".equals(arg) && i + 1 < args.length) {
                    String raw = args[++i];
                    List<String> parsed = new ArrayList<>();
                    for (String part : raw.split(",")) {
                        String trimmed = part.trim();
                        if (!trimmed.isEmpty()) {
                            parsed.add(trimmed);
                        }
                    }
                    if (!parsed.isEmpty()) {
                        states = parsed;
                    }
                } else if ("--batch-size".equals(arg) && i + 1 < args.length) {
                    batchSize = Math.max(1, Integer.parseInt(args[++i]));
                } else if ("--max-batches".equals(arg) && i + 1 < args.length) {
                    maxBatches = Math.max(0L, Long.parseLong(args[++i]));
                } else if ("--years-of-history".equals(arg) && i + 1 < args.length) {
                    yearsOfHistory = Math.max(0, Integer.parseInt(args[++i]));
                } else if ("--seed".equals(arg) && i + 1 < args.length) {
                    seed = Long.parseLong(args[++i]);
                } else if ("--no-dedupe-dimensions".equals(arg)) {
                    dedupeDimensions = false;
                }
            }
            return new RunnerConfig(states, batchSize, maxBatches, yearsOfHistory, seed, dedupeDimensions);
        }
    }

    /**
     * In-memory CSVFileManager capturing all lines written by CSVExporter without touching disk.
     */
    private static final class InMemoryStreamCSVFileManager extends CSVFileManager {

        private final Map<String, List<String>> headersByTable = new HashMap<>();
        private final Map<String, List<Map<String, String>>> currentBatch = new LinkedHashMap<>();
        private final Map<String, OutputStreamWriter> writers = new HashMap<>();

        InMemoryStreamCSVFileManager() {
            super();
            for (Map.Entry<String, String> entry : CSVConstants.HEADER_LINE_MAP.entrySet()) {
                List<String> cols = parseCsvLine(entry.getValue().trim());
                List<String> lowerCols = new ArrayList<>(cols.size());
                for (String col : cols) {
                    lowerCols.add(col.trim().toLowerCase(Locale.ROOT));
                }
                headersByTable.put(entry.getKey(), lowerCols);
            }
        }

        @Override
        public synchronized OutputStreamWriter getWriter(String resourceKey) {
            return writers.computeIfAbsent(resourceKey, key -> new OutputStreamWriter(
                new LineRoutingOutputStream(key),
                StandardCharsets.UTF_8
            ));
        }

        @Override
        public void flushWriter(String resourceKey) {
            // In-memory stream flushes per batch at batch completion.
        }

        @Override
        public void writeResourceLine(String line, String resourceKey) {
            recordLine(resourceKey, line);
        }

        synchronized void recordLine(String resourceKey, String rawLine) {
            if (rawLine == null) {
                return;
            }
            String trimmed = stripTrailingNewlines(rawLine);
            if (trimmed.isEmpty()) {
                return;
            }
            List<String> headers = headersByTable.get(resourceKey);
            if (headers == null) {
                return;
            }
            List<String> values = parseCsvLine(trimmed);
            Map<String, String> row = new LinkedHashMap<>();
            for (int i = 0; i < headers.size(); i++) {
                String val = i < values.size() ? values.get(i).trim() : "";
                row.put(headers.get(i), val.isEmpty() ? null : val);
            }
            currentBatch.computeIfAbsent(resourceKey, k -> new ArrayList<>()).add(row);
        }

        synchronized Map<String, List<Map<String, String>>> drainBatch() {
            Map<String, List<Map<String, String>>> drained = new LinkedHashMap<>(currentBatch);
            currentBatch.clear();
            return drained;
        }

        private final class LineRoutingOutputStream extends OutputStream {
            private final String resourceKey;
            private final ByteArrayOutputStream lineBuf = new ByteArrayOutputStream();

            LineRoutingOutputStream(String resourceKey) {
                this.resourceKey = resourceKey;
            }

            @Override
            public void write(int b) {
                if (b == '\n') {
                    String line = lineBuf.toString(StandardCharsets.UTF_8);
                    lineBuf.reset();
                    recordLine(resourceKey, line);
                } else if (b != '\r') {
                    lineBuf.write(b);
                }
            }
        }

        private static String stripTrailingNewlines(String value) {
            int end = value.length();
            while (end > 0 && (value.charAt(end - 1) == '\n' || value.charAt(end - 1) == '\r')) {
                end--;
            }
            return value.substring(0, end);
        }

        private static List<String> parseCsvLine(String line) {
            List<String> fields = new ArrayList<>();
            StringBuilder current = new StringBuilder();
            boolean inQuotes = false;
            for (int i = 0; i < line.length(); i++) {
                char c = line.charAt(i);
                if (inQuotes) {
                    if (c == '"') {
                        if (i + 1 < line.length() && line.charAt(i + 1) == '"') {
                            current.append('"');
                            i++;
                        } else {
                            inQuotes = false;
                        }
                    } else {
                        current.append(c);
                    }
                } else if (c == '"') {
                    inQuotes = true;
                } else if (c == ',') {
                    fields.add(current.toString());
                    current.setLength(0);
                } else {
                    current.append(c);
                }
            }
            fields.add(current.toString());
            return fields;
        }
    }
}
