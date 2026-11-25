"""Real-time Kafka producer streaming all 19 Synthea tables directly from Synthea memory.

Spawns SyntheaStreamRunner (Java in-memory bridge, zero CSV disk writes), validates and enriches
each record for Microsoft Fabric Eventhouse, routes malformed records to <prefix>.dlq, and
publishes each Synthea table to its own dedicated Apache Kafka topic.
"""

from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
from typing import Any, Callable, Iterable

from confluent_kafka import KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic

from carepulse.fabric_config import (
    build_fabric_producer_config,
    load_env,
    resolve_fabric_credentials,
)
from carepulse.schemas import TABLE_SPECS, build_dlq_record, build_eventhouse_record


def find_java_executable() -> str:
    """Resolve java executable from PATH, JAVA_HOME, or common Windows JDK locations."""
    on_path = shutil.which("java")
    if on_path:
        return on_path

    candidates: list[Path] = []
    java_home = os.environ.get("JAVA_HOME")
    if java_home:
        candidates.append(Path(java_home) / "bin" / "java.exe")

    adoptium_root = Path(r"C:\Program Files\Eclipse Adoptium")
    if adoptium_root.exists():
        for jdk_dir in sorted(adoptium_root.glob("jdk-*"), reverse=True):
            candidates.append(jdk_dir / "bin" / "java.exe")

    for candidate in candidates:
        if candidate.exists():
            return str(candidate)

    raise FileNotFoundError(
        "Could not locate java. Ensure JDK 17+ is installed or set JAVA_HOME."
    )


def ensure_topics(
    bootstrap_servers: str,
    topics: Iterable[str],
    num_partitions: int = 3,
) -> None:
    """Create Kafka topics with Eventhouse-friendly partition defaults if absent."""
    admin = AdminClient({"bootstrap.servers": bootstrap_servers})
    existing = set(admin.list_topics(timeout=10).topics.keys())
    to_create = [
        NewTopic(topic, num_partitions=num_partitions, replication_factor=1)
        for topic in topics
        if topic not in existing
    ]
    if not to_create:
        return

    futures = admin.create_topics(to_create)
    for topic_name, future in futures.items():
        try:
            future.result(timeout=10)
        except KafkaException as exc:
            if "TOPIC_ALREADY_EXISTS" not in str(exc):
                raise RuntimeError(f"Failed creating topic {topic_name}: {exc}") from exc


class KafkaEventPublisher:
    """Publishes validated Eventhouse JSON payloads to dedicated Kafka topics and DLQ."""

    def __init__(
        self,
        producer: Producer | None,
        topic_prefix: str = "synthea",
        target_topic: str | None = None,
        timestamp_mode: str = "current",
        clean_names: bool = False,
        rate_per_sec: float = 25.0,
        dry_run: bool = False,
        on_dry_run_event: Callable[[str, str, dict[str, Any]], None] | None = None,
    ) -> None:
        self.producer = producer
        self.topic_prefix = topic_prefix
        self.target_topic = target_topic
        self.wrap_payload = target_topic is not None
        self.dlq_topic = target_topic or f"{topic_prefix}.dlq"
        self.timestamp_mode = timestamp_mode
        self.clean_names = clean_names
        self.delay_seconds = (1.0 / rate_per_sec) if rate_per_sec > 0 else 0.0
        self.dry_run = dry_run
        self.on_dry_run_event = on_dry_run_event
        self.counts_by_topic: dict[str, int] = {}
        self.delivery_errors = 0
        self.dlq_count = 0

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        if err is not None:
            self.delivery_errors += 1
            print(f"[ERROR] Kafka delivery failed for {msg.topic()}: {err}", file=sys.stderr)

    def _emit_message(self, topic: str, key: str, payload: dict[str, Any]) -> None:
        wire_payload = {"payload": payload} if self.wrap_payload else payload
        if self.dry_run:
            if self.on_dry_run_event:
                self.on_dry_run_event(topic, key, wire_payload)
            else:
                print(f"[DRY-RUN] topic={topic} key={key} payload={json.dumps(wire_payload, ensure_ascii=False)}")
        else:
            if self.producer is None:
                raise RuntimeError("Kafka Producer is required when dry_run is False.")
            encoded = json.dumps(wire_payload, ensure_ascii=False).encode("utf-8")
            key_bytes = key.encode("utf-8") if key else None
            while True:
                try:
                    self.producer.produce(
                        topic=topic,
                        key=key_bytes,
                        value=encoded,
                        on_delivery=self._delivery_callback,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)
            self.producer.poll(0)

        self.counts_by_topic[topic] = self.counts_by_topic.get(topic, 0) + 1

    def process_ndjson_line(self, raw_line: str) -> bool:
        """Parse, validate, and publish one raw NDJSON line, routing failures to DLQ.

        Returns True for valid events and False when routed to DLQ.
        """
        text = raw_line.strip()
        if not text:
            return False

        table_hint = "unknown"
        try:
            envelope = json.loads(text)
            if not isinstance(envelope, dict):
                raise ValueError("NDJSON line must decode to a JSON object")
            table_hint = str(envelope.get("table") or "unknown")
            topic, key, payload = build_eventhouse_record(
                envelope,
                topic_prefix=self.topic_prefix,
                timestamp_mode=self.timestamp_mode,
                clean_names=self.clean_names,
            )
            self._emit_message(self.target_topic or topic, key, payload)
            ok = True
        except Exception as exc:  # noqa: BLE001 — route any malformed record to DLQ without halting stream
            dlq_payload = build_dlq_record(text, table_hint, str(exc))
            self._emit_message(self.dlq_topic, table_hint, dlq_payload)
            self.dlq_count += 1
            ok = False

        if self.delay_seconds > 0:
            time.sleep(self.delay_seconds)
        return ok

    def flush(self) -> None:
        """Flush buffered messages to Kafka."""
        if self.producer is not None and not self.dry_run:
            self.producer.flush(timeout=15)


def _resolve_jar_path(jar_path: Path) -> Path:
    """Resolve a JAR from its explicit path, workspace root, or jars/ directory."""
    if jar_path.exists():
        return jar_path
    for candidate in (Path(jar_path.name), Path("jars") / jar_path.name):
        if candidate.exists():
            return candidate
    return jar_path


def build_bridge_command(args: argparse.Namespace) -> list[str]:
    """Assemble the Java command launching SyntheaStreamRunner in memory."""
    java_exe = find_java_executable()
    bridge_jar = _resolve_jar_path(args.bridge_jar)
    synthea_jar = _resolve_jar_path(args.synthea_jar)
    classpath = f"{bridge_jar}{os.pathsep}{synthea_jar}"
    cmd = [
        java_exe,
        "-cp",
        classpath,
        "synthea.stream.SyntheaStreamRunner",
        "--states",
        args.states,
        "--batch-size",
        str(args.batch_size),
        "--max-batches",
        str(args.max_batches),
        "--years-of-history",
        str(args.years_of_history),
    ]
    if args.seed is not None:
        cmd.extend(["--seed", str(args.seed)])
    if args.no_dedupe_dimensions:
        cmd.append("--no-dedupe-dimensions")
    return cmd


def stream_from_synthea_bridge(
    args: argparse.Namespace,
    publisher: KafkaEventPublisher,
) -> int:
    """Spawn SyntheaStreamRunner and publish its stdout NDJSON lines in real time."""
    bridge_jar = _resolve_jar_path(args.bridge_jar)
    synthea_jar = _resolve_jar_path(args.synthea_jar)
    if not bridge_jar.exists():
        raise FileNotFoundError(f"Missing Java bridge jar: {bridge_jar}")
    if not synthea_jar.exists():
        raise FileNotFoundError(f"Missing Synthea jar: {synthea_jar}")

    cmd = build_bridge_command(args)
    proc = subprocess.Popen(
        cmd,
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL if args.quiet_synthea else None,
        text=True,
        encoding="utf-8",
        bufsize=1,
    )

    total_sent = 0
    stopped_early = False
    try:
        assert proc.stdout is not None
        for line in proc.stdout:
            if not line.strip():
                continue
            publisher.process_ndjson_line(line)
            total_sent += 1
            if args.limit is not None and total_sent >= args.limit:
                stopped_early = True
                break
    except KeyboardInterrupt:
        stopped_early = True
        print("\n[Interrupted] Stopping Synthea stream gracefully...", file=sys.stderr)
    finally:
        publisher.flush()
        if proc.poll() is None:
            proc.terminate()
            try:
                proc.wait(timeout=5)
            except subprocess.TimeoutExpired:
                proc.kill()

    if not stopped_early and proc.returncode not in (0, None):
        stderr_out = proc.stderr.read() if proc.stderr else ""
        raise RuntimeError(f"Synthea bridge exited with code {proc.returncode}: {stderr_out}")
    return total_sent


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the real-time 19-table Kafka producer."""
    parser = argparse.ArgumentParser(
        description="Stream all 19 Synthea tables in real-time from memory to Apache Kafka for Fabric Eventhouse."
    )
    parser.add_argument("--bootstrap-servers", default="localhost:9092", help="Kafka bootstrap servers.")
    parser.add_argument("--topic-prefix", default="synthea", help="Kafka topic prefix (<prefix>.<table>).")
    parser.add_argument("--partitions", type=int, default=3, help="Partition count when auto-creating topics.")
    parser.add_argument(
        "--states",
        default="Massachusetts,California,Texas,New York,Florida",
        help="Comma-separated US states rotated across live micro-batches.",
    )
    parser.add_argument("--batch-size", type=int, default=1, help="Living patients simulated per live micro-batch.")
    parser.add_argument("--max-batches", type=int, default=0, help="Stop after N micro-batches (0 = run continuously).")
    parser.add_argument("--years-of-history", type=int, default=10, help="Synthea history window in years.")
    parser.add_argument("--seed", type=int, default=None, help="Optional starting seed for reproducible streams.")
    parser.add_argument(
        "--no-dedupe-dimensions",
        action="store_true",
        help="Re-emit unchanged organizations/providers/payers on every batch.",
    )
    parser.add_argument("--rate", type=float, default=25.0, help="Max events per second (0 = unlimited).")
    parser.add_argument("--limit", type=int, default=None, help="Stop after publishing N total events.")
    parser.add_argument(
        "--timestamp-mode",
        choices=("current", "original"),
        default="current",
        help="Stamp event_time with live UTC time ('current') or Synthea clinical time ('original').",
    )
    parser.add_argument(
        "--clean-names",
        action="store_true",
        help="Strip trailing synthetic digits from person names (e.g. Tracy345 -> Tracy).",
    )
    parser.add_argument("--quiet-synthea", action="store_true", help="Suppress Synthea JVM stderr progress logs.")
    parser.add_argument("--dry-run", action="store_true", help="Validate and print events without connecting to Kafka.")
    parser.add_argument(
        "--to-fabric",
        action="store_true",
        help="Publish directly to Microsoft Fabric Eventstream Custom Endpoint (reads FABRIC_CONNECTION_STRING from .env).",
    )
    parser.add_argument(
        "--fabric-connection-string",
        default=None,
        help="Fabric Eventstream SAS connection string (implies --to-fabric).",
    )
    parser.add_argument(
        "--fabric-topic",
        default=None,
        help="Optional override for Fabric Eventstream topic (EntityPath).",
    )
    parser.add_argument(
        "--bridge-jar",
        type=Path,
        default=Path("synthea-stream-bridge.jar"),
        help="Path to compiled synthea-stream-bridge.jar.",
    )
    parser.add_argument(
        "--synthea-jar",
        type=Path,
        default=Path("synthea-with-dependencies.jar"),
        help="Path to synthea-with-dependencies.jar.",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    """Entry point for real-time Synthea-to-Kafka streaming."""
    load_env()
    args = parse_args(argv)
    all_topics = [f"{args.topic_prefix}.{name}" for name in TABLE_SPECS] + [f"{args.topic_prefix}.dlq"]

    use_fabric = bool(args.to_fabric or args.fabric_connection_string)
    target_topic: str | None = None

    producer: Producer | None = None
    if use_fabric:
        conn_str, override_topic = resolve_fabric_credentials(
            args.fabric_connection_string,
            args.fabric_topic,
        )
        if not conn_str:
            if args.dry_run:
                target_topic = override_topic or "fabric_dry_run_topic"
            else:
                raise SystemExit(
                    "Missing Fabric connection string. Set FABRIC_CONNECTION_STRING in .env or pass --fabric-connection-string."
                )
        else:
            fabric_conf, target_topic = build_fabric_producer_config(conn_str, override_topic=override_topic)
            if not args.dry_run:
                producer = Producer(fabric_conf)
    elif not args.dry_run:
        ensure_topics(args.bootstrap_servers, all_topics, num_partitions=args.partitions)
        producer = Producer(
            {
                "bootstrap.servers": args.bootstrap_servers,
                "acks": "all",
                "enable.idempotence": True,
                "linger.ms": 10,
                "compression.type": "lz4",
                "retries": 5,
            }
        )

    publisher = KafkaEventPublisher(
        producer=producer,
        topic_prefix=args.topic_prefix,
        target_topic=target_topic,
        timestamp_mode=args.timestamp_mode,
        clean_names=args.clean_names,
        rate_per_sec=args.rate,
        dry_run=args.dry_run,
    )

    total_sent = stream_from_synthea_bridge(args, publisher)
    print(
        f"Summary: total_processed={total_sent}, dlq_count={publisher.dlq_count}, "
        f"delivery_errors={publisher.delivery_errors}, "
        f"topics_active={len(publisher.counts_by_topic)}, "
        f"by_topic={json.dumps(publisher.counts_by_topic)}",
        file=sys.stderr,
    )
    return 1 if publisher.delivery_errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
