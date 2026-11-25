"""Declarative schemas, cleaning/normalization rules, and Eventhouse KQL generator for all 19 Synthea tables."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import json
import re
from typing import Any

ENVELOPE_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_id", "string"),
    ("event_type", "string"),
    ("event_time", "datetime"),
    ("clinical_time", "datetime"),
    ("batch_id", "long"),
    ("seq", "long"),
    ("sim_state", "string"),
    ("schema_version", "string"),
    ("source_system", "string"),
)

_SYNTHEA_NAME_DIGITS_RE = re.compile(r"\d+$")


@dataclass(frozen=True)
class TableSpec:
    """Schema and normalization rules for one Synthea stream table."""

    name: str
    columns: tuple[tuple[str, str], ...]
    key_fields: tuple[str, ...]
    clinical_time_fields: tuple[str, ...] = ()
    defaults: dict[str, Any] = field(default_factory=dict)
    person_name_fields: tuple[str, ...] = ()


TABLE_SPECS: dict[str, TableSpec] = {
    "patients": TableSpec(
        name="patients",
        key_fields=("id",),
        clinical_time_fields=("birthdate",),
        person_name_fields=("first", "middle", "last", "maiden"),
        columns=(
            ("id", "string"),
            ("birthdate", "datetime"),
            ("deathdate", "datetime"),
            ("ssn", "string"),
            ("drivers", "string"),
            ("passport", "string"),
            ("prefix", "string"),
            ("first", "string"),
            ("middle", "string"),
            ("last", "string"),
            ("suffix", "string"),
            ("maiden", "string"),
            ("marital", "string"),
            ("race", "string"),
            ("ethnicity", "string"),
            ("gender", "string"),
            ("birthplace", "string"),
            ("address", "string"),
            ("city", "string"),
            ("state", "string"),
            ("county", "string"),
            ("fips", "string"),
            ("zip", "string"),
            ("lat", "real"),
            ("lon", "real"),
            ("healthcare_expenses", "real"),
            ("healthcare_coverage", "real"),
            ("income", "long"),
        ),
    ),
    "organizations": TableSpec(
        name="organizations",
        key_fields=("id",),
        columns=(
            ("id", "string"),
            ("name", "string"),
            ("address", "string"),
            ("city", "string"),
            ("state", "string"),
            ("zip", "string"),
            ("lat", "real"),
            ("lon", "real"),
            ("phone", "string"),
            ("revenue", "real"),
            ("utilization", "long"),
            ("npi", "string"),
        ),
    ),
    "providers": TableSpec(
        name="providers",
        key_fields=("id",),
        person_name_fields=("name",),
        columns=(
            ("id", "string"),
            ("organization", "string"),
            ("name", "string"),
            ("gender", "string"),
            ("speciality", "string"),
            ("address", "string"),
            ("city", "string"),
            ("state", "string"),
            ("zip", "string"),
            ("lat", "real"),
            ("lon", "real"),
            ("encounters", "long"),
            ("procedures", "long"),
            ("npi", "string"),
        ),
    ),
    "payers": TableSpec(
        name="payers",
        key_fields=("id",),
        columns=(
            ("id", "string"),
            ("name", "string"),
            ("ownership", "string"),
            ("address", "string"),
            ("city", "string"),
            ("state_headquartered", "string"),
            ("zip", "string"),
            ("phone", "string"),
            ("amount_covered", "real"),
            ("amount_uncovered", "real"),
            ("revenue", "real"),
            ("covered_encounters", "long"),
            ("uncovered_encounters", "long"),
            ("covered_medications", "long"),
            ("uncovered_medications", "long"),
            ("covered_procedures", "long"),
            ("uncovered_procedures", "long"),
            ("covered_immunizations", "long"),
            ("uncovered_immunizations", "long"),
            ("unique_customers", "long"),
            ("qols_avg", "real"),
            ("member_months", "long"),
        ),
    ),
    "payer_transitions": TableSpec(
        name="payer_transitions",
        key_fields=("patient",),
        clinical_time_fields=("start_date",),
        person_name_fields=("owner_name",),
        columns=(
            ("patient", "string"),
            ("memberid", "string"),
            ("start_date", "datetime"),
            ("end_date", "datetime"),
            ("payer", "string"),
            ("secondary_payer", "string"),
            ("plan_ownership", "string"),
            ("owner_name", "string"),
        ),
    ),
    "encounters": TableSpec(
        name="encounters",
        key_fields=("patient", "id"),
        clinical_time_fields=("start",),
        columns=(
            ("id", "string"),
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("organization", "string"),
            ("provider", "string"),
            ("payer", "string"),
            ("encounterclass", "string"),
            ("code", "string"),
            ("description", "string"),
            ("base_encounter_cost", "real"),
            ("total_claim_cost", "real"),
            ("payer_coverage", "real"),
            ("reasoncode", "string"),
            ("reasondescription", "string"),
        ),
    ),
    "conditions": TableSpec(
        name="conditions",
        key_fields=("patient",),
        clinical_time_fields=("start",),
        columns=(
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("system", "string"),
            ("code", "string"),
            ("description", "string"),
        ),
    ),
    "allergies": TableSpec(
        name="allergies",
        key_fields=("patient",),
        clinical_time_fields=("start",),
        columns=(
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("system", "string"),
            ("description", "string"),
            ("type", "string"),
            ("category", "string"),
            ("reaction1", "string"),
            ("description1", "string"),
            ("severity1", "string"),
            ("reaction2", "string"),
            ("description2", "string"),
            ("severity2", "string"),
        ),
    ),
    "careplans": TableSpec(
        name="careplans",
        key_fields=("patient", "id"),
        clinical_time_fields=("start",),
        columns=(
            ("id", "string"),
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("description", "string"),
            ("reasoncode", "string"),
            ("reasondescription", "string"),
        ),
    ),
    "medications": TableSpec(
        name="medications",
        key_fields=("patient",),
        clinical_time_fields=("start",),
        columns=(
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("payer", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("description", "string"),
            ("base_cost", "real"),
            ("payer_coverage", "real"),
            ("dispenses", "long"),
            ("totalcost", "real"),
            ("reasoncode", "string"),
            ("reasondescription", "string"),
        ),
    ),
    "procedures": TableSpec(
        name="procedures",
        key_fields=("patient",),
        clinical_time_fields=("start",),
        columns=(
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("system", "string"),
            ("code", "string"),
            ("description", "string"),
            ("base_cost", "real"),
            ("reasoncode", "string"),
            ("reasondescription", "string"),
        ),
    ),
    "immunizations": TableSpec(
        name="immunizations",
        key_fields=("patient",),
        clinical_time_fields=("date",),
        columns=(
            ("date", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("description", "string"),
            ("base_cost", "real"),
        ),
    ),
    "observations": TableSpec(
        name="observations",
        key_fields=("patient",),
        clinical_time_fields=("date",),
        defaults={"category": "uncategorized"},
        columns=(
            ("date", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("category", "string"),
            ("code", "string"),
            ("description", "string"),
            ("value", "string"),
            ("numeric_value", "real"),
            ("units", "string"),
            ("type", "string"),
        ),
    ),
    "imaging_studies": TableSpec(
        name="imaging_studies",
        key_fields=("patient", "id"),
        clinical_time_fields=("date",),
        columns=(
            ("id", "string"),
            ("date", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("series_uid", "string"),
            ("bodysite_code", "string"),
            ("bodysite_description", "string"),
            ("modality_code", "string"),
            ("modality_description", "string"),
            ("instance_uid", "string"),
            ("sop_code", "string"),
            ("sop_description", "string"),
            ("procedure_code", "string"),
        ),
    ),
    "devices": TableSpec(
        name="devices",
        key_fields=("patient",),
        clinical_time_fields=("start",),
        columns=(
            ("start", "datetime"),
            ("stop", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("description", "string"),
            ("udi", "string"),
        ),
    ),
    "supplies": TableSpec(
        name="supplies",
        key_fields=("patient",),
        clinical_time_fields=("date",),
        columns=(
            ("date", "datetime"),
            ("patient", "string"),
            ("encounter", "string"),
            ("code", "string"),
            ("description", "string"),
            ("quantity", "long"),
        ),
    ),
    "claims": TableSpec(
        name="claims",
        key_fields=("patientid", "id"),
        clinical_time_fields=("servicedate", "currentillnessdate"),
        columns=(
            ("id", "string"),
            ("patientid", "string"),
            ("providerid", "string"),
            ("primarypatientinsuranceid", "string"),
            ("secondarypatientinsuranceid", "string"),
            ("departmentid", "long"),
            ("patientdepartmentid", "long"),
            ("diagnosis1", "string"),
            ("diagnosis2", "string"),
            ("diagnosis3", "string"),
            ("diagnosis4", "string"),
            ("diagnosis5", "string"),
            ("diagnosis6", "string"),
            ("diagnosis7", "string"),
            ("diagnosis8", "string"),
            ("referringproviderid", "string"),
            ("appointmentid", "string"),
            ("currentillnessdate", "datetime"),
            ("servicedate", "datetime"),
            ("supervisingproviderid", "string"),
            ("status1", "string"),
            ("status2", "string"),
            ("statusp", "string"),
            ("outstanding1", "real"),
            ("outstanding2", "real"),
            ("outstandingp", "real"),
            ("lastbilleddate1", "datetime"),
            ("lastbilleddate2", "datetime"),
            ("lastbilleddatep", "datetime"),
            ("healthcareclaimtypeid1", "string"),
            ("healthcareclaimtypeid2", "string"),
        ),
    ),
    "claims_transactions": TableSpec(
        name="claims_transactions",
        key_fields=("patientid", "claimid", "id"),
        clinical_time_fields=("fromdate", "todate"),
        columns=(
            ("id", "string"),
            ("claimid", "string"),
            ("chargeid", "long"),
            ("patientid", "string"),
            ("type", "string"),
            ("amount", "real"),
            ("method", "string"),
            ("fromdate", "datetime"),
            ("todate", "datetime"),
            ("placeofservice", "string"),
            ("procedurecode", "string"),
            ("modifier1", "string"),
            ("modifier2", "string"),
            ("diagnosisref1", "string"),
            ("diagnosisref2", "string"),
            ("diagnosisref3", "string"),
            ("diagnosisref4", "string"),
            ("units", "real"),
            ("departmentid", "long"),
            ("notes", "string"),
            ("unitamount", "real"),
            ("transferoutid", "string"),
            ("transfertype", "string"),
            ("payments", "real"),
            ("adjustments", "real"),
            ("transfers", "real"),
            ("outstanding", "real"),
            ("appointmentid", "string"),
            ("linenote", "string"),
            ("patientinsuranceid", "string"),
            ("feescheduleid", "string"),
            ("providerid", "string"),
            ("supervisingproviderid", "string"),
        ),
    ),
    "patient_expenses": TableSpec(
        name="patient_expenses",
        key_fields=("patient_id",),
        clinical_time_fields=("year",),
        columns=(
            ("patient_id", "string"),
            ("year", "datetime"),
            ("payer_id", "string"),
            ("healthcare_expenses", "real"),
            ("insurance_costs", "real"),
            ("covered_costs", "real"),
        ),
    ),
}

DLQ_COLUMNS: tuple[tuple[str, str], ...] = (
    ("event_time", "datetime"),
    ("source_table", "string"),
    ("error_reason", "string"),
    ("raw_payload", "string"),
)


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


def _normalize_datetime_str(raw: Any) -> str | None:
    if raw is None:
        return None
    text = str(raw).strip()
    if not text:
        return None
    if len(text) == 10 and text[4] == "-" and text[7] == "-":
        return f"{text}T00:00:00Z"
    return text


def _strip_synthea_person_digits(name: str) -> str:
    tokens = [_SYNTHEA_NAME_DIGITS_RE.sub("", part) or part for part in name.split()]
    return " ".join(tokens)


def _coerce_typed_value(raw: Any, kql_type: str, field_name: str) -> Any:
    if raw is None:
        return None
    text = str(raw).strip()
    if text == "":
        return None
    if kql_type == "long":
        try:
            return int(float(text))
        except ValueError as exc:
            raise ValueError(f"Invalid long value for '{field_name}': {text!r}") from exc
    if kql_type == "real":
        try:
            return float(text)
        except ValueError as exc:
            raise ValueError(f"Invalid real value for '{field_name}': {text!r}") from exc
    if kql_type == "datetime":
        normalized = _normalize_datetime_str(text)
        if normalized is None:
            return None
        # Validate ISO-8601 shape before sending downstream to Eventhouse.
        probe = f"{normalized[:-1]}+00:00" if normalized.endswith("Z") else normalized
        datetime.fromisoformat(probe)
        return normalized
    return text


def _build_event_id(table: str, fields: dict[str, Any]) -> str:
    row_id = fields.get("id")
    if row_id:
        return f"{table}:{row_id}"
    canonical = "|".join(f"{k}={fields.get(k)}" for k in sorted(fields.keys()))
    digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
    return f"{table}:{digest}"


def build_eventhouse_record(
    raw_envelope: dict[str, Any],
    topic_prefix: str = "synthea",
    timestamp_mode: str = "current",
    clean_names: bool = False,
    schema_version: str = "1.0",
) -> tuple[str, str, dict[str, Any]]:
    """Validate and normalize a raw Synthea stream message into a flat Eventhouse-ready record.

    Returns (kafka_topic, partition_key, flat_json_payload).
    """
    table = str(raw_envelope.get("table") or "").strip().lower()
    if table not in TABLE_SPECS:
        raise ValueError(f"Unknown Synthea table: {table!r}")

    spec = TABLE_SPECS[table]
    raw_data = raw_envelope.get("data")
    if not isinstance(raw_data, dict):
        raise ValueError(f"Missing or non-dict 'data' for table {table!r}")

    normalized_input = {str(k).strip().lower(): v for k, v in raw_data.items()}
    typed_fields: dict[str, Any] = {}

    for col_name, kql_type in spec.columns:
        if table == "observations" and col_name == "numeric_value":
            obs_type = str(normalized_input.get("type") or "").strip().lower()
            raw_obs_val = normalized_input.get("value")
            if obs_type == "numeric" and raw_obs_val not in (None, ""):
                try:
                    typed_fields["numeric_value"] = float(str(raw_obs_val).strip())
                except ValueError:
                    typed_fields["numeric_value"] = None
            else:
                typed_fields["numeric_value"] = None
            continue

        raw_val = normalized_input.get(col_name)
        coerced = _coerce_typed_value(raw_val, kql_type, col_name)
        if coerced is None and col_name in spec.defaults:
            coerced = spec.defaults[col_name]
        if clean_names and col_name in spec.person_name_fields and isinstance(coerced, str):
            coerced = _strip_synthea_person_digits(coerced)
        typed_fields[col_name] = coerced

    clinical_time: str | None = None
    for time_field in spec.clinical_time_fields:
        candidate = typed_fields.get(time_field)
        if candidate:
            clinical_time = str(candidate)
            break

    now_iso = _utc_now_iso()
    event_time = now_iso if timestamp_mode == "current" else (clinical_time or now_iso)

    partition_key = ""
    for key_field in spec.key_fields:
        val = typed_fields.get(key_field)
        if val:
            partition_key = str(val)
            break

    payload: dict[str, Any] = {
        "event_id": _build_event_id(table, typed_fields),
        "event_type": table,
        "event_time": event_time,
        "clinical_time": clinical_time,
        "batch_id": int(raw_envelope.get("batch_id") or 0),
        "seq": int(raw_envelope.get("seq") or 0),
        "sim_state": raw_envelope.get("state"),
        "schema_version": schema_version,
        "source_system": "synthea",
    }
    payload.update(typed_fields)

    topic = f"{topic_prefix}.{spec.name}"
    return topic, partition_key, payload


def build_dlq_record(raw_line: str, table: str, error_reason: str) -> dict[str, Any]:
    """Construct a structured Dead Letter Queue event for malformed records."""
    return {
        "event_time": _utc_now_iso(),
        "source_table": table or "unknown",
        "error_reason": error_reason,
        "raw_payload": raw_line,
    }


def generate_eventhouse_kql(topic_prefix: str = "synthea") -> str:
    """Generate Microsoft Fabric Eventhouse KQL DDL and JSON ingestion mappings for all 19 tables + DLQ."""
    sections: list[str] = [
        "// =============================================================================",
        "// CarePulse - Microsoft Fabric Eventhouse Schema & JSON Ingestion Mappings",
        f"// Source Kafka topic prefix: {topic_prefix}.<table>",
        "// Paste into Eventhouse KQL Queryset, click inside line 6+, and press Run ONCE.",
        "// =============================================================================",
        ".execute database script with (ThrowOnErrors=true)",
        "<|",
    ]

    for spec in TABLE_SPECS.values():
        all_cols = ENVELOPE_COLUMNS + spec.columns
        col_defs = ", ".join(f'["{name}"]: {kql_type}' for name, kql_type in all_cols)
        mappings = [
            {"column": name, "path": f"$.{name}", "datatype": kql_type}
            for name, kql_type in all_cols
        ]
        mapping_json = json.dumps(mappings, separators=(",", ":"))
        sections.extend([
            f"// --- Table: {spec.name} (Kafka topic: {topic_prefix}.{spec.name}) ---",
            f".create-merge table {spec.name} ({col_defs})",
            f".create-or-alter table {spec.name} ingestion json mapping '{spec.name}_json_mapping' '{mapping_json}'",
            f".alter table {spec.name} policy streamingingestion enable",
        ])

    dlq_defs = ", ".join(f'["{name}"]: {kql_type}' for name, kql_type in DLQ_COLUMNS)
    dlq_mappings = json.dumps(
        [{"column": name, "path": f"$.{name}", "datatype": kql_type} for name, kql_type in DLQ_COLUMNS],
        separators=(",", ":"),
    )
    sections.extend([
        f"// --- Table: synthea_dlq (Kafka topic: {topic_prefix}.dlq) ---",
        f".create-merge table synthea_dlq ({dlq_defs})",
        f".create-or-alter table synthea_dlq ingestion json mapping 'synthea_dlq_json_mapping' '{dlq_mappings}'",
        ".alter table synthea_dlq policy streamingingestion enable",
        "",
    ])
    return "\n".join(sections)


_KQL_EXTRACTOR_BY_TYPE: dict[str, str] = {
    "datetime": "todatetime",
    "real": "toreal",
    "long": "tolong",
    "string": "tostring",
}


def _kql_update_policy_line(table_name: str, landing_table: str, query_name: str) -> str:
    policy_json = json.dumps(
        [
            {
                "IsEnabled": True,
                "Source": landing_table,
                "Query": f"{query_name}()",
                "IsTransactional": False,
                "PropagateIngestionProperties": False,
            }
        ],
        separators=(",", ":"),
    )
    return f".alter table {table_name} policy update @'{policy_json}'"


def generate_eventhouse_update_policies_kql(
    landing_table: str = "synthea_raw",
    fabric_topic: str = "esehdb5mv76pqjpzg7rtt7_eh",
) -> str:
    """Generate a one-click `.execute database script` creating Bronze + 19 Silver tables + Update Policies."""
    raw_mapping = json.dumps(
        [{"column": "payload", "path": "$.payload", "datatype": "dynamic"}],
        separators=(",", ":"),
    )
    sections: list[str] = [
        "// =============================================================================",
        "// CarePulse - Microsoft Fabric Eventhouse One-Click Setup (Bronze -> 19 Silver)",
        f"// Eventstream Custom Endpoint Topic: {fabric_topic}",
        "// Paste into Eventhouse KQL Queryset, click inside line 6+, and press Run ONCE.",
        "// =============================================================================",
        ".execute database script with (ThrowOnErrors=true)",
        "<|",
        f".create-merge table {landing_table} (payload: dynamic)",
        f".create-or-alter table {landing_table} ingestion json mapping '{landing_table}_json_mapping' '{raw_mapping}'",
        f".alter table {landing_table} policy streamingingestion enable",
    ]

    for spec in TABLE_SPECS.values():
        all_cols = ENVELOPE_COLUMNS + spec.columns
        col_defs = ", ".join(f'["{name}"]: {kql_type}' for name, kql_type in all_cols)
        projections = ", ".join(
            f'["{name}"] = {_KQL_EXTRACTOR_BY_TYPE[kql_type]}(payload["{name}"])'
            for name, kql_type in all_cols
        )
        fn_name = f"parse_{spec.name}"
        sections.extend([
            f"// --- Silver Table: {spec.name} ---",
            f".create-merge table {spec.name} ({col_defs})",
            f'.create-or-alter function {fn_name}() {{ {landing_table} | where tostring(payload["event_type"]) == "{spec.name}" | project {projections} }}',
            _kql_update_policy_line(spec.name, landing_table, fn_name),
        ])

    dlq_defs = ", ".join(f'["{name}"]: {kql_type}' for name, kql_type in DLQ_COLUMNS)
    dlq_proj = ", ".join(
        f'["{name}"] = {_KQL_EXTRACTOR_BY_TYPE[kql_type]}(payload["{name}"])'
        for name, kql_type in DLQ_COLUMNS
    )
    sections.extend([
        "// --- Silver Table: synthea_dlq ---",
        f".create-merge table synthea_dlq ({dlq_defs})",
        f'.create-or-alter function parse_synthea_dlq() {{ {landing_table} | where isnotempty(tostring(payload["error_reason"])) | project {dlq_proj} }}',
        _kql_update_policy_line("synthea_dlq", landing_table, "parse_synthea_dlq"),
        "",
    ])
    return "\n".join(sections)

