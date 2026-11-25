"""Unit and integration tests for all 19 Synthea Kafka topics, Fabric Eventstream bridge, and Eventhouse KQL."""

from __future__ import annotations

import json

from carepulse.bridge import FabricBridgeRunner
from carepulse.fabric_config import build_fabric_producer_config, parse_fabric_connection_string
from carepulse.producer import KafkaEventPublisher
from carepulse.schemas import (
    ENVELOPE_COLUMNS,
    TABLE_SPECS,
    build_eventhouse_record,
    generate_eventhouse_kql,
    generate_eventhouse_update_policies_kql,
)


def test_all_19_tables_defined_without_envelope_collisions() -> None:
    expected_tables = {
        "patients",
        "organizations",
        "providers",
        "payers",
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
        "patient_expenses",
    }
    assert set(TABLE_SPECS.keys()) == expected_tables

    envelope_names = {col for col, _ in ENVELOPE_COLUMNS}
    for name, spec in TABLE_SPECS.items():
        table_cols = {col for col, _ in spec.columns}
        assert not (envelope_names & table_cols), f"Column collision in {name}"


def test_observation_cleaning_and_numeric_extraction() -> None:
    numeric_raw = {
        "table": "observations",
        "batch_id": 1,
        "seq": 10,
        "state": "Texas",
        "data": {
            "date": "2024-02-01T10:15:00Z",
            "patient": "pat-1",
            "encounter": "enc-1",
            "category": None,
            "code": "8867-4",
            "description": "Heart rate",
            "value": "74.5",
            "units": "/min",
            "type": "numeric",
        },
    }
    topic, key, payload = build_eventhouse_record(numeric_raw, timestamp_mode="original")
    assert topic == "synthea.observations"
    assert key == "pat-1"
    assert payload["category"] == "uncategorized"
    assert payload["value"] == "74.5"
    assert payload["numeric_value"] == 74.5
    assert payload["sim_state"] == "Texas"
    assert payload["clinical_time"] == "2024-02-01T10:15:00Z"
    assert payload["event_time"] == "2024-02-01T10:15:00Z"

    text_raw = {
        "table": "observations",
        "batch_id": 1,
        "seq": 11,
        "state": "Texas",
        "data": {
            "date": "2024-02-01T10:15:00Z",
            "patient": "pat-1",
            "encounter": "enc-1",
            "category": "social-history",
            "code": "72166-2",
            "description": "Smoking status",
            "value": "Never smoker",
            "units": None,
            "type": "text",
        },
    }
    _, _, text_payload = build_eventhouse_record(text_raw)
    assert text_payload["value"] == "Never smoker"
    assert text_payload["numeric_value"] is None


def test_clean_names_strips_synthea_digits_from_patients_not_organizations() -> None:
    patient_raw = {
        "table": "patients",
        "batch_id": 1,
        "seq": 1,
        "state": "Massachusetts",
        "data": {
            "id": "pat-1",
            "birthdate": "1990-05-10",
            "first": "Tracy345",
            "middle": "Ann12",
            "last": "Ullrich385",
            "income": "52000",
        },
    }
    _, _, pat_payload = build_eventhouse_record(patient_raw, clean_names=True)
    assert pat_payload["first"] == "Tracy"
    assert pat_payload["middle"] == "Ann"
    assert pat_payload["last"] == "Ullrich"
    assert pat_payload["birthdate"] == "1990-05-10T00:00:00Z"
    assert pat_payload["income"] == 52000

    org_raw = {
        "table": "organizations",
        "batch_id": 1,
        "seq": 2,
        "state": "Massachusetts",
        "data": {"id": "org-1", "name": "CLINIC 319A", "utilization": "42"},
    }
    _, _, org_payload = build_eventhouse_record(org_raw, clean_names=True)
    assert org_payload["name"] == "CLINIC 319A"
    assert org_payload["utilization"] == 42


class _FakeKafkaProducer:
    def __init__(self) -> None:
        self.sent: list[tuple[str, bytes | None, dict[str, object]]] = []
        self.flushed = False

    def produce(self, topic: str, key: bytes | None, value: bytes, on_delivery=None) -> None:
        self.sent.append((topic, key, json.loads(value.decode("utf-8"))))
        if on_delivery:
            on_delivery(None, type("Msg", (), {"topic": lambda s: topic})())

    def poll(self, timeout: float) -> int:
        return 0

    def flush(self, timeout: float = 15) -> int:
        self.flushed = True
        return 0


def test_publisher_routes_all_19_tables_and_sends_invalid_to_dlq() -> None:
    fake = _FakeKafkaProducer()
    publisher = KafkaEventPublisher(producer=fake, rate_per_sec=0)  # type: ignore[arg-type]

    for idx, table in enumerate(TABLE_SPECS):
        row_data: dict[str, str] = {}
        if table in ("patients", "organizations", "providers", "payers", "encounters", "careplans", "imaging_studies", "claims", "claims_transactions"):
            row_data["id"] = f"{table}-id-{idx}"
        if table in ("payer_transitions", "encounters", "conditions", "allergies", "careplans", "medications", "procedures", "immunizations", "observations", "imaging_studies", "devices", "supplies"):
            row_data["patient"] = "pat-99"
        if table in ("claims", "claims_transactions"):
            row_data["patientid"] = "pat-99"
        if table == "patient_expenses":
            row_data["patient_id"] = "pat-99"
            row_data["year"] = "2025-12-01T06:38:36Z"

        line = json.dumps({"table": table, "batch_id": 1, "seq": idx + 1, "state": "Florida", "data": row_data})
        assert publisher.process_ndjson_line(line) is True

    assert publisher.process_ndjson_line("{not-valid-json") is False
    publisher.flush()

    assert fake.flushed is True
    assert publisher.dlq_count == 1
    assert publisher.delivery_errors == 0
    assert len(publisher.counts_by_topic) == 20  # 19 tables + dlq
    assert publisher.counts_by_topic["synthea.dlq"] == 1
    assert publisher.counts_by_topic["synthea.patient_expenses"] == 1


def test_publisher_single_target_topic_for_fabric_eventstream() -> None:
    fake = _FakeKafkaProducer()
    publisher = KafkaEventPublisher(
        producer=fake,  # type: ignore[arg-type]
        target_topic="fabric_eh_topic",
        rate_per_sec=0,
    )
    line = json.dumps({
        "table": "encounters",
        "batch_id": 1,
        "seq": 1,
        "state": "Texas",
        "data": {"id": "enc-1", "patient": "pat-1", "start": "2025-01-01T10:00:00Z"},
    })
    assert publisher.process_ndjson_line(line) is True
    assert fake.sent[0][0] == "fabric_eh_topic"
    assert fake.sent[0][2]["payload"]["event_type"] == "encounters"


def test_parse_fabric_connection_string_and_producer_config() -> None:
    conn = (
        "Endpoint=sb://demo-ns.servicebus.windows.net/;"
        "SharedAccessKeyName=key1;SharedAccessKey=secret123;EntityPath=demo_eh"
    )
    bootstrap, topic, cleaned = parse_fabric_connection_string(conn)
    assert bootstrap == "demo-ns.servicebus.windows.net:9093"
    assert topic == "demo_eh"
    assert cleaned == conn

    conf, resolved_topic = build_fabric_producer_config(conn)
    assert resolved_topic == "demo_eh"
    assert conf["bootstrap.servers"] == "demo-ns.servicebus.windows.net:9093"
    assert conf["security.protocol"] == "SASL_SSL"
    assert conf["sasl.mechanism"] == "PLAIN"
    assert conf["sasl.username"] == "$ConnectionString"
    assert conf["compression.type"] == "none"
    assert conf["enable.idempotence"] is False


def test_bridge_forwards_messages_to_fabric_topic() -> None:
    class _FakeMsg:
        def __init__(self, topic: str, key: bytes, val: bytes) -> None:
            self._topic = topic
            self._key = key
            self._val = val

        def error(self):
            return None

        def topic(self) -> str:
            return self._topic

        def key(self) -> bytes:
            return self._key

        def value(self) -> bytes:
            return self._val

    class _FakeConsumer:
        def __init__(self, msgs) -> None:
            self._msgs = list(msgs)
            self.committed = 0
            self.closed = False

        def poll(self, timeout: float = 1.0):
            return self._msgs.pop(0) if self._msgs else None

        def commit(self, asynchronous: bool = False) -> None:
            self.committed += 1

        def close(self) -> None:
            self.closed = True

    fake_prod = _FakeKafkaProducer()
    fake_cons = _FakeConsumer([
        _FakeMsg("synthea.encounters", b"pat-1", json.dumps({"event_type": "encounters"}).encode("utf-8")),
        _FakeMsg("synthea.observations", b"pat-1", json.dumps({"event_type": "observations"}).encode("utf-8")),
    ])
    runner = FabricBridgeRunner(consumer=fake_cons, producer=fake_prod, fabric_topic="eseh_topic")  # type: ignore[arg-type]
    count = runner.run(max_messages=2)

    assert count == 2
    assert fake_cons.committed == 1
    assert fake_cons.closed is True
    assert fake_prod.flushed is True
    assert [m[0] for m in fake_prod.sent] == ["eseh_topic", "eseh_topic"]
    assert fake_prod.sent[0][2] == {"payload": {"event_type": "encounters"}}


def test_eventhouse_kql_and_update_policies_cover_all_19_tables_and_dlq() -> None:
    kql = generate_eventhouse_kql(topic_prefix="synthea")
    assert ".execute database script with (ThrowOnErrors=true)" in kql
    for table in TABLE_SPECS:
        assert f".create-merge table {table} (" in kql
        assert f"'{table}_json_mapping'" in kql
    assert '.create-merge table synthea_dlq (' in kql
    assert '["first"]: string' in kql

    update_kql = generate_eventhouse_update_policies_kql(
        landing_table="synthea_raw",
        fabric_topic="esehdb5mv76pqjpzg7rtt7_eh",
    )
    assert ".execute database script with (ThrowOnErrors=true)" in update_kql
    assert ".create-merge table synthea_raw (" in update_kql
    assert "'synthea_raw_json_mapping'" in update_kql
    assert '"path":"$.payload"' in update_kql
    for table in TABLE_SPECS:
        assert f".create-or-alter function parse_{table}()" in update_kql
        assert f'.alter table {table} policy update' in update_kql
    assert ".create-or-alter function parse_synthea_dlq()" in update_kql


def test_gold_cleaning_and_dashboard_kql_scripts_valid() -> None:
    from pathlib import Path

    gold_kql = Path("kql/02_gold_cleaning.kql").read_text(encoding="utf-8")
    assert ".execute database script with (ThrowOnErrors=true)" in gold_kql
    assert "(?=" not in gold_kql  # RE2 in Kusto does not support lookaheads
    script_body = gold_kql.split("<|\n", 1)[1].strip()
    assert "\n\n" not in script_body

    for fn in (
        "clean_patients",
        "clean_providers",
        "clean_encounters",
        "clean_organizations",
        "clean_conditions",
        "clean_observations",
        "clean_medications",
        "clean_claims_transactions",
        "dq_pipeline_health",
    ):
        assert f".create-or-alter function {fn}()" in gold_kql

    # clean_encounters must be defined before clean_organizations (which calls it).
    assert gold_kql.index("clean_encounters()") < gold_kql.index(".create-or-alter function clean_organizations()")

    dash_kql = Path("kql/03_dashboard_queries.kql").read_text(encoding="utf-8")
    assert "_startTime" in dash_kql and "_endTime" in dash_kql
    assert "dq_pipeline_health()" in dash_kql
    assert "[Tile 12_KPI]" in dash_kql
    assert "[Tile 15]" in dash_kql
    assert Path("kql/01_bronze_silver_setup.kql").exists()
    assert Path("kql/direct_19_topics_setup.kql").exists()
