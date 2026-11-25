"""Bridge consuming all 19 local Synthea Kafka topics and forwarding live to Fabric Eventstream."""

from __future__ import annotations

import argparse
import json
import os
import sys

from confluent_kafka import Consumer, KafkaError, Producer

from carepulse.fabric_config import (
    build_fabric_producer_config,
    load_env,
    resolve_fabric_credentials,
)
from carepulse.schemas import TABLE_SPECS


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the local-Kafka-to-Fabric bridge."""
    parser = argparse.ArgumentParser(
        description="Forward all 19 local Synthea Kafka topics to Microsoft Fabric Eventstream."
    )
    parser.add_argument(
        "--local-bootstrap",
        default=os.environ.get("LOCAL_KAFKA_BOOTSTRAP", "localhost:9092"),
        help="Local Kafka bootstrap servers (default: localhost:9092).",
    )
    parser.add_argument(
        "--topic-prefix",
        default=os.environ.get("TOPIC_PREFIX", "synthea"),
        help="Prefix of local Synthea Kafka topics (default: synthea).",
    )
    parser.add_argument(
        "--group-id",
        default="carepulse-fabric-bridge",
        help="Kafka consumer group ID on the local broker.",
    )
    parser.add_argument(
        "--auto-offset-reset",
        choices=("earliest", "latest"),
        default="earliest",
        help="Where to start consuming if no committed offset exists (default: earliest).",
    )
    parser.add_argument(
        "--include-dlq",
        action="store_true",
        help="Also forward <prefix>.dlq events to Fabric.",
    )
    parser.add_argument("--fabric-connection-string", default=None, help="Fabric Eventstream SAS connection string.")
    parser.add_argument("--fabric-topic", default=None, help="Fabric Eventstream topic (EntityPath).")
    parser.add_argument("--max-messages", type=int, default=None, help="Stop after forwarding N messages.")
    parser.add_argument("--dry-run", action="store_true", help="Consume locally and print without sending to Fabric.")
    return parser.parse_args(argv)


class FabricBridgeRunner:
    """Forwards messages from local Kafka topics into a single Fabric Eventstream topic."""

    def __init__(
        self,
        consumer: Consumer,
        producer: Producer | None,
        fabric_topic: str,
        dry_run: bool = False,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.fabric_topic = fabric_topic
        self.dry_run = dry_run
        self.forwarded = 0
        self.errors = 0
        self.counts_by_source_topic: dict[str, int] = {}

    def _on_delivery(self, err: object, msg: object) -> None:
        if err is not None:
            self.errors += 1
            print(f"[ERROR] Fabric delivery failed: {err}", file=sys.stderr)

    def forward_message(self, source_topic: str, key: bytes | None, value: bytes | None) -> None:
        """Send one consumed record to Fabric Eventstream wrapped in {'payload': ...}."""
        if value is None:
            return
        decoded = json.loads(value.decode("utf-8"))
        wrapped = decoded if (isinstance(decoded, dict) and set(decoded.keys()) == {"payload"}) else {"payload": decoded}
        wire_bytes = json.dumps(wrapped, ensure_ascii=False).encode("utf-8")
        if self.dry_run:
            preview = wire_bytes.decode("utf-8", errors="replace")[:140]
            print(f"[DRY-RUN BRIDGE] {source_topic} -> {self.fabric_topic}: {preview}")
        else:
            assert self.producer is not None
            while True:
                try:
                    self.producer.produce(
                        topic=self.fabric_topic,
                        key=key,
                        value=wire_bytes,
                        on_delivery=self._on_delivery,
                    )
                    break
                except BufferError:
                    self.producer.poll(0.1)
            self.producer.poll(0)

        self.forwarded += 1
        self.counts_by_source_topic[source_topic] = self.counts_by_source_topic.get(source_topic, 0) + 1

    def run(self, max_messages: int | None = None) -> int:
        """Poll local Kafka continuously and forward until max_messages or Ctrl+C."""
        pending_commit = 0
        try:
            while max_messages is None or self.forwarded < max_messages:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    if self.producer is not None and not self.dry_run:
                        self.producer.poll(0)
                    continue
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    raise RuntimeError(f"Local Kafka consumer error: {msg.error()}")

                self.forward_message(msg.topic(), msg.key(), msg.value())
                pending_commit += 1

                if pending_commit >= 50:
                    if self.producer is not None and not self.dry_run:
                        self.producer.flush(timeout=15)
                    self.consumer.commit(asynchronous=False)
                    pending_commit = 0
        except KeyboardInterrupt:
            print("\n[Interrupted] Flushing bridge and committing offsets...", file=sys.stderr)
        finally:
            if self.producer is not None and not self.dry_run:
                self.producer.flush(timeout=15)
            if pending_commit > 0:
                self.consumer.commit(asynchronous=False)
            self.consumer.close()
        return self.forwarded


def main(argv: list[str] | None = None) -> int:
    """Entry point for bridging local Kafka topics into Fabric Eventstream."""
    load_env()
    args = parse_args(argv)

    source_topics = [f"{args.topic_prefix}.{name}" for name in TABLE_SPECS]
    if args.include_dlq:
        source_topics.append(f"{args.topic_prefix}.dlq")

    consumer = Consumer(
        {
            "bootstrap.servers": args.local_bootstrap,
            "group.id": args.group_id,
            "auto.offset.reset": args.auto_offset_reset,
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe(source_topics)

    producer: Producer | None = None
    if args.dry_run:
        _, fabric_topic = resolve_fabric_credentials(args.fabric_connection_string, args.fabric_topic)
        fabric_topic = fabric_topic or "fabric_dry_run_topic"
    else:
        conn_str, override_topic = resolve_fabric_credentials(args.fabric_connection_string, args.fabric_topic)
        if not conn_str:
            raise SystemExit(
                "Missing Fabric connection string. Set FABRIC_CONNECTION_STRING in .env or pass --fabric-connection-string."
            )
        fabric_conf, fabric_topic = build_fabric_producer_config(conn_str, override_topic=override_topic)
        producer = Producer(fabric_conf)

    print(
        f"[Bridge] Subscribed to {len(source_topics)} local topics on {args.local_bootstrap} -> "
        f"Fabric topic '{fabric_topic}'",
        file=sys.stderr,
    )
    runner = FabricBridgeRunner(consumer=consumer, producer=producer, fabric_topic=fabric_topic, dry_run=args.dry_run)
    runner.run(max_messages=args.max_messages)
    print(
        f"Bridge Summary: forwarded={runner.forwarded}, errors={runner.errors}, "
        f"by_topic={json.dumps(runner.counts_by_source_topic)}",
        file=sys.stderr,
    )
    return 1 if runner.errors > 0 else 0


if __name__ == "__main__":
    raise SystemExit(main())
