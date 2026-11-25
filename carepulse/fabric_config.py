"""Shared configuration helpers for connecting Kafka clients to Microsoft Fabric Eventstream."""

from __future__ import annotations

import os
from typing import Any
from urllib.parse import urlparse

from dotenv import load_dotenv


def load_env() -> None:
    """Load environment variables from .env in the working directory if present."""
    load_dotenv()


def parse_fabric_connection_string(
    connection_string: str,
    override_topic: str | None = None,
) -> tuple[str, str, str]:
    """Extract (bootstrap_servers, topic, cleaned_conn_str) from a Fabric Eventstream connection string."""
    cleaned = connection_string.strip().strip('"').strip("'")
    if not cleaned:
        raise ValueError("Fabric connection string cannot be empty.")

    parts: dict[str, str] = {}
    for segment in cleaned.split(";"):
        if "=" not in segment:
            continue
        key, value = segment.split("=", 1)
        parts[key.strip().lower()] = value.strip()

    endpoint = parts.get("endpoint")
    if not endpoint:
        raise ValueError("Fabric connection string is missing 'Endpoint=sb://...'.")

    parsed = urlparse(endpoint)
    host = parsed.hostname
    if not host:
        raise ValueError(f"Could not parse host from Endpoint: {endpoint!r}")

    topic = (override_topic or parts.get("entitypath") or "").strip()
    if not topic:
        raise ValueError(
            "Fabric topic is missing. Include 'EntityPath=...' in the connection string or pass --fabric-topic."
        )

    bootstrap_servers = f"{host}:9093"
    return bootstrap_servers, topic, cleaned


def build_fabric_producer_config(
    connection_string: str,
    override_topic: str | None = None,
) -> tuple[dict[str, Any], str]:
    """Build confluent_kafka.Producer configuration tuned for Fabric Eventstream Kafka endpoints."""
    bootstrap_servers, topic, cleaned = parse_fabric_connection_string(
        connection_string,
        override_topic=override_topic,
    )
    # Azure Event Hubs / Fabric Kafka endpoints reject lz4/snappy and require SASL_SSL + PLAIN
    # with higher timeouts and keepalive to survive cloud load balancer idle drops.
    conf: dict[str, Any] = {
        "bootstrap.servers": bootstrap_servers,
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "PLAIN",
        "sasl.username": "$ConnectionString",
        "sasl.password": cleaned,
        "acks": "all",
        "enable.idempotence": False,
        "compression.type": "none",
        "linger.ms": 20,
        "retries": 5,
        "request.timeout.ms": 60000,
        "socket.keepalive.enable": True,
        "metadata.max.age.ms": 180000,
    }
    return conf, topic


def resolve_fabric_credentials(
    cli_connection_string: str | None,
    cli_topic: str | None,
) -> tuple[str, str | None]:
    """Resolve Fabric connection string and optional topic from CLI flags or environment."""
    conn_str = (cli_connection_string or os.environ.get("FABRIC_CONNECTION_STRING") or "").strip()
    topic = (cli_topic or os.environ.get("FABRIC_TOPIC") or "").strip() or None
    return conn_str, topic
