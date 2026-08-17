"""
Apply ScyllaDB-native ingestion for a ScyllaDB sink node.

ScyllaDB has no Kafka-engine-table equivalent to ClickHouse's (see clickhouse_sink.py),
so consumption can't happen inside the database itself. Instead the standalone
`scylla-consumer` container (producer/scylla_consumer.py) does the consuming; this module's
job is just to ensure the keyspace/table exist and to hand that consumer its routing
config — which topic(s) feed which table, and how source fields map onto sink columns —
via a small JSON file on the shared jobs-repo volume. The consumer polls that file and
picks up new/changed targets without a restart.

All DDL is idempotent ('IF NOT EXISTS'). The JSON file is merged (not overwritten), so
multiple pipelines/tables can coexist; writes are atomic (temp file + rename) so the
consumer never observes a half-written file.
"""
from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
from typing import Any

from cassandra.cluster import Cluster
from cassandra.policies import DCAwareRoundRobinPolicy

from flink_job_generator import resolve_jobs_repo
from sql_parser import Field, _fields_for_node, _safe_identifier, _safe_string, upstream_kafka_sources

_CQL_TYPE_MAP = {
    "STRING": "text",
    "DOUBLE": "double",
    "FLOAT": "float",
    "INT": "int",
    "BIGINT": "bigint",
    "BOOLEAN": "boolean",
    "BYTES": "blob",
}

_TARGETS_FILENAME = "scylla-sink-targets.json"


def _cql_type(field_type: str) -> str:
    return _CQL_TYPE_MAP.get(field_type.upper(), "text")


def _field_map(sink_fields: list[Field], src_fields: list[Field]) -> dict[str, str | None]:
    """sink column name -> matching source field name, or None if unmapped (insert NULL)."""
    src_names = {f.name for f in src_fields}
    return {f.name: (f.name if f.name in src_names else None) for f in sink_fields}


def _ensure_keyspace_and_table(
    contact_points: list[str],
    port: int,
    keyspace: str,
    table: str,
    sink_fields: list[Field],
    pk_fields: list[str],
) -> None:
    cluster = Cluster(
        contact_points,
        port=port,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc="datacenter1"),
        protocol_version=4,
    )
    try:
        session = cluster.connect()
        session.execute(
            f"CREATE KEYSPACE IF NOT EXISTS {keyspace} "
            f"WITH replication = {{'class': 'SimpleStrategy', 'replication_factor': 1}}"
        )
        col_defs = ", ".join(f"{f.name} {_cql_type(f.data_type)}" for f in sink_fields)
        pk = ", ".join(pk_fields)
        session.execute(
            f"CREATE TABLE IF NOT EXISTS {keyspace}.{table} ({col_defs}, PRIMARY KEY ({pk}))"
        )
    finally:
        cluster.shutdown()


def _targets_path() -> Path:
    return resolve_jobs_repo() / _TARGETS_FILENAME


def _load_targets(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    try:
        return json.loads(path.read_text())
    except (OSError, json.JSONDecodeError):
        return {}


def _write_targets(path: Path, targets: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = path.with_suffix(f"{path.suffix}.tmp-{os.getpid()}")
    tmp_path.write_text(json.dumps(targets, indent=2, sort_keys=True))
    os.replace(tmp_path, path)


async def apply_scylla_sink(
    node: dict[str, Any],
    edges: list[dict[str, Any]],
    all_nodes: dict[str, dict[str, Any]],
    global_cfg: dict[str, Any],
) -> str:
    """Idempotently ensure the sink keyspace/table exist, and publish the topic->table
    routing config the scylla-consumer container needs. Returns a summary string."""
    props = node.get("properties") or {}
    label = _safe_identifier(node["label"])
    table = _safe_string(props.get("table", label.lower().replace(" ", "_")))

    sc = global_cfg["scylladb"]
    keyspace = _safe_string(props.get("keyspace", sc.get("keyspace", "xstream")))
    contact_points = sc.get("contact_points", ["localhost"])
    port = int(sc.get("port", 9042))

    sink_fields = _fields_for_node(node)
    pk_raw = props.get("primary_key") or []
    pk_fields = pk_raw if isinstance(pk_raw, list) and pk_raw else [sink_fields[0].name]

    await asyncio.to_thread(
        _ensure_keyspace_and_table, contact_points, port, keyspace, table, sink_fields, pk_fields
    )

    unique_sources = upstream_kafka_sources(node["id"], edges, all_nodes)
    topics: dict[str, dict[str, Any]] = {}
    for src_id in unique_sources:
        src_node = all_nodes[src_id]
        src_props = src_node.get("properties") or {}
        src_label = _safe_identifier(src_node["label"])
        topic = _safe_string(src_props.get("topic", src_label.lower().replace(" ", "_")))
        src_fields = _fields_for_node(src_node)
        topics[topic] = {"field_map": _field_map(sink_fields, src_fields)}

    targets_path = _targets_path()
    targets = _load_targets(targets_path)
    table_key = f"{keyspace}.{table}"
    if topics:
        targets[table_key] = {"keyspace": keyspace, "table": table, "primary_key": pk_fields, "topics": topics}
        _write_targets(targets_path, targets)
        return (
            f"ScyllaDB: ensured table `{table_key}`; consumer config updated for "
            f"{len(topics)} topic(s): {', '.join(topics)}"
        )

    return f"ScyllaDB: table `{table_key}` ensured, but node has no upstream Kafka source — nothing will flow in."
