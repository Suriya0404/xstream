"""
Parse Flink SQL snippets and auto-generate Flink SQL DDL + DML for pipeline nodes.
"""
from __future__ import annotations

import re
from typing import NamedTuple

import sqlparse
import sqlparse.sql as sp
import sqlparse.tokens as T

from core.constants import NodeType


class Field(NamedTuple):
    name: str
    data_type: str


_TYPE_MAP = {
    "STRING": "STRING", "DOUBLE": "DOUBLE", "FLOAT": "FLOAT",
    "INT": "INT", "BIGINT": "BIGINT", "BOOLEAN": "BOOLEAN", "BYTES": "BYTES",
}


def _safe_identifier(value: str) -> str:
    """Strip characters not valid in a Flink table/column identifier (backtick-quoted)."""
    return re.sub(r"[^\w\-]", "", str(value))


def _safe_string(value: str) -> str:
    """Escape single quotes for Flink SQL WITH-clause string literals."""
    return str(value).replace("'", "''")


def _handles_to_fields(handles: list) -> list[Field]:
    fields = []
    for h in (handles or []):
        if isinstance(h, (list, tuple)) and len(h) >= 2:
            name = str(h[0])
            dtype = _TYPE_MAP.get(str(h[1]).upper(), "STRING")
            fields.append(Field(name=name, data_type=dtype))
    return fields


def _fields_for_node(node: dict) -> list[Field]:
    flink_sql = (node.get("flink_sql") or "").strip()
    if flink_sql:
        f = parse_fields(flink_sql)
        if f:
            return f
    handles = (node.get("properties") or {}).get("_handles", [])
    if handles:
        return _handles_to_fields(handles)
    return [Field("id", "STRING"), Field("payload", "STRING")]


def _trace_kafka_sources(node_id: str, edges: list[dict], all_nodes: dict) -> list[str]:
    """Walk upstream edges; return IDs of all reachable Kafka source nodes."""
    visited: set[str] = set()
    kafka_ids: list[str] = []
    stack = [node_id]
    while stack:
        nid = stack.pop()
        if nid in visited:
            continue
        visited.add(nid)
        node = all_nodes.get(nid)
        if not node:
            continue
        if node["node_type"] == NodeType.KAFKA:
            kafka_ids.append(nid)
        else:
            for e in edges:
                if e["target_id"] == nid and e["source_id"] in all_nodes:
                    stack.append(e["source_id"])
    return kafka_ids


def upstream_kafka_sources(node_id: str, edges: list[dict], all_nodes: dict[str, dict]) -> list[str]:
    """Direct + traced-through Kafka source node IDs feeding a native sink (ClickHouse/ScyllaDB),
    deduplicated and in discovery order. Shared by clickhouse_sink.py and scylla_sink_config.py."""
    direct_upstream_ids = [
        e["source_id"] for e in edges
        if e["target_id"] == node_id and e["source_id"] in all_nodes
    ]
    kafka_source_ids: list[str] = []
    for uid in direct_upstream_ids:
        if all_nodes[uid]["node_type"] == NodeType.KAFKA:
            kafka_source_ids.append(uid)
        else:
            kafka_source_ids.extend(_trace_kafka_sources(uid, edges, all_nodes))
    seen: set[str] = set()
    return [sid for sid in kafka_source_ids if not (sid in seen or seen.add(sid))]


def _parse_fields_with_sqlparse(sql: str) -> list[Field]:
    """
    Use sqlparse to locate the column-list parenthesis of a CREATE TABLE statement,
    then parse each line to extract field definitions.

    sqlparse reliably identifies the first Parenthesis token even when the WITH (...)
    clause also contains parentheses, avoiding the nested-paren pitfall of plain regex.
    """
    parsed = sqlparse.parse(sql.strip())
    if not parsed:
        return []
    stmt = parsed[0]

    # First Parenthesis token is always the column list
    paren = next((tok for tok in stmt.tokens if isinstance(tok, sp.Parenthesis)), None)
    if paren is None:
        return []

    # Strip outer parens, normalise to one clause per line, then parse
    inner = paren.value[1:-1]

    # Flatten to comma-separated segments that each hold one definition.
    # We treat newlines as separators too so multi-line DDL works naturally.
    segments = re.split(r",|\n", inner)

    fields: list[Field] = []
    for seg in segments:
        seg = seg.strip()
        if not seg:
            continue
        if re.match(r"(PRIMARY|WATERMARK|CONSTRAINT|UNIQUE)\b", seg, re.IGNORECASE):
            continue
        if re.search(r"\b(METADATA|VIRTUAL)\b", seg, re.IGNORECASE):
            continue
        m = re.match(r"`?(\w+)`?\s+([A-Z][A-Z0-9_]*)", seg, re.IGNORECASE)
        if m:
            name = m.group(1)
            dtype = _TYPE_MAP.get(m.group(2).upper(), m.group(2).upper())
            fields.append(Field(name=name, data_type=dtype))
    return fields


def parse_fields(sql: str) -> list[Field]:
    """
    Extract typed field definitions from a Flink SQL snippet.
    Tries sqlparse on CREATE TABLE first; falls back to regex for SELECT.
    """
    sql = sql.strip()

    if re.search(r"\bCREATE\s+TABLE\b", sql, re.IGNORECASE):
        fields = _parse_fields_with_sqlparse(sql)
        if fields:
            return fields

    # Fallback: extract column aliases from a SELECT clause
    select_match = re.search(r"SELECT\s+(.*?)\s+FROM", sql, re.IGNORECASE | re.DOTALL)
    if select_match:
        fields = []
        for col in select_match.group(1).split(","):
            m = re.search(r"(?:AS\s+)?`?(\w+)`?\s*$", col.strip(), re.IGNORECASE)
            if m:
                fields.append(Field(m.group(1), "STRING"))
        if fields:
            return fields
    return []


def generate_flink_sql(
    node: dict,
    edges: list[dict],
    all_nodes: dict[str, dict],
    global_cfg: dict,
) -> str:
    node_type = node["node_type"]
    props = node.get("properties") or {}
    flink_sql = (node.get("flink_sql") or "").strip()
    label = _safe_identifier(node["label"])

    lines: list[str] = [f"-- Node: {label} ({node_type})", ""]
    fields = _fields_for_node(node)

    # ── DDL ──────────────────────────────────────────────
    if node_type == NodeType.KAFKA:
        topic = _safe_string(props.get("topic", label.lower().replace(" ", "_")))
        bootstrap = _safe_string(global_cfg["kafka"]["bootstrap_servers"])
        group_id = _safe_string(props.get("group_id", f"xstream_{topic}"))
        fmt = _safe_string(props.get("format", "json"))
        col_defs = "\n  ".join(f"`{f.name}` {f.data_type}," for f in fields)

        lines += [
            f"CREATE TABLE IF NOT EXISTS `{label}` (",
            f"  {col_defs}",
            f"  `_kafka_ts` TIMESTAMP(3) METADATA FROM 'timestamp' VIRTUAL",
            f") WITH (",
            f"  'connector' = 'kafka',",
            f"  'topic' = '{topic}',",
            f"  'properties.bootstrap.servers' = '{bootstrap}',",
            f"  'properties.group.id' = '{group_id}',",
            f"  'scan.startup.mode' = 'earliest-offset',",
            f"  'format' = '{fmt}'",
            f");",
        ]

    # ScyllaDB and ClickHouse sinks are handled natively (clickhouse_sink.py,
    # scylla_sink_config.py) — neither has a usable Flink Table connector
    # ('cassandra' has no SQL factory at all; 'jdbc' ships no ClickHouse dialect).
    # No DDL/DML emitted here for those node types.

    # ── DML ──────────────────────────────────────────────
    if flink_sql and not flink_sql.upper().startswith("CREATE"):
        lines += ["", flink_sql]

    return "\n".join(line for line in lines if line is not None)
