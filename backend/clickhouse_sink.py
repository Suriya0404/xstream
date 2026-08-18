"""
Apply ClickHouse-native ingestion for a ClickHouse sink node.

Flink's bundled JDBC connector ships no ClickHouse dialect (`'connector'='jdbc'` sinks
can never actually run — see investigation history), so ClickHouse sinks bypass Flink
entirely. Instead we create a Kafka-engine source table + materialized view per upstream
Kafka source, feeding one MergeTree table — the same pattern already proven to work for
the hand-built finnhub_kafka_source -> finnhub_enriched_mv -> finnhub_analytics chain.

All DDL is idempotent ('IF NOT EXISTS'). To converge onto a pre-existing, differently
named Kafka-engine/materialized-view pair instead of creating a duplicate consumer for
the same topic (which would double-insert rows), set node.properties.ch_source_overrides:
  { "<topic>": { "source_table": "<existing kafka-engine table>", "mv_name": "<existing mv>" } }
"""
from __future__ import annotations

import re
from typing import Any

import httpx

from sql_parser import Field, _fields_for_node, _safe_identifier, _safe_string, upstream_kafka_sources

_CH_TYPE_MAP = {
    "STRING": "String",
    "DOUBLE": "Float64",
    "FLOAT": "Float32",
    "INT": "Int32",
    "BIGINT": "Int64",
    "BOOLEAN": "Bool",
    "BYTES": "String",
}


def _ch_type(field_type: str) -> str:
    return _CH_TYPE_MAP.get(field_type.upper(), "String")


def _safe_table_part(value: str) -> str:
    return re.sub(r"[^\w]", "_", value).strip("_") or "src"


async def _exec_ddl(client: httpx.AsyncClient, sql: str) -> None:
    r = await client.post("/", params={"query": sql})
    if r.status_code >= 400:
        raise RuntimeError(f"ClickHouse DDL failed:\n{sql}\n\n{r.text}")


async def _dest_column_types(client: httpx.AsyncClient, database: str, table: str) -> dict[str, str]:
    """Return {column_name: clickhouse_type} for an existing destination table.
    Empty dict if the table doesn't exist yet (we just created it as Nullable)."""
    query = (
        f"SELECT name, type FROM system.columns "
        f"WHERE database = '{database}' AND table = '{table}' FORMAT TSV"
    )
    r = await client.post("/", params={"query": query})
    if r.status_code >= 400:
        return {}
    types: dict[str, str] = {}
    for line in r.text.strip().splitlines():
        if "\t" in line:
            name, ctype = line.split("\t", 1)
            types[name] = ctype.strip()
    return types


def _zero_literal(ch_type: str) -> str:
    """A concrete default value insertable into a NON-Nullable column of this type."""
    t = ch_type.lower()
    if "string" in t or "uuid" in t:
        return "''"
    if "bool" in t:
        return "false"
    # Numeric, Date and DateTime all accept 0 (epoch / 1970-01-01).
    return "0"


def _select_aligned(
    sink_fields: list[Field],
    src_fields: list[Field],
    dest_types: dict[str, str],
) -> str:
    """Build the MV SELECT list, aligning each sink column to the source.

    For columns the source lacks, fill a placeholder — but honour the destination
    column's real nullability: NULL only where the destination is Nullable, else a
    typed default, so inserts into a pre-existing non-Nullable table don't fail with
    CANNOT_INSERT_NULL_IN_ORDINARY_COLUMN."""
    src_names = {f.name for f in src_fields}
    exprs = []
    for f in sink_fields:
        if f.name in src_names:
            exprs.append(f"`{f.name}`")
            continue
        dest_type = dest_types.get(f.name) or f"Nullable({_ch_type(f.data_type)})"
        if dest_type.startswith("Nullable("):
            exprs.append(f"CAST(NULL AS {dest_type}) AS `{f.name}`")
        else:
            exprs.append(f"CAST({_zero_literal(dest_type)} AS {dest_type}) AS `{f.name}`")
    return ", ".join(exprs)


async def apply_clickhouse_sink(
    node: dict[str, Any],
    edges: list[dict[str, Any]],
    all_nodes: dict[str, dict[str, Any]],
    global_cfg: dict[str, Any],
) -> str:
    """Idempotently ensure the sink table, Kafka-engine source table(s), and
    materialized view(s) exist for a ClickHouse sink node. Returns a summary string."""
    props = node.get("properties") or {}
    label = _safe_identifier(node["label"])
    table = _safe_string(props.get("table", label.lower().replace(" ", "_")))
    overrides: dict[str, dict[str, str]] = props.get("ch_source_overrides") or {}

    ch = global_cfg["clickhouse"]
    database = _safe_string(props.get("database", ch.get("database", "xstream")))
    host = _safe_string(ch.get("host", "localhost"))
    http_port = int(ch.get("http_port", 8123))
    kafka_bootstrap = _safe_string(global_cfg["kafka"]["bootstrap_servers"])

    sink_fields = _fields_for_node(node)
    pk_raw = props.get("primary_key") or []
    pk_fields = pk_raw if isinstance(pk_raw, list) and pk_raw else [sink_fields[0].name]
    pk_set = set(pk_fields)
    order_by = ", ".join(f"`{p}`" for p in pk_fields)
    # ORDER BY / sorting-key columns must NOT be Nullable (MergeTree rejects a
    # nullable key unless allow_nullable_key); everything else stays Nullable so
    # multi-source merges can leave columns unset.
    sink_col_defs = ", ".join(
        f"`{f.name}` {_ch_type(f.data_type)}" if f.name in pk_set
        else f"`{f.name}` Nullable({_ch_type(f.data_type)})"
        for f in sink_fields
    )

    unique_sources = upstream_kafka_sources(node["id"], edges, all_nodes)

    async with httpx.AsyncClient(base_url=f"http://{host}:{http_port}", timeout=30) as client:
        await _exec_ddl(client, f"CREATE DATABASE IF NOT EXISTS `{database}`")
        await _exec_ddl(
            client,
            f"CREATE TABLE IF NOT EXISTS `{database}`.`{table}` ({sink_col_defs}, "
            f"`inserted_at` DateTime DEFAULT now()) ENGINE = MergeTree ORDER BY ({order_by})",
        )

        # Read the destination table's real column types (it may pre-exist with a
        # different — e.g. non-Nullable — schema than the one we'd create above).
        dest_types = await _dest_column_types(client, database, table)

        applied: list[str] = []
        for src_id in unique_sources:
            src_node = all_nodes[src_id]
            src_props = src_node.get("properties") or {}
            src_label = _safe_identifier(src_node["label"])
            topic = _safe_string(src_props.get("topic", src_label.lower().replace(" ", "_")))
            src_fields = _fields_for_node(src_node)

            override = overrides.get(topic) or {}
            src_table = override.get("source_table") or f"kafka_src__{_safe_table_part(topic)}"
            mv_name = override.get("mv_name") or f"mv__{_safe_table_part(topic)}__to__{table}"

            src_col_defs = ", ".join(f"`{f.name}` {_ch_type(f.data_type)}" for f in src_fields)
            await _exec_ddl(
                client,
                f"CREATE TABLE IF NOT EXISTS `{database}`.`{src_table}` ({src_col_defs}) "
                f"ENGINE = Kafka SETTINGS kafka_broker_list = '{kafka_bootstrap}', "
                f"kafka_topic_list = '{topic}', kafka_group_name = 'xstream_{src_table}', "
                f"kafka_format = 'JSONEachRow', kafka_skip_broken_messages = 1",
            )
            select_exprs = _select_aligned(sink_fields, src_fields, dest_types)
            await _exec_ddl(
                client,
                f"CREATE MATERIALIZED VIEW IF NOT EXISTS `{database}`.`{mv_name}` "
                f"TO `{database}`.`{table}` AS SELECT {select_exprs} FROM `{database}`.`{src_table}`",
            )
            applied.append(f"{topic} -> {src_table} -> {mv_name} -> {table}")

    if not applied:
        return f"ClickHouse: table `{table}` ensured, but node has no upstream Kafka source — nothing will flow in."
    return f"ClickHouse: applied {len(applied)} source(s) for `{table}`: " + "; ".join(applied)
