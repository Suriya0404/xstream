"""
Generate per-node PyFlink job Python files and a workflow YAML file for a pipeline.

Output layout:
  flink-jobs/<pipeline_name>/<label>_<node_id>_job.py   — one file per node
  workflows/<pipeline_name>.yaml                        — workflow topology
"""
from __future__ import annotations

import os
import re
from pathlib import Path
from typing import Any

import yaml

from sql_parser import (
    _fields_for_node,
    _safe_string,
    upstream_kafka_sources,
)


def resolve_jobs_repo() -> Path:
    """Return the x-stream-jobs repo root: env JOBS_REPO_PATH → config jobs_repo.path → project root."""
    env_path = os.environ.get("JOBS_REPO_PATH")
    if env_path:
        p = Path(env_path)
        return p if p.is_absolute() else Path(__file__).parent.parent / p

    try:
        import config as cfg  # noqa: PLC0415
        repo_cfg = cfg.get("jobs_repo")
        cfg_path = repo_cfg.get("path") if repo_cfg else None
        if cfg_path:
            p = Path(cfg_path)
            return p if p.is_absolute() else Path(__file__).parent.parent / p
    except Exception:
        pass

    return Path(__file__).parent.parent


# ── Helpers ───────────────────────────────────────────────────────────────────

def _safe(s: str) -> str:
    """Convert a string to a safe filesystem/Python identifier."""
    return re.sub(r"[^a-zA-Z0-9_]", "_", s).lower().strip("_") or "node"


def _handle_index(handle: str) -> int:
    """Parse the numeric index from a handle id like 'out-3' or 'in-0'."""
    try:
        return int(handle.rsplit("-", 1)[-1])
    except (ValueError, IndexError):
        return 0


def _get_fields(node: dict[str, Any]) -> list[tuple[str, str]]:
    """Return [(field_name, field_type), ...] for a node."""
    props = node.get("properties") or {}
    handles = props.get("_handles") or []
    return [(row[0], row[1] if len(row) > 1 else "STRING") for row in handles]


# ── PyFlink job template ──────────────────────────────────────────────────────

_JOB_TEMPLATE = '''\
"""
Flink job for node: {label}
Type      : {node_type}
Pipeline  : {pipeline_name}
Node ID   : {node_id}
{pk_comment}\
{mappings_comment}\
"""
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, StreamTableEnvironment


FLINK_SQL = """
{flink_sql}
"""


def get_table_env() -> StreamTableEnvironment:
    env = StreamExecutionEnvironment.get_execution_environment()
    settings = EnvironmentSettings.in_streaming_mode()
    return StreamTableEnvironment.create(env, environment_settings=settings)


def register_table(t_env: StreamTableEnvironment) -> None:
    """Register this node\'s table/connector with the given table environment."""
    t_env.execute_sql(FLINK_SQL.strip())


if __name__ == "__main__":
    t_env = get_table_env()
    register_table(t_env)
'''


def _render_job(
    pipeline_name: str,
    node: dict[str, Any],
    field_mappings_comment: str,
) -> str:
    props = node.get("properties") or {}
    primary_key = props.get("primary_key") or ""
    flink_sql = (node.get("flink_sql") or "").strip()

    if not flink_sql:
        flink_sql = _fallback_sql(node)

    if isinstance(primary_key, list):
        pk_str = ", ".join(primary_key)
    else:
        pk_str = primary_key or ""
    pk_comment = f"Primary key: {pk_str}\n" if pk_str else ""

    return _JOB_TEMPLATE.format(
        label=node.get("label", node["id"]),
        node_type=node.get("node_type", "kafka"),
        pipeline_name=pipeline_name,
        node_id=node["id"],
        pk_comment=pk_comment,
        mappings_comment=field_mappings_comment,
        flink_sql=flink_sql,
    )


def _fallback_sql(node: dict[str, Any]) -> str:
    """Generate a minimal CREATE TABLE SQL when the node has no flink_sql set."""
    node_type = node.get("node_type", "kafka")
    label = _safe(node.get("label", node["id"]))
    props = node.get("properties") or {}
    fields = _get_fields(node)

    pk_raw = props.get("primary_key", "")
    if isinstance(pk_raw, list):
        pk = ", ".join(pk_raw)
    else:
        pk = pk_raw or ""

    col_parts = [f"  {name}  {typ}" for name, typ in fields]
    if pk:
        col_parts.append(f"  PRIMARY KEY ({pk}) NOT ENFORCED")
    col_lines = ",\n".join(col_parts)

    if node_type == "kafka":
        with_block = (
            f"  'connector' = 'kafka',\n"
            f"  'topic'     = '{props.get('topic', label)}',\n"
            f"  'properties.bootstrap.servers' = '{props.get('bootstrap_servers', 'localhost:9092')}',\n"
            f"  'format'    = '{props.get('format', 'json')}'"
        )
    elif node_type == "scylladb":
        with_block = (
            f"  'connector' = 'cassandra',\n"
            f"  'host'      = '{props.get('host', 'localhost')}',\n"
            f"  'keyspace'  = '{props.get('keyspace', 'xstream')}',\n"
            f"  'table'     = '{props.get('table', label)}'"
        )
    else:  # clickhouse
        host = props.get("host", "localhost")
        database = props.get("database", "xstream")
        table = props.get("table", label)
        with_block = (
            f"  'connector'  = 'jdbc',\n"
            f"  'url'        = 'jdbc:clickhouse://{host}:9000/{database}',\n"
            f"  'table-name' = '{table}'"
        )

    return (
        f"CREATE TABLE {label} (\n"
        f"{col_lines}\n"
        f") WITH (\n"
        f"{with_block}\n"
        f");"
    )


# ── Public API ────────────────────────────────────────────────────────────────

def generate_node_job(
    pipeline_name: str,
    node: dict[str, Any],
    node_lookup: dict[str, dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    jobs_root: Path,
) -> Path:
    """Write a PyFlink job file for one node; return its path."""
    flink_jobs_dir = jobs_root / "flink-jobs"
    flink_jobs_dir.mkdir(parents=True, exist_ok=True)
    pipeline_dir = flink_jobs_dir / _safe(pipeline_name)
    pipeline_dir.mkdir(exist_ok=True)

    node_id = node["id"]
    label = node.get("label", node_id)
    props = node.get("properties") or {}
    fields = _get_fields(node)
    field_mappings = props.get("field_mappings") or {}

    # Build field-mappings comment for the file header
    mapping_lines: list[str] = []
    for idx, (field_name, _) in enumerate(fields):
        mapping = field_mappings.get(str(idx))
        if mapping:
            src_node = node_lookup.get(mapping.get("source_node_id", ""))
            src_label = src_node.get("label", mapping["source_node_id"]) if src_node else mapping.get("source_node_id", "?")
            mapping_lines.append(f"  {field_name} <- {src_label}.{mapping.get('source_field', '?')}")
        else:
            # Try to infer from edges
            in_edge = next(
                (e for e in edges
                 if e.get("target_id") == node_id
                 and _handle_index(e.get("target_handle") or "in-999") == idx),
                None,
            )
            if in_edge:
                src_node = node_lookup.get(in_edge["source_id"])
                src_label = src_node.get("label", in_edge["source_id"]) if src_node else in_edge["source_id"]
                src_fields = _get_fields(src_node) if src_node else []
                src_idx = _handle_index(in_edge.get("source_handle") or "out-0")
                src_field = src_fields[src_idx][0] if src_idx < len(src_fields) else f"field_{src_idx}"
                mapping_lines.append(f"  {field_name} <- {src_label}.{src_field}")

    mappings_comment = ""
    if mapping_lines:
        mappings_comment = "Field mappings:\n" + "\n".join(mapping_lines) + "\n"

    content = _render_job(pipeline_name, node, mappings_comment)
    filename = f"{_safe(label)}_{_safe(node_id)}_job.py"
    filepath = pipeline_dir / filename
    filepath.write_text(content)
    return filepath


def generate_workflow_file(
    pipeline_name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    *,
    workflows_root: Path,
) -> Path:
    """Write a YAML workflow file describing the full pipeline topology; return its path."""
    workflows_dir = workflows_root / "workflows"
    workflows_dir.mkdir(parents=True, exist_ok=True)
    filepath = workflows_dir / f"{_safe(pipeline_name)}.yaml"

    node_lookup = {n["id"]: n for n in nodes}

    workflow_nodes: list[dict] = []
    for node in nodes:
        props = node.get("properties") or {}
        fields = _get_fields(node)
        field_mappings = props.get("field_mappings") or {}

        field_defs: list[dict] = []
        for idx, (fname, ftype) in enumerate(fields):
            fd: dict[str, Any] = {"name": fname, "type": ftype}
            mapping = field_mappings.get(str(idx))
            if mapping:
                src_node = node_lookup.get(mapping.get("source_node_id", ""))
                fd["mapped_from"] = {
                    "node_id": mapping["source_node_id"],
                    "node_label": src_node.get("label") if src_node else mapping["source_node_id"],
                    "field": mapping.get("source_field"),
                }
            field_defs.append(fd)

        safe_label = _safe(node.get("label", node["id"]))
        safe_id = _safe(node["id"])
        job_file = f"flink-jobs/{_safe(pipeline_name)}/{safe_label}_{safe_id}_job.py"

        clean_props = {
            k: v for k, v in props.items()
            if k not in ("_handles", "field_mappings")
        }

        wn: dict[str, Any] = {
            "id": node["id"],
            "label": node.get("label"),
            "type": node.get("node_type"),
            "job_file": job_file,
        }
        if clean_props:
            wn["properties"] = clean_props
        pk = props.get("primary_key")
        if pk:
            wn["primary_key"] = pk if isinstance(pk, list) else [pk]
        if field_defs:
            wn["fields"] = field_defs

        workflow_nodes.append(wn)

    workflow_edges: list[dict] = []
    for edge in edges:
        src_node = node_lookup.get(edge.get("source_id", ""))
        tgt_node = node_lookup.get(edge.get("target_id", ""))
        src_fields = _get_fields(src_node) if src_node else []
        tgt_fields = _get_fields(tgt_node) if tgt_node else []

        src_idx = _handle_index(edge.get("source_handle") or "out-0")
        tgt_idx = _handle_index(edge.get("target_handle") or "in-0")

        src_field = src_fields[src_idx][0] if src_idx < len(src_fields) else f"field_{src_idx}"
        tgt_field = tgt_fields[tgt_idx][0] if tgt_idx < len(tgt_fields) else f"field_{tgt_idx}"

        workflow_edges.append({
            "id": edge.get("id"),
            "from": {
                "node_id": edge["source_id"],
                "node_label": src_node.get("label") if src_node else edge["source_id"],
                "field": src_field,
            },
            "to": {
                "node_id": edge["target_id"],
                "node_label": tgt_node.get("label") if tgt_node else edge["target_id"],
                "field": tgt_field,
            },
        })

    workflow: dict[str, Any] = {
        "name": pipeline_name,
        "nodes": workflow_nodes,
        "edges": workflow_edges,
    }
    filepath.write_text(
        yaml.dump(workflow, default_flow_style=False, sort_keys=False, allow_unicode=True)
    )
    return filepath


# ── Runnable per-pipeline PyFlink job (StatementSet) ──────────────────────────

_PIPELINE_JOB_TEMPLATE = '''\
"""
Flink pipeline job for: {pipeline_name}

Generated by x-stream. This is ONE runnable Flink job for the whole pipeline:
it registers every source + sink table and runs all INSERTs together in a single
StatementSet, so every source/sink operator runs concurrently in one JobGraph.

Sources: {source_labels}
Sinks  : {sink_labels}
"""
from pyflink.table import EnvironmentSettings, TableEnvironment


DDLS = [
{ddl_entries}
]

INSERTS = [
{insert_entries}
]


def build(t_env: TableEnvironment) -> None:
    for ddl in DDLS:
        t_env.execute_sql(ddl)
    stmt_set = t_env.create_statement_set()
    for insert in INSERTS:
        stmt_set.add_insert_sql(insert)
    stmt_set.execute()


if __name__ == "__main__":
    t_env = TableEnvironment.create(EnvironmentSettings.in_streaming_mode())
    t_env.get_config().set("parallelism.default", "{parallelism}")
    # Bound aggregate state growth for the merge (idle keys expire after 1h).
    t_env.get_config().set("table.exec.state.ttl", "3600000")
    build(t_env)
'''


def _flink_kafka_source_ddl(node: dict[str, Any], global_config: dict, table: str) -> str:
    props = node.get("properties") or {}
    fields = _fields_for_node(node)
    topic = _safe_string(props.get("topic", table))
    bootstrap = _safe_string((global_config.get("kafka") or {}).get("bootstrap_servers", "kafka:29092"))
    group_id = _safe_string(props.get("group_id", f"xstream_{topic}"))
    fmt = _safe_string(props.get("format", "json"))
    col_defs = ",\n  ".join(f"`{f.name}` {f.data_type}" for f in fields)
    return (
        f"CREATE TABLE `{table}` (\n  {col_defs}\n) WITH (\n"
        f"  'connector' = 'kafka',\n"
        f"  'topic' = '{topic}',\n"
        f"  'properties.bootstrap.servers' = '{bootstrap}',\n"
        f"  'properties.group.id' = '{group_id}',\n"
        f"  'scan.startup.mode' = 'earliest-offset',\n"
        f"  'format' = '{fmt}'\n"
        f")"
    )


def _sink_pk(node: dict[str, Any], sink_fields: list) -> list[str]:
    pk_raw = (node.get("properties") or {}).get("primary_key") or []
    if isinstance(pk_raw, list) and pk_raw:
        return pk_raw
    return [sink_fields[0].name] if sink_fields else ["id"]


def _flink_mongo_sink_ddl(node: dict[str, Any], global_config: dict, table: str) -> str:
    props = node.get("properties") or {}
    mongo = global_config.get("mongodb") or {}
    uri = _safe_string(props.get("uri") or mongo.get("uri", "mongodb://mongo:27017"))
    database = _safe_string(props.get("database") or mongo.get("database", "xstream"))
    collection = _safe_string(
        props.get("collection") or props.get("table") or _safe(node.get("label", node["id"]))
    )
    fields = _fields_for_node(node)
    pk_cols = _sink_pk(node, fields)
    col_defs = ",\n  ".join(f"`{f.name}` {f.data_type}" for f in fields)
    pk_line = f",\n  PRIMARY KEY ({', '.join(f'`{p}`' for p in pk_cols)}) NOT ENFORCED"
    return (
        f"CREATE TABLE `{table}` (\n  {col_defs}{pk_line}\n) WITH (\n"
        f"  'connector' = 'mongodb',\n"
        f"  'uri' = '{uri}',\n"
        f"  'database' = '{database}',\n"
        f"  'collection' = '{collection}'\n"
        f")"
    )


def _projection_select(
    sink_fields: list,
    src_id: str,
    src_node: dict[str, Any],
    field_mappings: dict,
    src_table: str,
) -> str:
    """One source's SELECT, projected to the sink's full column set (NULL for cols
    this source doesn't supply) so all sources are UNION-compatible."""
    src_names = {f.name for f in _fields_for_node(src_node)}
    cols: list[str] = []
    for idx, f in enumerate(sink_fields):
        mapping = field_mappings.get(str(idx)) or {}
        if mapping.get("source_node_id") == src_id and mapping.get("source_field") in src_names:
            expr = f"`{mapping['source_field']}`"
        elif f.name in src_names:
            expr = f"`{f.name}`"
        else:
            expr = f"CAST(NULL AS {f.data_type})"
        cols.append(f"{expr} AS `{f.name}`")
    return f"SELECT {', '.join(cols)} FROM `{src_table}`"


def _merge_insert(sink_table: str, sink_fields: list, pk_cols: list[str], projections: list[str]) -> str:
    """INSERT that merges N source projections into the sink, keyed by PK.

    Single source → direct INSERT. Multiple sources → UNION ALL then GROUP BY pk
    with LAST_VALUE(col) FILTER (WHERE col IS NOT NULL), i.e. the latest non-null
    value per column, so different topics fill different columns without clobbering."""
    if len(projections) == 1:
        return f"INSERT INTO `{sink_table}`\n{projections[0]}"

    pk_set = set(pk_cols)
    select_cols: list[str] = []
    for f in sink_fields:
        if f.name in pk_set:
            select_cols.append(f"`{f.name}`")
        else:
            select_cols.append(
                f"LAST_VALUE(`{f.name}`) FILTER (WHERE `{f.name}` IS NOT NULL) AS `{f.name}`"
            )
    union = "\n  UNION ALL\n  ".join(projections)
    group_by = ", ".join(f"`{p}`" for p in pk_cols)
    return (
        f"INSERT INTO `{sink_table}`\n"
        f"SELECT {', '.join(select_cols)}\n"
        f"FROM (\n  {union}\n)\n"
        f"GROUP BY {group_by}"
    )


def generate_pipeline_job(
    pipeline_name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    global_config: dict,
    *,
    jobs_root: Path | None = None,
) -> Path:
    """Write one runnable per-pipeline PyFlink StatementSet job; return its path.

    This milestone handles Kafka sources and MongoDB sinks. Other sink types are
    skipped here (they keep their existing native paths)."""
    from core.constants import NodeType  # noqa: PLC0415

    jobs_root = jobs_root or resolve_jobs_repo()
    node_lookup = {n["id"]: n for n in nodes}
    flink_dir = jobs_root / "flink-jobs" / _safe(pipeline_name)
    flink_dir.mkdir(parents=True, exist_ok=True)

    sources = [n for n in nodes if n.get("node_type") == NodeType.KAFKA]
    sinks = [n for n in nodes if n.get("node_type") == NodeType.MONGODB]

    ddls: list[str] = []
    src_table_of: dict[str, str] = {}
    for s in sources:
        table = _safe(s.get("label", s["id"]))
        src_table_of[s["id"]] = table
        ddls.append(_flink_kafka_source_ddl(s, global_config, table))

    inserts: list[str] = []
    for sink in sinks:
        sink_table = _safe(sink.get("label", sink["id"]))
        ddls.append(_flink_mongo_sink_ddl(sink, global_config, sink_table))

        sink_fields = _fields_for_node(sink)
        props = sink.get("properties") or {}
        field_mappings = props.get("field_mappings") or {}
        pk_cols = _sink_pk(sink, sink_fields)

        src_ids = [sid for sid in upstream_kafka_sources(sink["id"], edges, node_lookup)
                   if sid in src_table_of]
        if not src_ids:
            continue
        projections = [
            _projection_select(sink_fields, sid, node_lookup[sid], field_mappings, src_table_of[sid])
            for sid in src_ids
        ]
        inserts.append(_merge_insert(sink_table, sink_fields, pk_cols, projections))

    parallelism = (global_config.get("flink") or {}).get("parallelism", 1)
    ddl_entries = ",\n".join(f'    """{d}"""' for d in ddls)
    insert_entries = ",\n".join(f'    """{i}"""' for i in inserts)
    content = _PIPELINE_JOB_TEMPLATE.format(
        pipeline_name=pipeline_name,
        source_labels=", ".join(s.get("label", s["id"]) for s in sources) or "(none)",
        sink_labels=", ".join(s.get("label", s["id"]) for s in sinks) or "(none)",
        ddl_entries=ddl_entries,
        insert_entries=insert_entries,
        parallelism=parallelism,
    )
    filepath = flink_dir / f"{_safe(pipeline_name)}_pipeline_job.py"
    filepath.write_text(content)
    return filepath


def generate_pipeline_artifacts(
    pipeline_name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
    global_config: dict | None = None,
) -> tuple[list[Path], Path]:
    """Generate all files for a pipeline. Returns (job_paths, workflow_path).

    Writes the runnable per-pipeline StatementSet job (the file Run launches) plus
    the topology workflow YAML. Falls back to loading config if not supplied."""
    if global_config is None:
        import config as cfg  # noqa: PLC0415
        global_config = cfg.load_config()

    repo = resolve_jobs_repo()
    pipeline_job = generate_pipeline_job(pipeline_name, nodes, edges, global_config, jobs_root=repo)
    workflow_file = generate_workflow_file(pipeline_name, nodes, edges, workflows_root=repo)
    return [pipeline_job], workflow_file
