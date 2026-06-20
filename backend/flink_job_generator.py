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


def _resolve_jobs_repo() -> Path:
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

    col_lines = "\n".join(f"  {name}  {typ}," for name, typ in fields)
    pk_raw = props.get("primary_key", "")
    if isinstance(pk_raw, list):
        pk = ", ".join(pk_raw)
    else:
        pk = pk_raw or ""
    pk_line = f"\n  PRIMARY KEY ({pk}) NOT ENFORCED," if pk else ""

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
        f"{col_lines}{pk_line}\n"
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


def generate_pipeline_artifacts(
    pipeline_name: str,
    nodes: list[dict[str, Any]],
    edges: list[dict[str, Any]],
) -> tuple[list[Path], Path]:
    """Generate all files for a pipeline. Returns (node_job_paths, workflow_path)."""
    repo = _resolve_jobs_repo()
    node_lookup = {n["id"]: n for n in nodes}
    job_files = [
        generate_node_job(pipeline_name, n, node_lookup, edges, jobs_root=repo)
        for n in nodes
    ]
    workflow_file = generate_workflow_file(pipeline_name, nodes, edges, workflows_root=repo)
    return job_files, workflow_file
