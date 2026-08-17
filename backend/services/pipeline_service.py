"""
Pipeline execution service: topological ordering and Flink job submission.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime

import structlog
from sqlalchemy.orm import Session

import models
from clickhouse_sink import apply_clickhouse_sink
from core.constants import NodeType
from scylla_sink_config import apply_scylla_sink
from sql_parser import generate_flink_sql

log = structlog.get_logger()


def topo_sort(node_map: dict, edge_list: list[dict]) -> list[str]:
    """Kahn's algorithm topological sort — returns node IDs sources-first."""
    in_deg: dict[str, int] = {nid: 0 for nid in node_map}
    adj: dict[str, list[str]] = defaultdict(list)
    for e in edge_list:
        if e["source_id"] in node_map and e["target_id"] in node_map:
            adj[e["source_id"]].append(e["target_id"])
            in_deg[e["target_id"]] += 1
    queue = deque(nid for nid, deg in in_deg.items() if deg == 0)
    result: list[str] = []
    while queue:
        nid = queue.popleft()
        result.append(nid)
        for child in adj[nid]:
            in_deg[child] -= 1
            if in_deg[child] == 0:
                queue.append(child)
    return result


def _add_log(
    db: Session,
    run_id: int,
    level: str,
    message: str,
    node_id: str | None = None,
    node_label: str | None = None,
) -> None:
    db.add(models.PipelineRunLog(
        run_id=run_id, level=level, message=message,
        node_id=node_id, node_label=node_label,
    ))
    db.commit()


async def _execute_native_sink(
    db: Session,
    run_id: int,
    node: dict,
    edge_list: list[dict],
    node_map: dict[str, dict],
    global_config: dict,
) -> None:
    """Apply a ScyllaDB/ClickHouse sink via its native path (deferred from Flink)."""
    node_id = node["id"]
    label = node.get("label", node_id)
    ntype = node["node_type"]
    _add_log(db, run_id, "INFO", f"Starting {ntype} node '{label}'…", node_id, label)
    if ntype == NodeType.CLICKHOUSE:
        summary = await apply_clickhouse_sink(node, edge_list, node_map, global_config)
    else:
        summary = await apply_scylla_sink(node, edge_list, node_map, global_config)
    _add_log(db, run_id, "INFO", summary, node_id, label)
    _add_log(db, run_id, "INFO", f"✓ {label} ready", node_id, label)


async def _run_via_flink(
    db: Session,
    run_id: int,
    pipeline_name: str,
    node_map: dict[str, dict],
    edge_list: list[dict],
    global_config: dict,
) -> None:
    """Generate the per-pipeline PyFlink job and launch it with `flink run -py`.
    Kafka sources + MongoDB sinks run inside the one Flink job; ScyllaDB/ClickHouse
    nodes (if any) still go through their native paths."""
    from services import job_launcher
    from flink_job_generator import generate_pipeline_job

    ordered = topo_sort(node_map, edge_list)
    flink_types = {NodeType.KAFKA, NodeType.MONGODB}
    flink_ids = [nid for nid in ordered if node_map[nid]["node_type"] in flink_types]
    native_ids = [nid for nid in ordered
                  if node_map[nid]["node_type"] in (NodeType.SCYLLADB, NodeType.CLICKHOUSE)]

    _add_log(db, run_id, "INFO", f"Pipeline run started — Flink job for {len(flink_ids)} node(s).")

    any_failed = False

    # Native sinks (deferred types) keep their existing behavior.
    for nid in native_ids:
        node = node_map[nid]
        label = node.get("label", nid)
        try:
            await _execute_native_sink(db, run_id, node, edge_list, node_map, global_config)
        except Exception as exc:
            any_failed = True
            log.error("native_sink_failed", run_id=run_id, node_id=nid, error=str(exc))
            _add_log(db, run_id, "ERROR", f"✗ {label} failed: {exc}", nid, label)

    for nid in flink_ids:
        node = node_map[nid]
        _add_log(db, run_id, "INFO",
                 f"Starting {node['node_type']} node '{node.get('label', nid)}'…", nid, node.get("label", nid))

    job_id = ""
    try:
        nodes = list(node_map.values())
        job_file = generate_pipeline_job(pipeline_name, nodes, edge_list, global_config)
        _add_log(db, run_id, "INFO", f"Generated Flink job: {job_file.name}")
        # Clear any prior RUNNING state for this pipeline's job (best-effort).
        try:
            await job_launcher.stop(job_file)
        except Exception:
            pass
        _add_log(db, run_id, "INFO", "Submitting Flink job (flink run -py)…")
        state = await job_launcher.launch(job_file)
        job_id = state.get("job_id") or ""
        _add_log(db, run_id, "INFO", f"Flink job accepted (job_id={job_id or 'n/a'}).")
        for nid in flink_ids:
            label = node_map[nid].get("label", nid)
            _add_log(db, run_id, "INFO", f"✓ {label} ready", nid, label)
    except Exception as exc:
        any_failed = True
        log.error("flink_launch_failed", run_id=run_id, error=str(exc))
        _add_log(db, run_id, "ERROR", f"✗ Flink job launch failed: {exc}")

    run = db.get(models.PipelineRun, run_id)
    run.flink_job_id = job_id or "submitted"
    if any_failed:
        run.status = "failed"
        run.finished_at = datetime.utcnow()
        run.logs = "Pipeline finished with errors — see per-node logs."
        db.commit()
        _add_log(db, run_id, "ERROR", "Pipeline finished with errors — see per-node logs.")
    else:
        run.status = "running"
        run.logs = "Flink job submitted successfully."
        db.commit()
        _add_log(db, run_id, "INFO", f"Flink job submitted successfully (job_id={run.flink_job_id}).")


async def _execute_node(
    db: Session,
    run_id: int,
    node: dict,
    edge_list: list[dict],
    node_map: dict[str, dict],
    global_config: dict,
    sql_gateway,
) -> str:
    """[Legacy gateway path] Run a single node's setup and stream per-node logs."""
    node_id = node["id"]
    label = node.get("label", node_id)
    ntype = node["node_type"]

    _add_log(db, run_id, "INFO", f"Starting {ntype} node '{label}'…", node_id, label)

    if ntype in (NodeType.CLICKHOUSE, NodeType.SCYLLADB):
        await _execute_native_sink(db, run_id, node, edge_list, node_map, global_config)
        return ""

    node_sql = generate_flink_sql(node, edge_list, node_map, global_config)
    _add_log(db, run_id, "DEBUG", f"Submitting SQL:\n{node_sql}", node_id, label)
    job_id = await sql_gateway.run_pipeline_sql(node_sql)
    _add_log(db, run_id, "INFO", f"Flink statement accepted (job_id={job_id or 'n/a'})", node_id, label)
    _add_log(db, run_id, "INFO", f"✓ {label} ready", node_id, label)
    return job_id


async def _run_via_gateway(
    db: Session,
    run_id: int,
    node_map: dict[str, dict],
    edge_list: list[dict],
    global_config: dict,
    sql_gateway,
) -> None:
    """[Legacy path] Submit per-node Kafka SQL to the SQL Gateway; native sinks run
    natively. Used for pipelines without a MongoDB (Flink-launched) sink."""
    ordered = topo_sort(node_map, edge_list)
    _add_log(db, run_id, "INFO", f"Pipeline run started — {len(ordered)} node(s).")

    parallelism = global_config.get("flink", {}).get("parallelism", 1)
    try:
        await sql_gateway.execute(f"SET 'parallelism.default' = '{parallelism}';")
        _add_log(db, run_id, "INFO", f"Set parallelism.default = {parallelism}")
    except Exception as exc:
        _add_log(db, run_id, "WARNING", f"Could not set parallelism: {exc}")

    any_failed = False
    last_job_id = ""
    for node_id in ordered:
        node = node_map[node_id]
        label = node.get("label", node_id)
        try:
            job_id = await _execute_node(db, run_id, node, edge_list, node_map, global_config, sql_gateway)
            if job_id:
                last_job_id = job_id
        except Exception as exc:
            any_failed = True
            log.error("node_execution_failed", run_id=run_id, node_id=node_id, error=str(exc))
            _add_log(db, run_id, "ERROR", f"✗ {label} failed: {exc}", node_id, label)

    run = db.get(models.PipelineRun, run_id)
    run.flink_job_id = last_job_id or "submitted"
    if any_failed:
        run.status = "failed"
        run.finished_at = datetime.utcnow()
        run.logs = "Pipeline finished with errors — see per-node logs."
        db.commit()
        _add_log(db, run_id, "ERROR", "Pipeline finished with errors — see per-node logs.")
    else:
        run.status = "running"
        run.logs = "Pipeline submitted successfully."
        db.commit()
        _add_log(db, run_id, "INFO", f"Pipeline submitted successfully (job_id={run.flink_job_id}).")


async def execute_pipeline(
    run_id: int,
    pipeline_name: str,
    node_map: dict[str, dict],
    edge_list: list[dict],
    global_config: dict,
    sql_gateway,
) -> None:
    """Execute a pipeline run. Pipelines with a MongoDB sink are compiled to one
    runnable PyFlink job and launched via `flink run -py`; others use the legacy
    SQL-Gateway path (preserving existing ScyllaDB/ClickHouse pipelines)."""
    from database import SessionLocal

    db: Session = SessionLocal()
    try:
        run = db.get(models.PipelineRun, run_id)
        if not run:
            return
        run.status = "running"
        db.commit()

        has_mongo = any(n["node_type"] == NodeType.MONGODB for n in node_map.values())
        if has_mongo:
            await _run_via_flink(db, run_id, pipeline_name, node_map, edge_list, global_config)
        else:
            await _run_via_gateway(db, run_id, node_map, edge_list, global_config, sql_gateway)

    except Exception as exc:
        log.error("pipeline_run_failed", run_id=run_id, error=str(exc))
        run = db.get(models.PipelineRun, run_id)
        if run:
            _add_log(db, run_id, "ERROR", str(exc))
            run.status = "failed"
            run.logs = str(exc)
            run.finished_at = datetime.utcnow()
            db.commit()
    finally:
        db.close()
