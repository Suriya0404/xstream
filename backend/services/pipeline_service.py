"""
Pipeline execution service: topological ordering and Flink job submission.
"""
from __future__ import annotations

from collections import defaultdict, deque
from datetime import datetime

import structlog
from sqlalchemy.orm import Session

import models
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


def _add_log(db: Session, run_id: int, level: str, message: str) -> None:
    db.add(models.PipelineRunLog(run_id=run_id, level=level, message=message))
    db.commit()


async def execute_pipeline(
    run_id: int,
    node_map: dict[str, dict],
    edge_list: list[dict],
    global_config: dict,
    sql_gateway,
) -> None:
    from database import SessionLocal

    db: Session = SessionLocal()
    try:
        run = db.get(models.PipelineRun, run_id)
        if not run:
            return
        run.status = "running"
        db.commit()

        ordered = topo_sort(node_map, edge_list)
        sql_parts: list[str] = []

        parallelism = global_config.get("flink", {}).get("parallelism", 1)
        sql_parts.append(f"SET 'parallelism.default' = '{parallelism}';")

        for node_id in ordered:
            node = node_map[node_id]
            sql_parts.append(generate_flink_sql(node, edge_list, node_map, global_config))

        full_sql = "\n\n".join(sql_parts)
        log.info("pipeline_sql_submitting", run_id=run_id, sql_preview=full_sql[:500])
        _add_log(db, run_id, "INFO", f"Generated SQL:\n{full_sql}")

        job_id = await sql_gateway.run_pipeline_sql(full_sql)

        run.flink_job_id = job_id or "submitted"
        run.status = "running"
        run.logs = full_sql  # keep for backward compat with existing API response
        db.commit()
        _add_log(db, run_id, "INFO", f"Flink job submitted: {run.flink_job_id}")

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
