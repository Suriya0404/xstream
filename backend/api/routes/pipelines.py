"""
Pipeline CRUD, run, and status endpoints.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

import structlog
from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

import config as cfg
import models
from database import get_db
from services.pipeline_service import execute_pipeline

log = structlog.get_logger()

router = APIRouter(prefix="/api")


# ── Pydantic schemas ──────────────────────────────────────

class NodeIn(BaseModel):
    id: str
    node_type: str
    label: str
    pos_x: float = 0
    pos_y: float = 0
    flink_sql: Optional[str] = None
    properties: Optional[dict] = None


class EdgeIn(BaseModel):
    id: str
    source_id: str
    target_id: str
    source_handle: Optional[str] = None
    target_handle: Optional[str] = None


class PipelineIn(BaseModel):
    name: str = "default"
    nodes: list[NodeIn]
    edges: list[EdgeIn]


class ParseSQLRequest(BaseModel):
    sql: str


# ── Helpers ───────────────────────────────────────────────

def _get_or_create_pipeline(db: Session, name: str) -> models.Pipeline:
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        pipeline = models.Pipeline(name=name)
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)
    return pipeline


# ── Routes ────────────────────────────────────────────────

@router.post("/parse-sql")
def parse_sql(req: ParseSQLRequest):
    from sql_parser import parse_fields
    fields = parse_fields(req.sql)
    return {"fields": [{"name": f.name, "data_type": f.data_type} for f in fields]}


@router.get("/pipelines")
def list_pipelines(db: Session = Depends(get_db)):
    pipelines = (
        db.query(models.Pipeline)
        .order_by(models.Pipeline.updated_at.desc())
        .all()
    )
    result = []
    for p in pipelines:
        latest_run = (
            db.query(models.PipelineRun)
            .filter_by(pipeline_id=p.id)
            .order_by(models.PipelineRun.id.desc())
            .first()
        )
        node_count = db.query(models.Node).filter_by(pipeline_id=p.id).count()
        result.append({
            "id": p.id,
            "name": p.name,
            "created_at": p.created_at,
            "updated_at": p.updated_at,
            "node_count": node_count,
            "last_run_status": latest_run.status if latest_run else None,
            "last_run_at": latest_run.started_at if latest_run else None,
        })
    return result


@router.get("/pipeline/{name}")
def get_pipeline(name: str, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        return {"nodes": [], "edges": []}
    nodes = db.query(models.Node).filter_by(pipeline_id=pipeline.id).all()
    edges = db.query(models.Edge).filter_by(pipeline_id=pipeline.id).all()
    return {
        "id": pipeline.id,
        "name": pipeline.name,
        "nodes": [
            {
                "id": n.id, "node_type": n.node_type, "label": n.label,
                "pos_x": n.pos_x, "pos_y": n.pos_y,
                "flink_sql": n.flink_sql, "properties": n.properties,
            }
            for n in nodes
        ],
        "edges": [
            {
                "id": e.id, "source_id": e.source_id, "target_id": e.target_id,
                "source_handle": e.source_handle, "target_handle": e.target_handle,
            }
            for e in edges
        ],
    }


@router.post("/pipeline")
def save_pipeline(payload: PipelineIn, db: Session = Depends(get_db)):
    pipeline = _get_or_create_pipeline(db, payload.name)
    db.query(models.Node).filter_by(pipeline_id=pipeline.id).delete()
    db.query(models.Edge).filter_by(pipeline_id=pipeline.id).delete()

    for n in payload.nodes:
        db.add(models.Node(
            id=n.id, pipeline_id=pipeline.id, node_type=n.node_type,
            label=n.label, pos_x=n.pos_x, pos_y=n.pos_y,
            flink_sql=n.flink_sql, properties=n.properties,
        ))
    for e in payload.edges:
        db.add(models.Edge(
            id=e.id, pipeline_id=pipeline.id,
            source_id=e.source_id, target_id=e.target_id,
            source_handle=e.source_handle, target_handle=e.target_handle,
        ))
    db.commit()

    # Generate per-node Flink job files and workflow YAML (best-effort)
    job_files: list[str] = []
    workflow_file: str | None = None
    artifact_error: str | None = None
    try:
        from flink_job_generator import generate_pipeline_artifacts, resolve_jobs_repo
        nodes_data = [
            {"id": n.id, "node_type": n.node_type, "label": n.label,
             "flink_sql": n.flink_sql, "properties": n.properties}
            for n in payload.nodes
        ]
        edges_data = [
            {"id": e.id, "source_id": e.source_id, "target_id": e.target_id,
             "source_handle": e.source_handle, "target_handle": e.target_handle}
            for e in payload.edges
        ]
        jobs_root = resolve_jobs_repo()
        log.info("generating_pipeline_artifacts", pipeline=payload.name, jobs_root=str(jobs_root))
        generated_jobs, generated_workflow = generate_pipeline_artifacts(payload.name, nodes_data, edges_data)
        job_files = [str(p) for p in generated_jobs]
        workflow_file = str(generated_workflow)
        log.info("pipeline_artifacts_generated", pipeline=payload.name,
                 job_count=len(job_files), workflow=workflow_file)
    except Exception as exc:
        artifact_error = str(exc)
        log.error("pipeline_artifact_generation_failed", pipeline=payload.name, error=artifact_error, exc_info=True)

    return {
        "status": "saved",
        "pipeline_id": pipeline.id,
        "job_files": job_files,
        "workflow_file": workflow_file,
        "artifact_error": artifact_error,
    }


@router.post("/pipeline/{name}/run")
async def run_pipeline(
    name: str,
    request: Request,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
):
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    nodes = db.query(models.Node).filter_by(pipeline_id=pipeline.id).all()
    edges = db.query(models.Edge).filter_by(pipeline_id=pipeline.id).all()
    if not nodes:
        raise HTTPException(status_code=400, detail="Pipeline has no nodes")

    run = models.PipelineRun(
        pipeline_id=pipeline.id,
        status="pending",
        started_at=datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)

    node_map = {
        n.id: {"id": n.id, "node_type": n.node_type, "label": n.label,
               "flink_sql": n.flink_sql, "properties": n.properties}
        for n in nodes
    }
    edge_list = [{"id": e.id, "source_id": e.source_id, "target_id": e.target_id} for e in edges]
    global_config = cfg.load_config()
    sql_gateway = request.app.state.sql_gateway

    background_tasks.add_task(
        execute_pipeline, run.id, name, node_map, edge_list, global_config, sql_gateway
    )
    return {"status": "queued", "run_id": run.id}


@router.get("/pipeline/{name}/runs")
def get_runs(name: str, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        return []
    runs = (
        db.query(models.PipelineRun)
        .filter_by(pipeline_id=pipeline.id)
        .order_by(models.PipelineRun.id.desc())
        .limit(20)
        .all()
    )
    return [
        {
            "id": r.id, "status": r.status, "flink_job_id": r.flink_job_id,
            "started_at": r.started_at, "finished_at": r.finished_at, "logs": r.logs,
        }
        for r in runs
    ]


@router.get("/pipeline/{name}/runs/{run_id}/logs")
def get_run_logs(
    name: str,
    run_id: int,
    node_id: Optional[str] = None,
    db: Session = Depends(get_db),
):
    """Return structured log rows for a run. Pass ?node_id=<id> to filter to one node."""
    q = db.query(models.PipelineRunLog).filter_by(run_id=run_id)
    if node_id is not None:
        q = q.filter(models.PipelineRunLog.node_id == node_id)
    logs = q.order_by(models.PipelineRunLog.id.asc()).all()
    return [
        {
            "id": l.id, "level": l.level, "message": l.message, "timestamp": l.timestamp,
            "node_id": l.node_id, "node_label": l.node_label,
        }
        for l in logs
    ]


def _derive_node_status(node_logs: list) -> str:
    """Infer a node's status from its log rows.
    failed > success (✓ marker) > running (started, not done) > pending (no logs)."""
    if not node_logs:
        return "pending"
    if any(l.level == "ERROR" for l in node_logs):
        return "failed"
    if any((l.message or "").startswith("✓") for l in node_logs):
        return "success"
    return "running"


@router.get("/pipeline/{name}/runs/{run_id}/summary")
def get_run_summary(name: str, run_id: int, db: Session = Depends(get_db)):
    """Per-node status summary for a run — drives the Summary tab in the log pane."""
    from collections import defaultdict

    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")
    run = db.get(models.PipelineRun, run_id)

    nodes = db.query(models.Node).filter_by(pipeline_id=pipeline.id).all()
    logs = (
        db.query(models.PipelineRunLog)
        .filter_by(run_id=run_id)
        .order_by(models.PipelineRunLog.id.asc())
        .all()
    )
    by_node: dict[str, list] = defaultdict(list)
    for l in logs:
        if l.node_id:
            by_node[l.node_id].append(l)

    node_summaries = []
    for n in nodes:
        nlogs = by_node.get(n.id, [])
        last = nlogs[-1] if nlogs else None
        node_summaries.append({
            "node_id": n.id,
            "node_label": n.label,
            "node_type": n.node_type,
            "status": _derive_node_status(nlogs),
            "last_level": last.level if last else None,
            "last_message": last.message if last else None,
            "timestamp": last.timestamp if last else None,
            "log_count": len(nlogs),
            "error_count": sum(1 for x in nlogs if x.level == "ERROR"),
        })

    return {
        "run_id": run_id,
        "run_status": run.status if run else None,
        "flink_job_id": run.flink_job_id if run else None,
        "nodes": node_summaries,
    }


@router.post("/pipeline/{name}/stop")
async def stop_pipeline(name: str, request: Request, db: Session = Depends(get_db)):
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        raise HTTPException(status_code=404, detail="Pipeline not found")

    run = (
        db.query(models.PipelineRun)
        .filter_by(pipeline_id=pipeline.id, status="running")
        .order_by(models.PipelineRun.id.desc())
        .first()
    )
    if not run:
        raise HTTPException(status_code=404, detail="No running pipeline found")

    job_id = run.flink_job_id
    if job_id and job_id != "submitted":
        try:
            await request.app.state.job_manager.cancel_job(job_id)
        except Exception:
            pass  # update local status even if Flink cancel fails

    # Also clear the launcher's state.json for this pipeline's generated job so a
    # re-run isn't blocked by the double-launch guard (best-effort).
    try:
        from flink_job_generator import resolve_jobs_repo, _safe
        from services import job_launcher
        safe = _safe(name)
        job_file = resolve_jobs_repo() / "flink-jobs" / safe / f"{safe}_pipeline_job.py"
        if job_file.exists():
            await job_launcher.stop(job_file)
    except Exception:
        pass

    run.status = "cancelled"
    run.finished_at = datetime.utcnow()
    db.commit()
    db.add(models.PipelineRunLog(run_id=run.id, level="INFO", message="Pipeline cancelled by user"))
    db.commit()
    return {"status": "cancelled", "run_id": run.id}


@router.get("/pipeline/{name}/health")
async def pipeline_health(name: str, request: Request, db: Session = Depends(get_db)):
    """Heartbeat check: is the Flink job behind this pipeline's latest run actually RUNNING?"""
    pipeline = db.query(models.Pipeline).filter_by(name=name).first()
    if not pipeline:
        return {"healthy": None, "state": None, "job_id": None}

    run = (
        db.query(models.PipelineRun)
        .filter_by(pipeline_id=pipeline.id)
        .order_by(models.PipelineRun.id.desc())
        .first()
    )
    if not run or not run.flink_job_id or run.flink_job_id == "submitted":
        return {"healthy": None, "state": run.status if run else None, "job_id": None}

    try:
        job = await request.app.state.job_manager.get_job(run.flink_job_id)
        state = job.get("state", "UNKNOWN")
    except Exception:
        return {"healthy": False, "state": "UNREACHABLE", "job_id": run.flink_job_id}

    return {"healthy": state == "RUNNING", "state": state, "job_id": run.flink_job_id}


@router.get("/flink/jobs")
async def flink_jobs(request: Request):
    return {"jobs": await request.app.state.job_manager.get_jobs()}


@router.get("/flink/jobs/{job_id}")
async def flink_job_detail(job_id: str, request: Request):
    return await request.app.state.job_manager.get_job(job_id)


@router.delete("/flink/jobs/{job_id}")
async def flink_cancel_job(job_id: str, request: Request):
    await request.app.state.job_manager.cancel_job(job_id)
    return {"status": "cancelled", "job_id": job_id}
