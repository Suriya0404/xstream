"""
Pipeline CRUD, run, and status endpoints.
"""
from __future__ import annotations

from datetime import datetime
from typing import Optional

from fastapi import APIRouter, BackgroundTasks, Depends, HTTPException, Request
from pydantic import BaseModel
from sqlalchemy.orm import Session

import config as cfg
import models
from database import get_db
from services.pipeline_service import execute_pipeline

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
    try:
        from flink_job_generator import generate_pipeline_artifacts
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
        generate_pipeline_artifacts(payload.name, nodes_data, edges_data)
    except Exception:
        pass  # Don't fail the save if file generation errors

    return {"status": "saved", "pipeline_id": pipeline.id}


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
        execute_pipeline, run.id, node_map, edge_list, global_config, sql_gateway
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
def get_run_logs(name: str, run_id: int, db: Session = Depends(get_db)):
    """Return structured log rows for a specific pipeline run."""
    logs = (
        db.query(models.PipelineRunLog)
        .filter_by(run_id=run_id)
        .order_by(models.PipelineRunLog.id.asc())
        .all()
    )
    return [
        {"id": l.id, "level": l.level, "message": l.message, "timestamp": l.timestamp}
        for l in logs
    ]


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

    run.status = "cancelled"
    run.finished_at = datetime.utcnow()
    db.commit()
    db.add(models.PipelineRunLog(run_id=run.id, level="INFO", message="Pipeline cancelled by user"))
    db.commit()
    return {"status": "cancelled", "run_id": run.id}


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
