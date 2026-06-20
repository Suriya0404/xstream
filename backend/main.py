"""
x-stream Backend API — entry point.
"""
from __future__ import annotations

import time
from contextlib import asynccontextmanager

import structlog
from fastapi import FastAPI, Request, Response
from fastapi.middleware.cors import CORSMiddleware
from prometheus_fastapi_instrumentator import Instrumentator

import config as cfg
import models
from database import engine
from flink_runner import FlinkSQLGateway, FlinkJobManager
from api.routes import health, pipelines, chat

structlog.configure(
    processors=[
        structlog.contextvars.merge_contextvars,
        structlog.processors.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.processors.JSONRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(20),  # INFO
    logger_factory=structlog.PrintLoggerFactory(),
)
log = structlog.get_logger()


@asynccontextmanager
async def lifespan(app: FastAPI):
    flink_cfg = cfg.get("flink")
    app.state.sql_gateway = FlinkSQLGateway(
        flink_cfg.get("sql_gateway_url", "http://localhost:8083")
    )
    app.state.job_manager = FlinkJobManager(
        flink_cfg.get("jobmanager_url", "http://localhost:8081")
    )
    try:
        models.Base.metadata.create_all(bind=engine)
        log.info("db_tables_ready")
        _seed_sample_if_empty()
    except Exception as exc:
        log.warning("mysql_not_reachable_at_startup", error=str(exc))
    yield


app = FastAPI(title="x-stream API", version="1.0.0", lifespan=lifespan)

_cors_cfg = cfg.get("cors")
_allowed_origins: list[str] = _cors_cfg.get(
    "allowed_origins", ["http://localhost:5173", "http://localhost:8000"]
)
app.add_middleware(
    CORSMiddleware,
    allow_origins=_allowed_origins,
    allow_methods=["GET", "POST", "PUT", "DELETE", "OPTIONS"],
    allow_headers=["*"],
)

# ── Request logging middleware ─────────────────────────────
@app.middleware("http")
async def _log_requests(request: Request, call_next) -> Response:
    start = time.perf_counter()
    response = await call_next(request)
    log.info(
        "http_request",
        method=request.method,
        path=request.url.path,
        status=response.status_code,
        duration_ms=round((time.perf_counter() - start) * 1000, 1),
    )
    return response

# ── Prometheus metrics ─────────────────────────────────────
Instrumentator().instrument(app).expose(app, endpoint="/metrics")

app.include_router(health.router)
app.include_router(pipelines.router)
app.include_router(chat.router)


def _seed_sample_if_empty() -> None:
    from database import SessionLocal
    db = SessionLocal()
    try:
        if db.query(models.Pipeline).count() > 0:
            return
        pipeline = models.Pipeline(name="Sample Workflow")
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)

        sample_nodes = [
            models.Node(id="s1", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-orders", pos_x=0, pos_y=0,
                        properties={"topic": "orders", "format": "json",
                                    "_handles": [["order_id", "STRING"], ["amount", "DOUBLE"], ["customer_id", "STRING"]]}),
            models.Node(id="s2", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-customers", pos_x=600, pos_y=0,
                        properties={"topic": "customers", "format": "json",
                                    "_handles": [["customer_id", "STRING"], ["name", "STRING"], ["email", "STRING"]]}),
            models.Node(id="s3", pipeline_id=pipeline.id, node_type="scylladb",
                        label="scylla-merged", pos_x=300, pos_y=300,
                        properties={"keyspace": "xstream", "table": "merged_orders",
                                    "_handles": [["order_id", "STRING"], ["amount", "DOUBLE"], ["name", "STRING"]]}),
            models.Node(id="s4", pipeline_id=pipeline.id, node_type="clickhouse",
                        label="clickhouse-reports", pos_x=300, pos_y=600,
                        properties={"database": "xstream", "table": "reports",
                                    "_handles": [["order_id", "STRING"], ["amount", "DOUBLE"], ["name", "STRING"]]}),
        ]
        for n in sample_nodes:
            db.add(n)

        sample_edges = [
            models.Edge(id="se1", pipeline_id=pipeline.id, source_id="s1", target_id="s3",
                        source_handle="out-0", target_handle="in-0"),
            models.Edge(id="se2", pipeline_id=pipeline.id, source_id="s2", target_id="s3",
                        source_handle="out-0", target_handle="in-0"),
            models.Edge(id="se3", pipeline_id=pipeline.id, source_id="s3", target_id="s4",
                        source_handle="out-0", target_handle="in-0"),
        ]
        for e in sample_edges:
            db.add(e)

        db.commit()
        log.info("Seeded 'Sample Workflow'")
    except Exception as exc:
        log.warning("Could not seed sample workflow: %s", exc)
        db.rollback()
    finally:
        db.close()
