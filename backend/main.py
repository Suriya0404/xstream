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
from database import engine, ensure_schema_migrations
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
    # create_all, migrations, and seeding are isolated so a failure in one never
    # skips the others. In particular create_all can raise a benign 1050
    # ("table already exists") when multiple workers race on first boot — that must
    # not prevent ensure_schema_migrations() from adding later-added columns.
    try:
        models.Base.metadata.create_all(bind=engine, checkfirst=True)
        log.info("db_tables_ready")
    except Exception as exc:
        log.warning("create_all_failed", error=str(exc))

    try:
        ensure_schema_migrations()
    except Exception as exc:
        log.warning("schema_migration_failed", error=str(exc))

    try:
        _seed_sample_if_empty()
        _seed_demo_pipeline_if_missing()
        _seed_mongo_demo_if_missing()
        _seed_mongo_ch_demo_if_missing()
    except Exception as exc:
        log.warning("seed_failed", error=str(exc))
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


def _seed_demo_pipeline_if_missing() -> None:
    """One-off migration of the old hardcoded finnhub-enriched -> finnhub_merged consumer
    flow into a real pipeline definition (finnhub is just a test workflow here — any other
    workflow gets created through the UI). Running it once hands scylla_sink_config.py the
    routing config the now-generic scylla-consumer container needs — same schema the
    consumer used to have baked in."""
    from database import SessionLocal
    db = SessionLocal()
    try:
        if db.query(models.Pipeline).filter_by(name="Finnhub Live").first():
            return
        pipeline = models.Pipeline(name="Finnhub Live")
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)

        finnhub_fields = [
            ["symbol", "STRING"], ["price", "DOUBLE"], ["volume", "DOUBLE"],
            ["quote_current", "DOUBLE"], ["quote_high", "DOUBLE"], ["quote_low", "DOUBLE"],
            ["pct_change", "DOUBLE"], ["timestamp", "BIGINT"],
        ]
        nodes = [
            models.Node(id="fh1", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-finnhub-enriched", pos_x=0, pos_y=0,
                        properties={"topic": "finnhub-enriched", "format": "json",
                                    "_handles": finnhub_fields}),
            models.Node(id="fh2", pipeline_id=pipeline.id, node_type="scylladb",
                        label="scylla-finnhub-merged", pos_x=600, pos_y=0,
                        properties={"keyspace": "xstream", "table": "finnhub_merged",
                                    "primary_key": ["symbol"], "_handles": finnhub_fields}),
        ]
        for n in nodes:
            db.add(n)

        db.add(models.Edge(id="fhe1", pipeline_id=pipeline.id, source_id="fh1", target_id="fh2",
                            source_handle="out-0", target_handle="in-0"))

        db.commit()
        log.info("Seeded 'Finnhub Live' pipeline")
    except Exception as exc:
        log.warning("Could not seed Finnhub Live pipeline: %s", exc)
        db.rollback()
    finally:
        db.close()


def _seed_mongo_demo_if_missing() -> None:
    """Seed a 'Finnhub Mongo' pipeline: two Kafka sources merged by symbol into a
    MongoDB sink. This is the pipeline compiled to a runnable PyFlink StatementSet
    job and launched via `flink run -py` (kafka-trades fills price/volume,
    kafka-enriched fills the quote_* columns — merged per symbol)."""
    from database import SessionLocal
    db = SessionLocal()
    try:
        if db.query(models.Pipeline).filter_by(name="Finnhub Mongo").first():
            return
        pipeline = models.Pipeline(name="Finnhub Mongo")
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)

        merged_fields = [
            ["symbol", "STRING"], ["price", "DOUBLE"], ["volume", "DOUBLE"],
            ["quote_current", "DOUBLE"], ["quote_high", "DOUBLE"], ["quote_low", "DOUBLE"],
            ["pct_change", "DOUBLE"], ["timestamp", "BIGINT"],
        ]
        trades_fields = [
            ["symbol", "STRING"], ["price", "DOUBLE"], ["volume", "DOUBLE"], ["timestamp", "BIGINT"],
        ]
        nodes = [
            models.Node(id="fm_enriched", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-enriched", pos_x=0, pos_y=0,
                        properties={"topic": "finnhub-enriched", "format": "json",
                                    "_handles": merged_fields}),
            models.Node(id="fm_trades", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-trades", pos_x=0, pos_y=300,
                        properties={"topic": "finnhub-trades", "format": "json",
                                    "_handles": trades_fields}),
            models.Node(id="fm_mongo", pipeline_id=pipeline.id, node_type="mongodb",
                        label="mongo-merge", pos_x=600, pos_y=150,
                        properties={"database": "xstream", "collection": "finnhub_merged",
                                    "primary_key": ["symbol"], "_handles": merged_fields,
                                    "field_mappings": {
                                        "0": {"source_node_id": "fm_enriched", "source_field": "symbol"},
                                        "1": {"source_node_id": "fm_trades", "source_field": "price"},
                                        "2": {"source_node_id": "fm_trades", "source_field": "volume"},
                                        "3": {"source_node_id": "fm_enriched", "source_field": "quote_current"},
                                        "4": {"source_node_id": "fm_enriched", "source_field": "quote_high"},
                                        "5": {"source_node_id": "fm_enriched", "source_field": "quote_low"},
                                        "6": {"source_node_id": "fm_enriched", "source_field": "pct_change"},
                                        "7": {"source_node_id": "fm_enriched", "source_field": "timestamp"},
                                    }}),
        ]
        for n in nodes:
            db.add(n)

        db.add(models.Edge(id="fme1", pipeline_id=pipeline.id, source_id="fm_enriched",
                           target_id="fm_mongo", source_handle="out-0", target_handle="in-0"))
        db.add(models.Edge(id="fme2", pipeline_id=pipeline.id, source_id="fm_trades",
                           target_id="fm_mongo", source_handle="out-0", target_handle="in-1"))

        db.commit()
        log.info("Seeded 'Finnhub Mongo' pipeline")
    except Exception as exc:
        log.warning("Could not seed Finnhub Mongo pipeline: %s", exc)
        db.rollback()
    finally:
        db.close()


def _seed_mongo_ch_demo_if_missing() -> None:
    """Seed a 'Finnhub Mongo CH' pipeline: two Kafka topics merged by symbol into a
    MongoDB sink AND into a ClickHouse table. The Kafka sources + MongoDB sink compile
    into one runnable Flink StatementSet job (launched via `flink run -py`); the
    ClickHouse node is fed by its native Kafka-engine + materialized-view path
    (apply_clickhouse_sink), which traces back through the mongo node to the same two
    Kafka topics. So both stores receive the merged per-symbol data."""
    from database import SessionLocal
    db = SessionLocal()
    try:
        if db.query(models.Pipeline).filter_by(name="Finnhub Mongo CH").first():
            return
        pipeline = models.Pipeline(name="Finnhub Mongo CH")
        db.add(pipeline)
        db.commit()
        db.refresh(pipeline)

        merged_fields = [
            ["symbol", "STRING"], ["price", "DOUBLE"], ["volume", "DOUBLE"],
            ["quote_current", "DOUBLE"], ["quote_high", "DOUBLE"], ["quote_low", "DOUBLE"],
            ["pct_change", "DOUBLE"], ["timestamp", "BIGINT"],
        ]
        trades_fields = [
            ["symbol", "STRING"], ["price", "DOUBLE"], ["volume", "DOUBLE"], ["timestamp", "BIGINT"],
        ]
        mongo_mappings = {
            "0": {"source_node_id": "fmc_enriched", "source_field": "symbol"},
            "1": {"source_node_id": "fmc_trades", "source_field": "price"},
            "2": {"source_node_id": "fmc_trades", "source_field": "volume"},
            "3": {"source_node_id": "fmc_enriched", "source_field": "quote_current"},
            "4": {"source_node_id": "fmc_enriched", "source_field": "quote_high"},
            "5": {"source_node_id": "fmc_enriched", "source_field": "quote_low"},
            "6": {"source_node_id": "fmc_enriched", "source_field": "pct_change"},
            "7": {"source_node_id": "fmc_enriched", "source_field": "timestamp"},
        }
        nodes = [
            models.Node(id="fmc_enriched", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-enriched", pos_x=0, pos_y=0,
                        properties={"topic": "finnhub-enriched", "format": "json",
                                    "_handles": merged_fields}),
            models.Node(id="fmc_trades", pipeline_id=pipeline.id, node_type="kafka",
                        label="kafka-trades", pos_x=0, pos_y=300,
                        properties={"topic": "finnhub-trades", "format": "json",
                                    "_handles": trades_fields}),
            models.Node(id="fmc_mongo", pipeline_id=pipeline.id, node_type="mongodb",
                        label="mongo-merge", pos_x=600, pos_y=150,
                        properties={"database": "xstream", "collection": "finnhub_merged_ch",
                                    "primary_key": ["symbol"], "_handles": merged_fields,
                                    "field_mappings": mongo_mappings}),
            models.Node(id="fmc_ch", pipeline_id=pipeline.id, node_type="clickhouse",
                        label="ch-analytics", pos_x=1200, pos_y=150,
                        properties={"database": "xstream", "table": "finnhub_analytics_mongo",
                                    "primary_key": ["symbol"], "_handles": merged_fields}),
        ]
        for n in nodes:
            db.add(n)

        db.add(models.Edge(id="fmce1", pipeline_id=pipeline.id, source_id="fmc_enriched",
                           target_id="fmc_mongo", source_handle="out-0", target_handle="in-0"))
        db.add(models.Edge(id="fmce2", pipeline_id=pipeline.id, source_id="fmc_trades",
                           target_id="fmc_mongo", source_handle="out-0", target_handle="in-1"))
        db.add(models.Edge(id="fmce3", pipeline_id=pipeline.id, source_id="fmc_mongo",
                           target_id="fmc_ch", source_handle="out-0", target_handle="in-0"))

        db.commit()
        log.info("Seeded 'Finnhub Mongo CH' pipeline")
    except Exception as exc:
        # Another uvicorn worker likely seeded concurrently (duplicate PK). Roll back
        # and drop the empty pipeline row this worker created, so no orphan remains.
        log.warning("Could not seed Finnhub Mongo CH pipeline: %s", exc)
        db.rollback()
        try:
            for orphan in db.query(models.Pipeline).filter_by(name="Finnhub Mongo CH").all():
                if db.query(models.Node).filter_by(pipeline_id=orphan.id).count() == 0:
                    db.delete(orphan)
            db.commit()
        except Exception:
            db.rollback()
    finally:
        db.close()
