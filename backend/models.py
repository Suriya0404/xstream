from datetime import datetime
from sqlalchemy import Column, String, Integer, Float, Text, JSON, DateTime, ForeignKey, Enum
from database import Base


class Pipeline(Base):
    __tablename__ = "pipelines"
    id          = Column(Integer, primary_key=True, autoincrement=True)
    name        = Column(String(255), nullable=False, default="untitled")
    description = Column(Text)
    created_at  = Column(DateTime, default=datetime.utcnow)
    updated_at  = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class Node(Base):
    __tablename__ = "nodes"
    id          = Column(String(64), primary_key=True)
    pipeline_id = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    node_type   = Column(Enum("kafka", "scylladb", "clickhouse", "mongodb"), nullable=False)
    label       = Column(String(255), nullable=False)
    pos_x       = Column(Float, default=0)
    pos_y       = Column(Float, default=0)
    flink_sql   = Column(Text)
    properties  = Column(JSON)
    created_at  = Column(DateTime, default=datetime.utcnow)


class Edge(Base):
    __tablename__ = "edges"
    id            = Column(String(64), primary_key=True)
    pipeline_id   = Column(Integer, ForeignKey("pipelines.id", ondelete="CASCADE"), nullable=False)
    source_id     = Column(String(64), nullable=False)
    target_id     = Column(String(64), nullable=False)
    source_handle = Column(String(64))
    target_handle = Column(String(64))


class PipelineRun(Base):
    __tablename__ = "pipeline_runs"
    id           = Column(Integer, primary_key=True, autoincrement=True)
    pipeline_id  = Column(Integer, ForeignKey("pipelines.id"), nullable=False)
    status       = Column(Enum("pending", "running", "success", "failed", "cancelled"), default="pending")
    flink_job_id = Column(String(255))
    logs         = Column(Text)
    started_at   = Column(DateTime)
    finished_at  = Column(DateTime)


class PipelineRunLog(Base):
    """Structured per-event log rows for a pipeline run (replaces blob TEXT logs).

    node_id / node_label are null for pipeline-level (summary) messages and set for
    per-node messages so the UI can filter logs into per-node tabs."""
    __tablename__ = "pipeline_run_logs"
    id         = Column(Integer, primary_key=True, autoincrement=True)
    run_id     = Column(Integer, ForeignKey("pipeline_runs.id", ondelete="CASCADE"), nullable=False)
    node_id    = Column(String(64))     # null = pipeline-level / summary message
    node_label = Column(String(255))
    level      = Column(Enum("DEBUG", "INFO", "WARNING", "ERROR"), default="INFO")
    message    = Column(Text, nullable=False)
    timestamp  = Column(DateTime, default=datetime.utcnow)
