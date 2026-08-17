from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker, DeclarativeBase
import config as cfg


def _url() -> str:
    c = cfg.get("mysql")
    return (
        f"mysql+pymysql://{c['user']}:{c['password']}"
        f"@{c['host']}:{c['port']}/{c['database']}"
    )


engine = create_engine(_url(), pool_pre_ping=True, pool_recycle=3600)
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)


class Base(DeclarativeBase):
    pass


def ensure_schema_migrations() -> None:
    """Idempotently add columns introduced after the initial release.

    create_all() never ALTERs existing tables, so columns added to a model later
    won't appear on a pre-existing MySQL table without this. Each addition is
    guarded by an information_schema check so it's safe to run on every startup."""
    additions: list[tuple[str, str, str]] = [
        ("pipeline_run_logs", "node_id", "VARCHAR(64) NULL"),
        ("pipeline_run_logs", "node_label", "VARCHAR(255) NULL"),
    ]
    # (table, column, required_enum_value, full_column_ddl) — widen an ENUM in place
    # when a new value was added to a model after the table already existed.
    enum_widenings: list[tuple[str, str, str, str]] = [
        (
            "nodes", "node_type", "mongodb",
            "ENUM('kafka','scylladb','clickhouse','mongodb') NOT NULL",
        ),
    ]
    with engine.begin() as conn:
        for table, column, ddl in additions:
            exists = conn.execute(
                text(
                    "SELECT 1 FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": column},
            ).first()
            if not exists:
                conn.execute(text(f"ALTER TABLE `{table}` ADD COLUMN `{column}` {ddl}"))

        for table, column, required_value, full_ddl in enum_widenings:
            col_type = conn.execute(
                text(
                    "SELECT COLUMN_TYPE FROM information_schema.columns "
                    "WHERE table_schema = DATABASE() "
                    "AND table_name = :t AND column_name = :c"
                ),
                {"t": table, "c": column},
            ).scalar()
            if col_type is not None and f"'{required_value}'" not in col_type:
                conn.execute(text(f"ALTER TABLE `{table}` MODIFY COLUMN `{column}` {full_ddl}"))


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
