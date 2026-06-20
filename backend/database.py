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


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()
