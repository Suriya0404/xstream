"""
Shared logging configuration for all producer/consumer processes.

Writes to both console (stderr) and a rotating log file.
Files are rotated when they reach LOG_MAX_BYTES (default 10 MB),
keeping LOG_BACKUP_COUNT (default 10) compressed backups — capping
total disk usage at ~100 MB per process.

Environment overrides:
  LOG_DIR          directory for log files   (default: ./logs/)
  LOG_MAX_BYTES    max bytes per file         (default: 10485760 = 10 MB)
  LOG_BACKUP_COUNT number of backup files     (default: 10)
"""
import logging
import logging.handlers
import os
from pathlib import Path

LOG_DIR          = Path(os.getenv("LOG_DIR", str(Path(__file__).parent / "logs")))
LOG_MAX_BYTES    = int(os.getenv("LOG_MAX_BYTES",    str(10 * 1024 * 1024)))
LOG_BACKUP_COUNT = int(os.getenv("LOG_BACKUP_COUNT", "10"))

_FMT = logging.Formatter(
    "%(asctime)s  %(levelname)-7s  [%(name)s]  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def configure_logging(name: str, level: int = logging.INFO) -> logging.Logger:
    """
    Return a logger writing to stderr and logs/<name>.log.
    Safe to call multiple times — handlers are not duplicated.
    """
    LOG_DIR.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if logger.handlers:
        return logger  # already configured

    logger.setLevel(level)
    logger.propagate = False

    file_handler = logging.handlers.RotatingFileHandler(
        LOG_DIR / f"{name}.log",
        maxBytes=LOG_MAX_BYTES,
        backupCount=LOG_BACKUP_COUNT,
        encoding="utf-8",
    )
    file_handler.setFormatter(_FMT)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(_FMT)
    logger.addHandler(console_handler)

    return logger
