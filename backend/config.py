import os
import yaml
from functools import lru_cache
from pathlib import Path

# Env-var → (section, key) mappings for all sensitive fields.
# Values present in the environment take precedence over config.yaml.
_ENV_OVERRIDES: list[tuple[str, str, str]] = [
    ("MYSQL_PASSWORD",      "mysql",      "password"),
    ("ANTHROPIC_API_KEY",   "anthropic",  "api_key"),
    ("FINNHUB_API_KEY",     "finnhub",    "api_key"),
    ("SCYLLADB_USERNAME",   "scylladb",   "username"),
    ("SCYLLADB_PASSWORD",   "scylladb",   "password"),
    ("CLICKHOUSE_USER",     "clickhouse", "user"),
    ("CLICKHOUSE_PASSWORD", "clickhouse", "password"),
    ("JOBS_REPO_PATH",      "jobs_repo",  "path"),
]


@lru_cache(maxsize=1)
def load_config() -> dict:
    path = os.environ.get("CONFIG_PATH", str(Path(__file__).parent.parent / "config.yaml"))
    with open(path) as f:
        cfg: dict = yaml.safe_load(f)

    for env_var, section, key in _ENV_OVERRIDES:
        val = os.environ.get(env_var)
        if val is not None:
            cfg.setdefault(section, {})[key] = val

    origins_env = os.environ.get("ALLOWED_ORIGINS")
    if origins_env:
        cfg.setdefault("cors", {})["allowed_origins"] = [
            o.strip() for o in origins_env.split(",") if o.strip()
        ]

    return cfg


def get(section: str) -> dict:
    return load_config().get(section, {})
