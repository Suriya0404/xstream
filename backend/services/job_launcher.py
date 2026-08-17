"""
Launch / stop the generated per-pipeline PyFlink job.

Thin async wrapper around x-stream-jobs/scripts/flink_job_control.py, which does the
actual `docker cp` of the job file into the Flink JobManager container and
`docker exec … flink run -py`. We shell out to it (rather than reimplement) so the
job-control logic + state tracking lives in one place (the jobs repo).
"""
from __future__ import annotations

import asyncio
import json
import os
import sys
from pathlib import Path

from flink_job_generator import resolve_jobs_repo

FLINK_DOCKER_CONTAINER = os.environ.get("FLINK_DOCKER_CONTAINER", "xstream-flink-jobmanager")
_TIMEOUT_S = 180.0


def _control_script() -> Path:
    return resolve_jobs_repo() / "scripts" / "flink_job_control.py"


async def _run(action: str, job_file: Path) -> dict:
    script = _control_script()
    if not script.exists():
        raise RuntimeError(f"flink_job_control.py not found at {script}")
    cmd = [
        sys.executable, str(script), action,
        "--job-file", str(job_file),
        "--docker-container", FLINK_DOCKER_CONTAINER,
        "--mode", "cluster",
    ]
    proc = await asyncio.create_subprocess_exec(
        *cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )
    try:
        out_b, err_b = await asyncio.wait_for(proc.communicate(), timeout=_TIMEOUT_S)
    except asyncio.TimeoutError:
        proc.kill()
        raise RuntimeError(f"flink_job_control {action} timed out after {_TIMEOUT_S:.0f}s")

    out, err = out_b.decode(errors="replace"), err_b.decode(errors="replace")
    if proc.returncode != 0:
        raise RuntimeError(err.strip() or out.strip() or f"{action} exited {proc.returncode}")
    try:
        return json.loads(out)
    except json.JSONDecodeError:
        return {"raw": out.strip()}


async def launch(job_file: Path) -> dict:
    """Submit the job; returns the state payload incl. `job_id`."""
    return await _run("launch", job_file)


async def stop(job_file: Path) -> dict:
    """Cancel the job and mark its state STOPPED."""
    return await _run("stop", job_file)
