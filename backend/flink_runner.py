"""
Submit and monitor Flink SQL jobs via the SQL Gateway REST API.
"""
import asyncio
import httpx
import logging
from typing import Optional

log = logging.getLogger(__name__)


class FlinkSQLGateway:
    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")
        self._session_id: Optional[str] = None

    async def _client(self) -> httpx.AsyncClient:
        return httpx.AsyncClient(base_url=self.base, timeout=30)

    # ── Session management ────────────────────────────────
    async def open_session(self) -> str:
        async with httpx.AsyncClient(base_url=self.base, timeout=30) as client:
            r = await client.post("/v1/sessions", json={"sessionName": "xstream"})
            r.raise_for_status()
            self._session_id = r.json()["sessionHandle"]
            log.info("SQL Gateway session: %s", self._session_id)
            return self._session_id

    async def get_or_create_session(self) -> str:
        if self._session_id:
            return self._session_id
        return await self.open_session()

    # ── Statement execution ───────────────────────────────
    async def execute(self, sql: str, timeout_s: float = 120.0) -> dict:
        # Retry once on session-level errors (404/500) by resetting the cached session.
        # This handles SQL Gateway restarts where _session_id has become stale.
        submit_r: httpx.Response | None = None
        for attempt in range(2):
            session = await self.get_or_create_session()
            async with httpx.AsyncClient(base_url=self.base, timeout=60) as client:
                submit_r = await client.post(
                    f"/v1/sessions/{session}/statements",
                    json={"statement": sql},
                )
                if submit_r.status_code in (404, 500) and attempt == 0:
                    log.warning(
                        "SQL Gateway session error (status=%s); resetting session and retrying. Body: %s",
                        submit_r.status_code,
                        submit_r.text[:500],
                    )
                    self._session_id = None
                    continue
                break

        if submit_r is None or submit_r.status_code >= 400:
            body = submit_r.text if submit_r is not None else "no response"
            raise RuntimeError(
                f"SQL Gateway {submit_r.status_code if submit_r else '?'} submitting statement"
                f":\n{sql[:300]}\n\n{body}"
            )

        op_handle = submit_r.json()["operationHandle"]

        async with httpx.AsyncClient(base_url=self.base, timeout=60) as client:
            # Poll with exponential backoff: start at 0.5s, cap at 5s
            elapsed = 0.0
            delay = 0.5
            status = "RUNNING"
            while elapsed < timeout_s:
                status_r = await client.get(
                    f"/v1/sessions/{session}/operations/{op_handle}/status"
                )
                status = status_r.json().get("status", "RUNNING")
                if status in ("FINISHED", "ERROR", "CLOSED", "CANCELED"):
                    break
                await asyncio.sleep(delay)
                elapsed += delay
                delay = min(delay * 2, 5.0)
            else:
                log.warning("Statement polling timed out after %.0fs", timeout_s)

            result_r = await client.get(
                f"/v1/sessions/{session}/operations/{op_handle}/result/0"
            )
            data = result_r.json()

            if status == "ERROR" or result_r.status_code >= 400 or "errors" in data:
                detail = data.get("errors", [str(data)])[0] if isinstance(data, dict) else str(data)
                raise RuntimeError(f"SQL Gateway error executing statement:\n{sql[:300]}\n\n{detail}")

            return data

    async def run_pipeline_sql(self, full_sql: str) -> str:
        """Split multi-statement SQL and execute each; returns last job id if any."""
        statements = [s.strip() for s in full_sql.split(";") if s.strip()]
        job_id = ""
        for stmt in statements:
            try:
                result = await self.execute(stmt + ";")
                # INSERT statements return a job id
                for row in result.get("results", {}).get("data", []):
                    for field in row.get("fields", []):
                        v = str(field)
                        if len(v) == 32 and v.isalnum():
                            job_id = v
            except Exception as exc:
                log.error("SQL error on statement: %s\n%s", stmt[:120], exc)
                raise
        return job_id


class FlinkJobManager:
    """Fallback: submit a JAR job via the Job Manager REST API."""

    def __init__(self, base_url: str):
        self.base = base_url.rstrip("/")

    async def get_jobs(self) -> list[dict]:
        async with httpx.AsyncClient(base_url=self.base, timeout=10) as client:
            r = await client.get("/jobs/overview")
            r.raise_for_status()
            return r.json().get("jobs", [])

    async def get_job(self, job_id: str) -> dict:
        async with httpx.AsyncClient(base_url=self.base, timeout=10) as client:
            r = await client.get(f"/jobs/{job_id}")
            r.raise_for_status()
            return r.json()

    async def cancel_job(self, job_id: str) -> None:
        async with httpx.AsyncClient(base_url=self.base, timeout=10) as client:
            await client.patch(f"/jobs/{job_id}", params={"mode": "cancel"})

    async def health(self) -> bool:
        try:
            async with httpx.AsyncClient(base_url=self.base, timeout=5) as client:
                r = await client.get("/overview")
                return r.status_code == 200
        except Exception:
            return False
