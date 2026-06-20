"""
Unit tests for FlinkSQLGateway: session management and polling backoff.
Uses respx to mock HTTP calls without a real Flink instance.
"""
import pytest
import respx
import httpx
from unittest.mock import AsyncMock, patch

from flink_runner import FlinkSQLGateway, FlinkJobManager


# ── FlinkJobManager.health ────────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_job_manager_health_ok():
    respx.get("http://flink:8081/overview").mock(return_value=httpx.Response(200))
    jm = FlinkJobManager("http://flink:8081")
    assert await jm.health() is True


@pytest.mark.asyncio
@respx.mock
async def test_job_manager_health_down():
    respx.get("http://flink:8081/overview").mock(side_effect=httpx.ConnectError("refused"))
    jm = FlinkJobManager("http://flink:8081")
    assert await jm.health() is False


# ── FlinkSQLGateway session ───────────────────────────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_open_session_returns_handle():
    respx.post("http://flink:8083/v1/sessions").mock(
        return_value=httpx.Response(200, json={"sessionHandle": "abc123"})
    )
    gw = FlinkSQLGateway("http://flink:8083")
    sid = await gw.open_session()
    assert sid == "abc123"
    assert gw._session_id == "abc123"


@pytest.mark.asyncio
@respx.mock
async def test_get_or_create_session_reuses_existing():
    gw = FlinkSQLGateway("http://flink:8083")
    gw._session_id = "existing"
    sid = await gw.get_or_create_session()
    assert sid == "existing"
    # No HTTP call should have been made
    assert len(respx.calls) == 0


# ── FlinkSQLGateway execute — exponential backoff ────────────────────────────

@pytest.mark.asyncio
@respx.mock
async def test_execute_polls_until_finished(monkeypatch):
    gw = FlinkSQLGateway("http://flink:8083")
    gw._session_id = "sess1"

    # Statement submission
    respx.post("http://flink:8083/v1/sessions/sess1/statements").mock(
        return_value=httpx.Response(200, json={"operationHandle": "op1"})
    )
    # First status = RUNNING, second = FINISHED
    status_calls = [
        httpx.Response(200, json={"status": "RUNNING"}),
        httpx.Response(200, json={"status": "FINISHED"}),
    ]
    respx.get("http://flink:8083/v1/sessions/sess1/operations/op1/status").mock(
        side_effect=status_calls
    )
    respx.get("http://flink:8083/v1/sessions/sess1/operations/op1/result/0").mock(
        return_value=httpx.Response(200, json={"results": {"data": []}})
    )

    sleep_calls: list[float] = []
    async def fake_sleep(s: float):
        sleep_calls.append(s)

    monkeypatch.setattr("flink_runner.asyncio.sleep", fake_sleep)

    result = await gw.execute("SELECT 1;")
    assert result == {"results": {"data": []}}
    # Should have slept once (0.5s backoff on first RUNNING → doubled to 1.0s next)
    assert len(sleep_calls) == 1
    assert sleep_calls[0] == pytest.approx(0.5)
