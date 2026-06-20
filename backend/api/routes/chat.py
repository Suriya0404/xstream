"""
Claude AI chat streaming endpoint.
"""
from __future__ import annotations

import json
import os
from typing import Any

import anthropic as _anthropic
from fastapi import APIRouter
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

import config as cfg

router = APIRouter(prefix="/api")

_SYSTEM_PROMPT = """You are an expert data pipeline assistant embedded in x-stream, a streaming data platform.
You help users design and modify Kafka/Flink/ScyllaDB/ClickHouse pipelines visually.

Node types available:
- kafka     (pink  #e92a67) — Kafka topic source/sink. Properties: topic, format, group_id.
- scylladb  (blue  #2a8af6) — ScyllaDB table sink. Properties: keyspace, table.
- clickhouse(amber #f6a82a) — ClickHouse table sink. Properties: database, table.

Each node has typed fields (handles): [["field_name", "TYPE"], ...] where TYPE is STRING, DOUBLE, BIGINT, FLOAT, INT, BOOLEAN.

When you want to change the workflow, embed a single JSON action block at the END of your response, like this:
<actions>
[
  {"type": "add_node", "node": {"id": "unique-id", "node_type": "kafka", "label": "my-source", "pos_x": 200, "pos_y": 100, "properties": {"topic": "my-topic", "format": "json"}, "handles": [["field", "STRING"]]}},
  {"type": "delete_node", "id": "node-id"},
  {"type": "add_edge", "edge": {"id": "e-new", "source_id": "src-id", "target_id": "tgt-id"}},
  {"type": "delete_edge", "id": "edge-id"},
  {"type": "update_node", "id": "node-id", "label": "new-label", "properties": {"topic": "new-topic"}, "handles": [["symbol","STRING"]]}
]
</actions>

Rules:
- Use only one <actions> block per response.
- Omit <actions> if no workflow change is needed.
- Be concise. Explain what you changed and why in 1-3 sentences before the action block.
- IDs must be unique strings (use short slugs like "kafka-prices" or "scylla-merged").
- For pos_x/pos_y, space nodes 400px apart horizontally and 350px vertically."""


class ChatMessage(BaseModel):
    role: str
    content: str


class ChatRequest(BaseModel):
    messages: list[ChatMessage]
    workflow: dict[str, Any]


def _anthropic_key() -> str:
    key = os.environ.get("ANTHROPIC_API_KEY", "")
    if not key:
        key = (cfg.get("anthropic") or {}).get("api_key", "")
    return key


async def _stream_chat(messages: list[ChatMessage], workflow: dict):
    workflow_summary = json.dumps({
        "nodes": [
            {
                "id": n.get("id"), "label": n.get("label"), "type": n.get("node_type"),
                "handles": (n.get("properties") or {}).get("_handles", []),
                "properties": {
                    k: v for k, v in (n.get("properties") or {}).items() if k != "_handles"
                },
            }
            for n in workflow.get("nodes", [])
        ],
        "edges": workflow.get("edges", []),
    }, indent=2)

    system = f"{_SYSTEM_PROMPT}\n\nCurrent workflow:\n```json\n{workflow_summary}\n```"
    api_key = _anthropic_key()
    if not api_key:
        yield 'data: {"text": "⚠️ Anthropic API key not configured. Set ANTHROPIC_API_KEY env var."}\n\n'
        yield "data: [DONE]\n\n"
        return

    client = _anthropic.AsyncAnthropic(api_key=api_key)
    async with client.messages.stream(
        model="claude-sonnet-4-6",
        max_tokens=1024,
        system=system,
        messages=[{"role": m.role, "content": m.content} for m in messages],
    ) as stream:
        async for text in stream.text_stream:
            yield f"data: {json.dumps({'text': text})}\n\n"
    yield "data: [DONE]\n\n"


@router.post("/chat")
async def chat(req: ChatRequest):
    return StreamingResponse(
        _stream_chat(req.messages, req.workflow),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
