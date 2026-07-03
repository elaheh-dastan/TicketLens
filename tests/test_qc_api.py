"""
Unit tests for the QC HTTP API.

The agent graph is replaced with a stub processor so these tests exercise the
HTTP layer (routing, validation, serialization, error handling) without needing
an LLM or a real agent build.
"""

import pytest
from fastapi.testclient import TestClient

from src.api.app import create_app
from src.kafka.models import QCTicketResultEvent


class FakeProcessor:
    """Stand-in for QCProcessor with no real agent graph."""

    def __init__(self, result=None, ready=True, raises=None):
        self._result = result
        self._ready = ready
        self._raises = raises
        self.calls = []

    @property
    def is_ready(self) -> bool:
        return self._ready

    async def initialize(self) -> None:
        # No-op: readiness is controlled by the constructor.
        return None

    async def evaluate(self, chat_id, chat_conversation):
        self.calls.append((chat_id, chat_conversation))
        if self._raises is not None:
            raise self._raises
        if self._result is not None:
            return self._result
        return QCTicketResultEvent(
            chat_id=chat_id,
            main_problem="Customer could not log in",
            score="8",
            tone_score="9",
            empathy_score="8",
            solution_quality="7",
            clarity_score="8",
            key_observations=["clear tone", "resolved issue"],
            reasons="Agent was polite and solved the problem.",
        )


SAMPLE_TICKET = {
    "chat_id": "chat-123",
    "chat_conversation": [
        {"role": "customer", "message": "I can't log in", "timestamp": "2026-01-01T00:00:00Z"},
        {"role": "agent", "message": "Let me help you reset it", "timestamp": "2026-01-01T00:00:05Z"},
    ],
}


def build_client(processor: FakeProcessor) -> TestClient:
    app = create_app(processor=processor)
    return TestClient(app)


def test_health_reports_ready():
    with build_client(FakeProcessor(ready=True)) as client:
        resp = client.get("/health")
    assert resp.status_code == 200
    assert resp.json() == {"status": "ok", "agent_ready": True}


def test_post_qc_returns_evaluation():
    processor = FakeProcessor()
    with build_client(processor) as client:
        resp = client.post("/api/v2/qc/evaluate", json=SAMPLE_TICKET)

    assert resp.status_code == 200
    body = resp.json()
    assert body["chat_id"] == "chat-123"
    assert body["score"] == "8"
    assert body["key_observations"] == ["clear tone", "resolved issue"]

    # The processor received the transformed conversation.
    assert len(processor.calls) == 1
    called_chat_id, called_conversation = processor.calls[0]
    assert called_chat_id == "chat-123"
    assert called_conversation[0]["role"] == "customer"
    assert called_conversation[0]["message"] == "I can't log in"


def test_post_qc_rejects_empty_conversation():
    with build_client(FakeProcessor()) as client:
        resp = client.post("/api/v2/qc/evaluate", json={"chat_id": "c1", "chat_conversation": []})
    assert resp.status_code == 422


def test_post_qc_requires_chat_id():
    with build_client(FakeProcessor()) as client:
        resp = client.post("/api/v2/qc/evaluate", json={"chat_conversation": SAMPLE_TICKET["chat_conversation"]})
    assert resp.status_code == 422


def test_post_qc_surfaces_processing_error_as_500():
    processor = FakeProcessor(raises=RuntimeError("model exploded"))
    with build_client(processor) as client:
        resp = client.post("/api/v2/qc/evaluate", json=SAMPLE_TICKET)
    assert resp.status_code == 500
    assert "QC evaluation failed" in resp.json()["detail"]


def test_post_qc_returns_503_when_agent_not_ready():
    with build_client(FakeProcessor(ready=False)) as client:
        resp = client.post("/api/v2/qc/evaluate", json=SAMPLE_TICKET)
    assert resp.status_code == 503
