"""
FastAPI application for QC ticket evaluation over HTTP.

This exposes the QC agent as a synchronous request/response API, an alternative
to the Kafka ingestion path. A client POSTs a ticket (a chat conversation) and
receives the QC evaluation in the same response.

    POST /qc      -> evaluate a ticket, return QCTicketResultEvent
    GET  /health  -> liveness / readiness probe

The agent graph is built once at startup and reused for every request.
"""

import logging
from contextlib import asynccontextmanager
from typing import List, Optional

from fastapi import Depends, FastAPI, HTTPException, Request
from pydantic import BaseModel, Field

from src.config.settings import get_settings
from src.kafka.models import QCTicketResultEvent
from src.service import QCProcessor

logger = logging.getLogger(__name__)


class ChatMessageModel(BaseModel):
    """A single message in a support conversation."""

    role: str = Field(..., description="Who sent the message, e.g. 'customer' or 'agent'")
    message: str = Field(..., description="Message text")
    timestamp: str = Field(default="", description="ISO 8601 timestamp of the message")


class TicketRequest(BaseModel):
    """A QC ticket submitted for evaluation."""

    chat_id: str = Field(..., description="Identifier of the chat being evaluated")
    chat_conversation: List[ChatMessageModel] = Field(
        ..., min_length=1, description="Ordered conversation messages"
    )
    # Optional metadata (accepted for parity with the Kafka event; not required)
    source: Optional[str] = Field(default=None, description="Origin of the ticket")
    time_stamp: Optional[float] = Field(default=None, description="Epoch timestamp")
    event_name: Optional[str] = Field(default=None, description="Event type name")


def get_processor(request: Request) -> QCProcessor:
    """Return the shared QCProcessor built at startup."""
    processor: Optional[QCProcessor] = getattr(request.app.state, "processor", None)
    if processor is None or not processor.is_ready:
        raise HTTPException(status_code=503, detail="QC agent is not ready")
    return processor


def create_app(
    agent_config_path: Optional[str] = None,
    processor: Optional[QCProcessor] = None,
) -> FastAPI:
    """
    Build the FastAPI app.

    Args:
        agent_config_path: QC agent YAML config (defaults to settings.api.agent_config_path).
            Ignored when an explicit ``processor`` is supplied.
        processor: Pre-built processor to use instead of constructing one. Primarily
            for tests, where a stub avoids building a real agent graph.
    """
    if processor is None:
        config_path = agent_config_path or get_settings().api.agent_config_path
        processor = QCProcessor(config_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await app.state.processor.initialize()
        logger.info("QC HTTP API ready")
        yield

    app = FastAPI(
        title="TicketLens QC API",
        description="Submit support-chat tickets and receive QC evaluations over HTTP.",
        version=get_settings().version,
        lifespan=lifespan,
    )
    app.state.processor = processor

    @app.get("/health")
    async def health() -> dict:
        proc: Optional[QCProcessor] = getattr(app.state, "processor", None)
        return {"status": "ok", "agent_ready": bool(proc and proc.is_ready)}

    @app.get("/status")
    async def status() -> dict:
        proc: Optional[QCProcessor] = getattr(app.state, "processor", None)
        return {
            "status": "ok",
            "version": get_settings().version,
            "agent_ready": bool(proc and proc.is_ready),
        }

    @app.post("/api/v2/qc/evaluate", response_model=QCTicketResultEvent)
    async def evaluate_ticket(
        ticket: TicketRequest,
        proc: QCProcessor = Depends(get_processor),
    ) -> QCTicketResultEvent:
        conversation = [msg.model_dump() for msg in ticket.chat_conversation]
        try:
            return await proc.evaluate(
                chat_id=ticket.chat_id,
                chat_conversation=conversation,
            )
        except Exception as exc:  # noqa: BLE001 - surface a clean 500 to the client
            logger.exception(f"QC evaluation failed for chat_id={ticket.chat_id}")
            raise HTTPException(
                status_code=500, detail=f"QC evaluation failed: {exc}"
            ) from exc

    return app


__all__ = ["create_app", "TicketRequest", "ChatMessageModel"]
