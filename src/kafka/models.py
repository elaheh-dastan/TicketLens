"""
Kafka message models for the Agent Framework.

This module defines the AgentEnvelope pattern for Kafka messages,
providing strict validation and type safety for agent results.
"""

from datetime import datetime, timezone
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field, field_validator


class MessageType(str, Enum):
    """Enumeration of message types."""

    AGENT_INPUT = "agent_input"
    AGENT_RESULT = "agent_result"
    AGENT_STATUS = "agent_status"
    AGENT_ERROR = "agent_error"


class EnvelopeStatus(str, Enum):
    """Enumeration of envelope status values."""

    COMPLETED = "COMPLETED"
    FAILED = "FAILED"
    PENDING = "PENDING"
    RUNNING = "RUNNING"


class EnvelopeMeta(BaseModel):
    """Metadata section of AgentEnvelope."""

    message_type: MessageType = Field(
        ..., description="Type of message (agent_result, agent_status, agent_error)"
    )
    source_agent: str = Field(
        ..., description="Name of the agent that produced this message"
    )
    correlation_id: str = Field(
        ..., description="Correlation ID to track the request (typically task_id)"
    )
    timestamp: str = Field(
        default_factory=lambda: datetime.now(timezone.utc).isoformat(),
        description="ISO 8601 timestamp when the message was created",
    )

    @field_validator("timestamp")
    @classmethod
    def validate_timestamp(cls, v: str) -> str:
        """Ensure timestamp is in ISO 8601 format."""
        try:
            datetime.fromisoformat(v.replace("Z", "+00:00"))
            return v
        except ValueError:
            raise ValueError(f"Invalid timestamp format: {v}")


class AgentEnvelope(BaseModel):
    """
    AgentEnvelope pattern for Kafka messages.

    This is the standard message format for agent results published to Kafka.
    It provides strict validation and routing information for consumers.

    Example:
        envelope = AgentEnvelope(
            meta=EnvelopeMeta(
                message_type=MessageType.AGENT_RESULT,
                source_agent="quiz_agent",
                correlation_id="550e8400-e29b-41d4-a716-446655440000"
            ),
            payload={"quiz_data": {...}},
            status=EnvelopeStatus.COMPLETED
        )
    """

    meta: EnvelopeMeta = Field(
        ..., description="Message metadata including type, source, and correlation ID"
    )
    payload: Dict[str, Any] = Field(
        default_factory=dict,
        description="Agent-specific result data (empty if status is FAILED)",
    )
    status: EnvelopeStatus = Field(
        ..., description="Execution status (COMPLETED, FAILED, PENDING, RUNNING)"
    )
    error: Optional[str] = Field(
        default=None, description="Error message if status is FAILED, null otherwise"
    )

    @field_validator("payload")
    @classmethod
    def validate_payload(cls, v: Dict[str, Any], info) -> Dict[str, Any]:
        """Ensure payload is empty when status is FAILED."""
        status = info.data.get("status")
        if status == EnvelopeStatus.FAILED and v:
            raise ValueError("Payload must be empty when status is FAILED")
        return v

    @field_validator("error")
    @classmethod
    def validate_error(cls, v: Optional[str], info) -> Optional[str]:
        """Ensure error is provided when status is FAILED."""
        status = info.data.get("status")
        if status == EnvelopeStatus.FAILED and not v:
            raise ValueError("Error message is required when status is FAILED")
        if status != EnvelopeStatus.FAILED and v:
            raise ValueError("Error message must be null when status is not FAILED")
        return v

    def to_dict(self) -> Dict[str, Any]:
        """Convert envelope to dictionary for JSON serialization."""
        return self.model_dump()

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "AgentEnvelope":
        """Create envelope from dictionary (e.g., from Kafka message)."""
        return cls(**data)

    @classmethod
    def create_success(
        cls,
        source_agent: str,
        correlation_id: str,
        payload: Dict[str, Any],
        message_type: MessageType = MessageType.AGENT_RESULT,
    ) -> "AgentEnvelope":
        """
        Create a success envelope.

        Args:
            source_agent: Name of the agent
            correlation_id: Correlation ID (task_id)
            payload: Result data
            message_type: Type of message (default: AGENT_RESULT)

        Returns:
            AgentEnvelope with COMPLETED status
        """
        return cls(
            meta=EnvelopeMeta(
                message_type=message_type,
                source_agent=source_agent,
                correlation_id=correlation_id,
            ),
            payload=payload,
            status=EnvelopeStatus.COMPLETED,
            error=None,
        )

    @classmethod
    def create_failure(
        cls,
        source_agent: str,
        correlation_id: str,
        error: str,
        message_type: MessageType = MessageType.AGENT_ERROR,
    ) -> "AgentEnvelope":
        """
        Create a failure envelope.

        Args:
            source_agent: Name of the agent
            correlation_id: Correlation ID (task_id)
            error: Error message
            message_type: Type of message (default: AGENT_ERROR)

        Returns:
            AgentEnvelope with FAILED status
        """
        return cls(
            meta=EnvelopeMeta(
                message_type=message_type,
                source_agent=source_agent,
                correlation_id=correlation_id,
            ),
            payload={},
            status=EnvelopeStatus.FAILED,
            error=error,
        )


class QCTicketResultEvent(BaseModel):
    """
    QC Ticket Result Event for Kafka publishing.

    This is the output schema matching the QCTicketResultEvent Avro schema
    consumed by downstream services.
    """

    chat_id: str = Field(..., description="Chat ID being evaluated")
    main_problem: str = Field(default="", description="Main problem identified")
    score: str = Field(default="", description="Overall QC score")
    tone_score: str = Field(default="", description="Tone evaluation score")
    empathy_score: str = Field(default="", description="Empathy evaluation score")
    solution_quality: str = Field(default="", description="Solution quality score")
    clarity_score: str = Field(default="", description="Clarity evaluation score")
    key_observations: list[str] = Field(
        default_factory=list, description="Key observations from evaluation"
    )
    reasons: str = Field(default="", description="Reasons for the scores")
    time_stamp: float = Field(
        default_factory=lambda: datetime.now(timezone.utc).timestamp(),
        description="Epoch timestamp",
    )
    event_name: str = Field(
        default="ticket_qc_results", description="Event type name"
    )

    def to_avro_dict(self) -> Dict[str, Any]:
        """Convert to Avro-compatible dictionary."""
        return self.model_dump()

    @classmethod
    def from_agent_result(
        cls,
        chat_id: str,
        agent_result: Dict[str, Any],
        event_name: str = "ticket_qc_results",
    ) -> "QCTicketResultEvent":
        """
        Create from agent output.

        Args:
            chat_id: Chat ID being evaluated
            agent_result: Result dict from agent invocation
            event_name: Event type name
        """
        return cls(
            chat_id=chat_id,
            main_problem=str(agent_result.get("main_problem", "")),
            score=str(agent_result.get("score", "")),
            tone_score=str(agent_result.get("tone_score", "")),
            empathy_score=str(agent_result.get("empathy_score", "")),
            solution_quality=str(agent_result.get("solution_quality", "")),
            clarity_score=str(agent_result.get("clarity_score", "")),
            key_observations=agent_result.get("key_observations", []),
            reasons=str(agent_result.get("reasons", "")),
            event_name=event_name,
        )


__all__ = [
    "MessageType",
    "EnvelopeStatus",
    "EnvelopeMeta",
    "AgentEnvelope",
    "QCTicketResultEvent",
]
