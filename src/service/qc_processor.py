"""
Transport-agnostic QC processing.

`QCProcessor` builds the QC agent graph once and evaluates a single ticket at a
time, returning a `QCTicketResultEvent`. It contains no I/O of its own, so it can
be reused by the Kafka service and the HTTP API alike:

    processor = QCProcessor("agent_config/qc_agent.yml")
    await processor.initialize()
    result = await processor.evaluate(chat_id="chat-1", chat_conversation=[...])
"""

import logging
from typing import Any, Dict, List, Optional

from src.agent.factory import AgentFactory
from src.kafka.models import QCTicketResultEvent

logger = logging.getLogger(__name__)


class QCProcessor:
    """Build the QC agent graph and evaluate tickets against it."""

    def __init__(self, agent_config_path: str = "agent_config/qc_agent.yml"):
        self.agent_config_path = agent_config_path
        self._factory: Optional[AgentFactory] = None
        self._graph = None

    async def initialize(self) -> None:
        """Load the agent config and build the graph (idempotent)."""
        if self._graph is not None:
            return
        logger.info(f"Building QC agent graph from {self.agent_config_path}")
        self._factory = AgentFactory(self.agent_config_path)
        await self._factory.load_config()
        self._graph = await self._factory.build_graph()
        logger.info("QC agent graph ready")

    @property
    def is_ready(self) -> bool:
        return self._graph is not None

    def _to_agent_input(
        self, chat_id: str, chat_conversation: List[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Transform a ticket into the agent's input state."""
        return {
            "chat_id": chat_id,
            "chat_conversation": chat_conversation,
            "correlation_id": chat_id,
            "messages": [],  # Required by base AgentState
        }

    async def evaluate(
        self, chat_id: str, chat_conversation: List[Dict[str, Any]]
    ) -> QCTicketResultEvent:
        """
        Run the QC agent on a single ticket.

        Args:
            chat_id: Identifier of the chat being evaluated
            chat_conversation: Ordered list of {role, message, timestamp} messages

        Returns:
            QCTicketResultEvent with the evaluation scores and observations

        Raises:
            RuntimeError: if the processor has not been initialized
        """
        if self._graph is None:
            raise RuntimeError("QCProcessor.initialize() must be called before evaluate()")

        logger.info(f"Evaluating ticket chat_id={chat_id}")
        agent_input = self._to_agent_input(chat_id, chat_conversation)
        agent_result = await self._graph.ainvoke(agent_input)
        return QCTicketResultEvent.from_agent_result(
            chat_id=chat_id,
            agent_result=agent_result,
        )


__all__ = ["QCProcessor"]
