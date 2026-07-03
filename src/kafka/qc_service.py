"""
QC Agent Kafka Service

This module provides a clean separation between Kafka I/O (infrastructure) and
the QC agent (business logic). The service:
1. Consumes messages from Kafka input topic
2. Transforms them into agent input format
3. Invokes the QC agent
4. Publishes results to Kafka output topic

This follows the principle that Kafka I/O is infrastructure concern,
not agent logic.
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Any, Dict, Optional

from src.agent.factory import AgentFactory
from src.kafka.consumer import KafkaConsumer
from src.kafka.models import QCTicketResultEvent
from src.kafka.producer import KafkaProducer
from src.config.settings import get_settings

logger = logging.getLogger(__name__)


class QCAgentKafkaService:
    """
    Kafka service that wraps the QC agent.

    This service handles all Kafka I/O operations while delegating
    the actual QC evaluation to the agent. This separation ensures:
    - Agent remains pure business logic
    - Kafka I/O is handled at infrastructure layer
    - Easy to test agent without Kafka
    - Easy to swap Kafka for other message brokers

    Usage:
        service = QCAgentKafkaService(
            agent_config_path="agent_config/qc_agent_dspy.yml",
            input_topic="qc-input",
            output_topic="qc-results"
        )
        await service.start()
    """

    def __init__(
        self,
        agent_config_path: str = "agent_config/qc_agent_dspy.yml",
        input_topic: Optional[str] = None,
        output_topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        source_agent: str = "QC_Agent",
        max_concurrent_tasks: int = 10,
        message_timeout_ms: int = 5000,
    ):
        """
        Initialize the QC Agent Kafka Service.

        Args:
            agent_config_path: Path to the QC agent YAML configuration
            input_topic: Kafka topic to consume from (uses settings default if None)
            output_topic: Kafka topic to publish to (uses settings default if None)
            consumer_group: Kafka consumer group (uses settings default if None)
            source_agent: Name of the source agent for envelope metadata
            max_concurrent_tasks: Maximum number of concurrent agent invocations
            message_timeout_ms: Timeout for consuming messages in milliseconds
        """
        self.agent_config_path = agent_config_path
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer_group = consumer_group
        self.source_agent = source_agent
        self.max_concurrent_tasks = max_concurrent_tasks
        self.message_timeout_ms = message_timeout_ms

        # Components (initialized lazily)
        self._agent_factory: Optional[AgentFactory] = None
        self._graph = None
        self._consumer: Optional[KafkaConsumer] = None
        self._producer: Optional[KafkaProducer] = None

        # State
        self._running = False
        self._semaphore: Optional[asyncio.Semaphore] = None
        self._tasks: set = set()

        # Statistics
        self._stats = {
            "messages_consumed": 0,
            "messages_processed": 0,
            "messages_failed": 0,
            "messages_published": 0,
        }

    async def initialize(self) -> None:
        """Initialize the agent and Kafka components."""
        settings = get_settings()

        # Validate Kafka is enabled
        if not settings.kafka.enabled:
            raise RuntimeError("Kafka is not enabled. Set KAFKA__ENABLED=true")

        # Set topics from settings if not provided
        self.input_topic = self.input_topic or settings.kafka.input_topic
        self.output_topic = self.output_topic or settings.kafka.results_topic
        self.consumer_group = self.consumer_group or settings.kafka.consumer_group

        logger.info("Initializing QC Agent Kafka Service")
        logger.info(f"  Agent config: {self.agent_config_path}")
        logger.info(f"  Input topic: {self.input_topic}")
        logger.info(f"  Output topic: {self.output_topic}")
        logger.info(f"  Consumer group: {self.consumer_group}")

        # Initialize agent
        self._agent_factory = AgentFactory(self.agent_config_path)
        await self._agent_factory.load_config()
        self._graph = await self._agent_factory.build_graph()
        logger.info("QC agent graph initialized successfully")

        # Initialize Kafka components
        self._consumer = KafkaConsumer()
        self._producer = KafkaProducer()

        # Initialize semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

        logger.info("QC Agent Kafka Service initialized")

    def _transform_kafka_to_agent_input(
        self, message_data: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Transform Kafka QCTicketInputEvent to agent input format.

        The incoming message is a flat QCTicketInputEvent with fields:
        chat_id, chat_conversation, source, time_stamp, event_name.

        Args:
            message_data: Deserialized QCTicketInputEvent dict from Kafka

        Returns:
            Dictionary with agent input data
        """
        return {
            "chat_id": message_data.get("chat_id", "unknown"),
            "chat_conversation": message_data.get("chat_conversation", []),
            "correlation_id": message_data.get("chat_id", "unknown"),
            "messages": [],  # Required by base AgentState
        }

    def _transform_agent_output_to_event(
        self,
        agent_result: Dict[str, Any],
        chat_id: str,
    ) -> QCTicketResultEvent:
        """
        Transform agent output to QCTicketResultEvent.

        Args:
            agent_result: Result from agent invocation
            chat_id: Chat ID being evaluated

        Returns:
            QCTicketResultEvent ready for Kafka publishing
        """
        return QCTicketResultEvent.from_agent_result(
            chat_id=chat_id,
            agent_result=agent_result,
        )

    async def _process_message(self, message_data: Dict[str, Any]) -> None:
        """
        Process a single Kafka message through the QC agent.

        This method:
        1. Transforms the Kafka message to agent input
        2. Invokes the agent
        3. Transforms the output and publishes to Kafka

        Args:
            message_data: Message data from Kafka consumer
        """
        chat_id = message_data.get("chat_id", "unknown")
        correlation_id = chat_id

        logger.info(
            f"Processing message: chat_id={chat_id}, correlation_id={correlation_id}"
        )

        try:
            # Transform to agent input
            agent_input = self._transform_kafka_to_agent_input(message_data)

            # Invoke agent
            async with self._semaphore:
                agent_result = await self._graph.ainvoke(agent_input)

            self._stats["messages_processed"] += 1
            logger.info(f"Agent processing complete for chat_id={chat_id}")

            # Transform to QCTicketResultEvent and publish
            event = self._transform_agent_output_to_event(agent_result, chat_id)
            await self._producer.publish_qc_result(
                event=event,
                topic=self.output_topic,
            )

        except Exception as e:
            self._stats["messages_failed"] += 1
            logger.exception(f"Error processing message {correlation_id}: {e}")

            # Publish error result
            await self._publish_result(
                correlation_id=correlation_id,
                payload={"chat_id": chat_id, "error": str(e)},
                success=False,
                error=str(e),
            )

    async def _publish_result(
        self,
        correlation_id: str,
        payload: Dict[str, Any],
        success: bool,
        error: Optional[str] = None,
    ) -> None:
        """
        Publish result to Kafka output topic.

        Args:
            correlation_id: Correlation ID for tracking
            payload: Result payload
            success: Whether processing was successful
            error: Error message if not successful
        """
        try:
            if success:
                await self._producer.publish_result(
                    source_agent=self.source_agent,
                    correlation_id=correlation_id,
                    payload=payload,
                    topic=self.output_topic,
                )
            else:
                await self._producer.publish_error(
                    source_agent=self.source_agent,
                    correlation_id=correlation_id,
                    error=error or "Unknown error",
                    topic=self.output_topic,
                )

            self._stats["messages_published"] += 1
            logger.info(f"Published result to topic '{self.output_topic}'")

        except Exception as e:
            logger.exception(f"Failed to publish result: {e}")

    async def process_message(self, message: dict):
        try:
            # Process message in background task
            self._stats["messages_consumed"] += 1
            task = asyncio.create_task(self._process_message(message))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        except asyncio.CancelledError:
            logger.info("Consumption loop cancelled")
        except Exception as e:
            logger.exception(f"Error in consumption loop: {e}")

    async def handle_error(correlation_id: str, error: Exception):
        """Handle errors during message processing."""
        logger.error(f"Error processing message {correlation_id}: {error}")

    async def start(self) -> None:
        """
        Start the Kafka service.

        This method initializes components and starts the consumption loop.
        """
        if self._running:
            logger.warning("Service is already running")
            return

        # Initialize if not already done
        if self._graph is None:
            await self.initialize()

        self._running = True
        logger.info("Starting QC Agent Kafka Service")

        # Create consumer
        consumer = KafkaConsumer()

        # Start consumption loop

        await consumer.consume_and_process(
            process_func=self.process_message, error_func=self.handle_error
        )

    async def stop(self) -> None:
        """
        Stop the Kafka service gracefully.

        This method:
        1. Stops accepting new messages
        2. Waits for in-flight tasks to complete
        3. Closes Kafka connections
        """
        logger.info("Stopping QC Agent Kafka Service...")
        self._running = False

        # Wait for in-flight tasks
        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} in-flight tasks...")
            await asyncio.gather(*self._tasks, return_exceptions=True)

        # Close consumer
        if self._consumer:
            try:
                await self._consumer.stop()
            except Exception as e:
                logger.error(f"Error closing consumer: {e}")

        # Close producer
        if self._producer:
            try:
                await self._producer.close()
            except Exception as e:
                logger.error(f"Error closing producer: {e}")

        logger.info("QC Agent Kafka Service stopped")
        logger.info(f"Statistics: {self._stats}")

    def get_stats(self) -> Dict[str, int]:
        """Get service statistics."""
        return self._stats.copy()

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running


async def run_qc_kafka_service(
    agent_config_path: str = "agent_config/qc_agent_dspy.yml",
    input_topic: Optional[str] = None,
    output_topic: Optional[str] = None,
) -> None:
    """
    Run the QC Agent Kafka Service.

    This is a convenience function to run the service with signal handling.

    Args:
        agent_config_path: Path to the QC agent configuration
        input_topic: Kafka input topic (uses settings default if None)
        output_topic: Kafka output topic (uses settings default if None)
    """
    settings = get_settings()

    if not settings.kafka.enabled:
        logger.error("Kafka is not enabled. Set KAFKA__ENABLED=true")
        return

    service = QCAgentKafkaService(
        agent_config_path=agent_config_path,
        input_topic=input_topic,
        output_topic=output_topic,
    )

    # Setup signal handlers
    loop = asyncio.get_event_loop()

    def signal_handler():
        logger.info("Received shutdown signal")
        asyncio.create_task(service.stop())

    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, signal_handler)

    try:
        await service.start()
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt")
    finally:
        await service.stop()


__all__ = ["QCAgentKafkaService", "run_qc_kafka_service"]
