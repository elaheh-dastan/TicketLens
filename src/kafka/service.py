"""
Agent Kafka Service

This module provides a clean separation between Kafka I/O (infrastructure) and
the agent (business logic). The service:
1. Consumes messages from Kafka input topic
2. Transforms them into agent input format
3. Invokes the agent
4. Publishes results to Kafka output topic

This follows the principle that Kafka I/O is infrastructure concern,
not agent logic. Can be used with any agent configuration.
"""

import asyncio
import logging
import signal
from datetime import datetime, timezone
from typing import Any, Callable, Dict, Optional

from src.agent.factory import AgentFactory
from src.kafka.consumer import KafkaConsumer
from src.kafka.producer import KafkaProducer
from src.config.settings import get_settings

logger = logging.getLogger(__name__)

# Type aliases for transform functions
InputTransformer = Callable[[Dict[str, Any]], Dict[str, Any]]
OutputTransformer = Callable[[Dict[str, Any]], Dict[str, Any]]


class AgentKafkaService:
    """
    Kafka service that wraps any agent.

    This service handles all Kafka I/O operations while delegating
    the business logic to the agent. This separation ensures:
    - Agent remains pure business logic
    - Kafka I/O is handled at infrastructure layer
    - Easy to test agent without Kafka
    - Easy to swap Kafka for other message brokers

    Usage:
        service = AgentKafkaService(
            agent_config_path="agent_config/my_agent.yml",
            input_topic="agent-input",
            output_topic="agent-results"
        )
        await service.start()
    """

    def __init__(
        self,
        agent_config_path: str,
        input_topic: Optional[str] = None,
        output_topic: Optional[str] = None,
        consumer_group: Optional[str] = None,
        source_agent: str = "Agent",
        max_concurrent_tasks: Optional[int] = None,
        message_timeout_ms: int = 5000,
        input_transformer: Optional[InputTransformer] = None,
        output_transformer: Optional[OutputTransformer] = None,
    ):
        """
        Initialize the Agent Kafka Service.

        Args:
            agent_config_path: Path to the agent YAML configuration
            input_topic: Kafka topic to consume from (uses settings default if None)
            output_topic: Kafka topic to publish to (uses settings default if None)
            consumer_group: Kafka consumer group (uses settings default if None)
            source_agent: Name of the source agent for envelope metadata
            max_concurrent_tasks: Maximum number of concurrent agent invocations (uses settings default if None)
            message_timeout_ms: Timeout for consuming messages in milliseconds
            input_transformer: Optional function to transform Kafka message to agent input.
                               Default: passes payload directly with correlation_id and messages.
            output_transformer: Optional function to transform agent output to Kafka payload.
                               Default: passes agent result directly with correlation_id and timestamp.
        """
        self.agent_config_path = agent_config_path
        self.input_topic = input_topic
        self.output_topic = output_topic
        self.consumer_group = consumer_group
        self.source_agent = source_agent
        self.max_concurrent_tasks = max_concurrent_tasks
        self.message_timeout_ms = message_timeout_ms
        self._input_transformer = input_transformer
        self._output_transformer = output_transformer

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

        if not self._input_transformer or not self._output_transformer :
            raise RuntimeError("Kafka required both input and output transformer")

        # Set values from settings if not provided
        self.input_topic = self.input_topic or settings.kafka.input_topic
        self.output_topic = self.output_topic or settings.kafka.results_topic
        self.consumer_group = self.consumer_group or settings.kafka.consumer_group
        self.max_concurrent_tasks = self.max_concurrent_tasks or settings.kafka.max_concurrent_tasks

        logger.info(f"Initializing Agent Kafka Service: config={self.agent_config_path}, "
        f"input topic={self.input_topic}, output topic={self.output_topic}, "
        f"consumer group={self.consumer_group}, max concurrent tasks={self.max_concurrent_tasks}"
        )

        # Initialize agent
        self._agent_factory = AgentFactory(self.agent_config_path)
        await self._agent_factory.load_config()
        self._graph = await self._agent_factory.build_graph()
        logger.info("agent graph initialized successfully")

        # Initialize Kafka components
        self._consumer = KafkaConsumer()
        self._producer = KafkaProducer()

        # Initialize semaphore for concurrency control
        self._semaphore = asyncio.Semaphore(self.max_concurrent_tasks)

        logger.info("Agent Kafka Service initialized")

    async def _process_message(self, message_data: Dict[str, Any]) -> None:
        """
        Process a single Kafka message through the agent.

        This method:
        1. Transforms the Kafka message to agent input
        2. Invokes the agent
        3. Transforms the output and publishes to Kafka

        Args:
            message_data: Message data from Kafka consumer
        """
        correlation_id = message_data.get("correlation_id", message_data.get("chat_id", "unknown"))

        logger.info(f"Processing message: correlation_id={correlation_id}")

        try:
            # Transform to agent input
            agent_input = self._input_transformer(message_data)

            # Reset Langfuse context so this message gets its own trace hierarchy
            try:
                from src.utils.langfuse_client import set_current_langfuse_span

                set_current_langfuse_span(None)
            except Exception:
                pass

            # Invoke agent
            async with self._semaphore:
                agent_result = await self._graph.ainvoke(agent_input)

            self._stats["messages_processed"] += 1
            logger.info(f"Agent processing complete for correlation_id={correlation_id}")

            # Transform to Kafka output
            payload = self._output_transformer(agent_result)

            # Publish success result
            await self._producer.publish_result(
                source_agent=self.source_agent,
                correlation_id=correlation_id,
                payload=payload,
                topic=self.output_topic,
            )

        except asyncio.CancelledError:
            logger.info(f"Task cancelled for correlation_id={correlation_id}")
            # Re-raise so the task is properly marked as 'Cancelled'
            raise

        except Exception as e:
            self._stats["messages_failed"] += 1
            logger.exception(f"Error processing message {correlation_id}: {e}")

            # Publish error result
            await self._producer.publish_error(
                source_agent=self.source_agent,
                correlation_id=correlation_id,
                error=str(e),
                topic=self.output_topic,
            )

    async def process_message(self, message: dict):
        try:
            self._stats["messages_consumed"] += 1
            task = asyncio.create_task(self._process_message(message))
            self._tasks.add(task)
            task.add_done_callback(self._tasks.discard)

        except asyncio.CancelledError:
            logger.info("Consumption loop cancelled")
        except Exception as e:
            logger.exception(f"Error in consumption loop: {e}")

    @staticmethod
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
        logger.info("Starting Agent Kafka Service")

        # Start consumption loop using instance-level consumer
        try:
            await self._consumer.consume_and_process(
                process_func=self.process_message,
                error_func=self.handle_error
            )
        except Exception as e:
            logger.error(f"Consumer loop crashed: {e}")
            await self.stop()

    async def stop(self, shutdown_timeout: int = 25) -> None:
        """
        Stop the Kafka service gracefully.

        This method:
        1. Stops accepting new messages
        2. Waits for in-flight tasks to complete (with timeout)
        3. Force-cancels stuck tasks if timeout is reached
        4. Closes Kafka connections

        Args:
            shutdown_timeout: Max seconds to wait for tasks before force-cancelling.
                              Default 25s leaves 5s buffer for K8s terminationGracePeriodSeconds (30s).
        """
        logger.info("Stopping Agent Kafka Service...")
        self._running = False

        # Wait for in-flight tasks with timeout
        if self._tasks:
            logger.info(f"Waiting for {len(self._tasks)} in-flight tasks (timeout={shutdown_timeout}s)...")
            try:
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=shutdown_timeout
                )
                logger.info("All tasks completed successfully")
            except asyncio.TimeoutError:
                logger.warning(
                    f"Shutdown timeout reached! {len(self._tasks)} tasks still running. "
                    "Force cancelling remaining tasks..."
                )
                # Force cancel stuck tasks
                for task in self._tasks:
                    if not task.done():
                        task.cancel()
                # Allow brief moment for cancellation cleanup
                await asyncio.gather(*self._tasks, return_exceptions=True)
                logger.info("Stuck tasks cancelled")

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

        logger.info("Agent Kafka Service stopped")
        logger.info(f"Statistics: {self._stats}")

    def get_stats(self) -> Dict[str, int]:
        """Get service statistics."""
        return self._stats.copy()

    @property
    def is_running(self) -> bool:
        """Check if service is running."""
        return self._running


async def run_kafka_service(
    agent_config_path: str,
    input_topic: Optional[str] = None,
    output_topic: Optional[str] = None,
    input_transformer: Optional[InputTransformer] = None,
    output_transformer: Optional[OutputTransformer] = None,
) -> None:
    """
    Run the Agent Kafka Service.

    This is a convenience function to run the service with signal handling.

    Args:
        agent_config_path: Path to the agent configuration
        input_topic: Kafka input topic (uses settings default if None)
        output_topic: Kafka output topic (uses settings default if None)
        input_transformer: Optional function to transform Kafka message to agent input
        output_transformer: Optional function to transform agent output to Kafka payload
    """
    settings = get_settings()

    if not settings.kafka.enabled:
        logger.error("Kafka is not enabled. Set KAFKA__ENABLED=true")
        return

    service = AgentKafkaService(
        agent_config_path=agent_config_path,
        input_topic=input_topic,
        output_topic=output_topic,
        input_transformer=input_transformer,
        output_transformer=output_transformer,
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


__all__ = ["AgentKafkaService", "run_kafka_service"]
