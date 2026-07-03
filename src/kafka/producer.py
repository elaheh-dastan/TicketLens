"""
Kafka producer utility for publishing agent results.

This module provides a simple interface to publish AgentEnvelope messages
to Kafka topics. It's designed to be called from the task manager when
tasks complete or fail.

Supports both JSON and Avro serialization formats, configurable via settings.
"""

import asyncio
import json
import logging
from typing import Any, Optional

from .models import AgentEnvelope, MessageType, QCTicketResultEvent

from aiokafka import AIOKafkaProducer
from src.config.settings import get_settings
from src.metrics.registry import get_metrics

logger = logging.getLogger(__name__)


class KafkaProducer:
    """
    Simple Kafka producer for publishing agent results.

    This class manages a single producer instance and provides
    methods to publish AgentEnvelope messages to Kafka topics.
    """

    def __init__(self):
        self._producer = None
        self._settings = None
        self._created_topics = set()  # Track topics we've created
        self._lock = asyncio.Lock()  # Protect producer initialization

    def _serialize_envelope(self, envelope: AgentEnvelope, topic: str) -> bytes:
        """Serialize an AgentEnvelope to bytes based on configured format."""
        if self._settings.kafka.serialization_format == "avro":
            from .avro_serializer import get_avro_serializer

            return get_avro_serializer().serialize(envelope, topic)
        else:
            return json.dumps(envelope.to_dict()).encode("utf-8")

    def _serialize_qc_result(self, event: QCTicketResultEvent, topic: str) -> bytes:
        """Serialize a QCTicketResultEvent to bytes based on configured format."""
        if self._settings.kafka.serialization_format == "avro":
            from .avro_serializer import get_avro_serializer

            return get_avro_serializer().serialize_qc_result(event, topic)
        else:
            return json.dumps(event.to_avro_dict()).encode("utf-8")

    async def _get_producer(self):
        """Get or create Kafka producer instance with thread-safe initialization."""
        async with self._lock:
            if self._producer is None:
                try:
                    self._settings = get_settings()

                    if not self._settings.kafka.enabled:
                        logger.warning("Kafka is disabled, skipping publish")
                        return None

                    # Create producer without value_serializer - serialization
                    # is handled per-message in publish methods to support
                    # multiple schemas (AgentEnvelope, QCTicketResultEvent).
                    # Note: aiokafka doesn't have a retries parameter - it uses retry_backoff_ms instead
                    self._producer = AIOKafkaProducer(
                        bootstrap_servers=self._settings.kafka.bootstrap_servers,
                        acks=self._settings.kafka.producer_acks,
                        compression_type=self._settings.kafka.producer_compression_type,
                        retry_backoff_ms=100,  # Retry backoff in milliseconds
                        **self._settings.kafka.auth_kwargs,
                    )

                    # Start producer
                    await self._producer.start()
                    logger.info(
                        f"Kafka producer started with {self._settings.kafka.serialization_format} serialization"
                    )

                except ImportError:
                    self._producer = None
                    logger.error(
                        "aiokafka not installed. Install with: pip install aiokafka"
                    )
                    return None
                except Exception as e:
                    self._producer = None
                    logger.exception(f"Failed to create Kafka producer: {e}")
                    return None

            return self._producer

    async def _ensure_topic_exists(self, topic: str) -> bool:
        """
        Ensure a Kafka topic exists, creating it if necessary.

        Args:
            topic: Topic name to check/create

        Returns:
            True if topic exists or was created, False on error
        """
        # Skip if we've already created this topic in this session
        if topic in self._created_topics:
            return True

        try:
            from aiokafka.admin import AIOKafkaAdminClient, NewTopic
            from aiokafka.errors import TopicAlreadyExistsError

            if not self._settings:
                return False

            # Create admin client
            admin_client = AIOKafkaAdminClient(
                bootstrap_servers=self._settings.kafka.bootstrap_servers,
                **self._settings.kafka.auth_kwargs,
            )

            try:
                await admin_client.start()

                # Try to create the topic
                new_topic = NewTopic(
                    name=topic,
                    num_partitions=self._settings.kafka.topic_partitions,
                    replication_factor=self._settings.kafka.topic_replication_factor,
                )

                try:
                    await admin_client.create_topics([new_topic])
                    logger.info(f"Created Kafka topic: {topic}")
                    self._created_topics.add(topic)
                    return True
                except TopicAlreadyExistsError:
                    logger.debug(f"Kafka topic already exists: {topic}")
                    self._created_topics.add(topic)
                    return True

            finally:
                await admin_client.close()

        except ImportError:
            logger.warning("aiokafka.admin not available, skipping topic creation")
            return True  # Assume topic exists
        except Exception as e:
            logger.warning(f"Failed to ensure topic exists: {e}")
            return True  # Continue anyway, let publish fail if topic doesn't exist

    async def publish_result(
        self,
        source_agent: str,
        correlation_id: str,
        payload: dict,
        topic: Optional[str] = None,
    ) -> bool:
        """
        Publish a successful agent result to Kafka.

        Args:
            source_agent: Name of the agent
            correlation_id: Correlation ID (task_id)
            payload: Result data
            topic: Kafka topic (None = use from settings)

        Returns:
            True if published successfully, False otherwise
        """
        # Ensure producer is initialized (which also initializes settings)
        producer = await self._get_producer()
        if producer is None:
            logger.error("Failed to get Kafka producer - producer is None")
            return False

        if not self._settings or not self._settings.kafka.publish_results:
            logger.warning("Kafka result publishing disabled")
            return False

        logger.info(f"Got producer instance: {producer}")

        # Create envelope
        envelope = AgentEnvelope.create_success(
            source_agent=source_agent,
            correlation_id=correlation_id,
            payload=payload,
            message_type=MessageType.AGENT_RESULT,
        )

        # Determine topic
        topic = topic or self._settings.kafka.results_topic

        # Ensure topic exists
        await self._ensure_topic_exists(topic)

        # Serialize and publish
        try:
            value = self._serialize_envelope(envelope, topic)

            # Headers for OpenTelemetry tracing and message tracking
            headers = [
                ("correlation_id", correlation_id.encode("utf-8")),
                ("source_agent", source_agent.encode("utf-8")),
            ]

            await producer.send_and_wait(topic=topic, value=value, headers=headers)
            logger.info(
                f"Published result to topic '{topic}' "
                f"(correlation_id={correlation_id}, agent={source_agent})"
            )
            m = get_metrics()
            if m:
                m.kafka_messages_produced_total.labels(topic=topic).inc()
            return True
        except Exception as e:
            logger.exception(f"Failed to publish result to Kafka: {e}")
            m = get_metrics()
            if m:
                m.kafka_produce_errors_total.labels(topic=topic).inc()
            return False

    async def publish_qc_result(
        self,
        event: QCTicketResultEvent,
        topic: Optional[str] = None,
    ) -> bool:
        """
        Publish a QCTicketResultEvent directly to Kafka.

        This bypasses the AgentEnvelope wrapper and publishes
        the flat QCTicketResultEvent schema expected by downstream consumers.

        Args:
            event: QCTicketResultEvent instance
            topic: Kafka topic (None = use from settings)

        Returns:
            True if published successfully, False otherwise
        """
        producer = await self._get_producer()
        if producer is None:
            logger.error("Failed to get Kafka producer - producer is None")
            return False

        if not self._settings or not self._settings.kafka.publish_results:
            logger.warning("Kafka result publishing disabled")
            return False

        topic = topic or self._settings.kafka.results_topic
        await self._ensure_topic_exists(topic)

        try:
            value = self._serialize_qc_result(event, topic)

            headers = [
                ("chat_id", event.chat_id.encode("utf-8")),
                ("event_name", event.event_name.encode("utf-8")),
            ]

            await producer.send_and_wait(
                topic=topic, value=value, headers=headers
            )
            logger.info(
                f"Published QC result to topic '{topic}' "
                f"(chat_id={event.chat_id})"
            )
            m = get_metrics()
            if m:
                m.kafka_messages_produced_total.labels(topic=topic).inc()
            return True
        except Exception as e:
            logger.exception(f"Failed to publish QC result to Kafka: {e}")
            m = get_metrics()
            if m:
                m.kafka_produce_errors_total.labels(topic=topic).inc()
            return False

    async def publish_error(
        self,
        source_agent: str,
        correlation_id: str,
        error: str,
        topic: Optional[str] = None,
    ) -> bool:
        """
        Publish an agent error to Kafka.

        Args:
            source_agent: Name of the agent
            correlation_id: Correlation ID (task_id)
            error: Error message
            topic: Kafka topic (None = use from settings)

        Returns:
            True if published successfully, False otherwise
        """
        # Ensure producer is initialized (which also initializes settings)
        producer = await self._get_producer()
        if producer is None:
            return False

        if not self._settings or not self._settings.kafka.publish_results:
            logger.debug("Kafka result publishing disabled")
            return False

        # Create envelope
        envelope = AgentEnvelope.create_failure(
            source_agent=source_agent,
            correlation_id=correlation_id,
            error=error,
            message_type=MessageType.AGENT_ERROR,
        )

        # Determine topic
        topic = topic or self._settings.kafka.results_topic

        # Ensure topic exists
        await self._ensure_topic_exists(topic)

        # Serialize and publish
        try:
            value = self._serialize_envelope(envelope, topic)

            # Headers for OpenTelemetry tracing and message tracking
            headers = [
                ("correlation_id", correlation_id.encode("utf-8")),
                ("source_agent", source_agent.encode("utf-8")),
            ]

            await producer.send_and_wait(topic=topic, value=value, headers=headers)
            logger.info(
                f"Published error to topic '{topic}' "
                f"(correlation_id={correlation_id}, agent={source_agent})"
            )
            m = get_metrics()
            if m:
                m.kafka_messages_produced_total.labels(topic=topic).inc()
            return True
        except Exception as e:
            logger.exception(f"Failed to publish error to Kafka: {e}")
            m = get_metrics()
            if m:
                m.kafka_produce_errors_total.labels(topic=topic).inc()
            return False

    async def close(self):
        """Close the producer connection, flushing any pending messages first."""
        if self._producer:
            try:
                # Flush ensures all buffered messages are sent before stopping
                await self._producer.flush()
                await self._producer.stop()
                logger.info("Kafka producer stopped")
            except Exception as e:
                logger.exception(f"Error stopping Kafka producer: {e}")
            finally:
                self._producer = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.close()


__all__ = ["KafkaProducer"]
