"""
Kafka consumer utility for consuming agent input messages.

This module provides a simple interface to consume messages from Kafka topics.
Supports both JSON and Avro serialization formats, configurable via settings.

For input messages, it deserializes QCTicketInputEvent Avro records into plain
dictionaries (no AgentEnvelope wrapping on the input side).
"""

import asyncio
import json
import logging
from typing import Optional, Callable, Any, Dict

from .avro_serializer import get_avro_serializer

from aiokafka import AIOKafkaConsumer
from src.config.settings import get_settings
from src.metrics.registry import get_metrics

logger = logging.getLogger(__name__)


class KafkaConsumer:
    """
    Kafka consumer for consuming agent input messages.

    This class manages a consumer instance and provides methods to
    consume messages from Kafka topics and process them.
    """

    def __init__(self):
        self._consumer = None
        self._settings = None
        self._running = False
        self._lock = asyncio.Lock()

    def _get_value_deserializer(self) -> Callable[[bytes], Dict[str, Any]]:
        """
        Get the appropriate value deserializer based on settings.

        Returns:
            Deserializer function that converts bytes to a dictionary
        """
        if self._settings.kafka.serialization_format == "avro":
            serializer = get_avro_serializer()
            topic = self._settings.kafka.input_topic

            def avro_deserializer(data: bytes) -> Dict[str, Any]:
                try:
                    record = serializer.deserialize_input(data, topic)
                    logger.info(f"Deserialized Avro message: {record}")
                    return record
                except Exception as e:
                    logger.error(f"Confluent Avro deserialization failed: {e}, raw_len={len(data)}")
                    # Fall back to JSON if Avro fails
                    try:
                        return json.loads(data.decode("utf-8"))
                    except Exception:
                        logger.error(f"JSON fallback also failed: {e}")
                        raise

            return avro_deserializer
        else:
            # Default to JSON deserialization
            def json_deserializer(data: bytes) -> Dict[str, Any]:
                try:
                    return json.loads(data.decode("utf-8"))
                except Exception as e:
                    logger.error(f"Failed to deserialize message: {e}")
                    raise

            return json_deserializer

    async def _get_consumer(self):
        """Get or create Kafka consumer instance with thread-safe initialization."""
        async with self._lock:
            if self._consumer is None:
                try:
                    self._settings = get_settings()

                    if not self._settings.kafka.enabled:
                        logger.warning("Kafka is disabled, skipping consumer creation")
                        return None

                    value_deserializer = self._get_value_deserializer()

                    self._consumer = AIOKafkaConsumer(
                        self._settings.kafka.input_topic,
                        bootstrap_servers=self._settings.kafka.bootstrap_servers,
                        group_id=self._settings.kafka.consumer_group,
                        auto_offset_reset=self._settings.kafka.auto_offset_reset,
                        max_poll_interval_ms=self._settings.kafka.max_poll_interval_ms,
                        value_deserializer=value_deserializer,
                        enable_auto_commit=False,
                        **self._settings.kafka.auth_kwargs,
                    )

                    await self._consumer.start()
                    logger.info(
                        f"Kafka consumer started for topic: {self._settings.kafka.input_topic} "
                        f"with {self._settings.kafka.serialization_format} deserialization"
                    )

                except ImportError:
                    self._consumer = None
                    logger.error(
                        "aiokafka not installed. Install with: pip install aiokafka"
                    )
                    return None
                except Exception as e:
                    self._consumer = None
                    logger.exception(f"Failed to create Kafka consumer: {e}")
                    return None

            return self._consumer

    async def consume_and_process(
        self,
        process_func: Callable[[Dict[str, Any]], Any],
        error_func: Optional[Callable[[str, Exception], None]] = None,
    ):
        """
        Continuously consume messages and process them.

        Args:
            process_func: Async function to process each message
            error_func: Optional function to handle errors
        """
        consumer = await self._get_consumer()
        if consumer is None:
            logger.error("Failed to get Kafka consumer")
            return

        self._running = True
        logger.info(
            f"Starting to consume messages from topic: {self._settings.kafka.input_topic}"
        )

        try:
            async for msg in consumer:
                # Check if stop() was called - prevents processing stale messages
                if not self._running:
                    logger.info("Consumer stopped, exiting loop")
                    break

                message_data = None
                try:
                    # Deserializer returns a plain dict
                    message_data = msg.value

                    logger.info(f"Parsed message data: {message_data}")
                    logger.info(
                        f"Processing message from topic '{msg.topic}' "
                        f"(partition={msg.partition}, offset={msg.offset})"
                    )

                    m = get_metrics()
                    if m:
                        m.kafka_messages_consumed_total.labels(topic=msg.topic).inc()

                    # Process the message
                    await process_func(message_data)

                except Exception as e:
                    logger.exception(f"Error processing message: {e}")
                    m = get_metrics()
                    if m:
                        m.kafka_consume_errors_total.labels(topic=msg.topic).inc()
                    if error_func:
                        correlation_id = (
                            message_data.get("chat_id", "unknown")
                            if message_data
                            else "unknown"
                        )
                        result = error_func(correlation_id, e)
                        # Support both sync and async error handlers
                        if asyncio.iscoroutine(result):
                            await result
                finally:
                    # Commit offset only after successful processing
                    # This ensures at-least-once delivery: if agent crashes mid-work,
                    # the message will be reprocessed on restart
                    await consumer.commit()
                    logger.debug(
                        f"Committed offset for partition={msg.partition}, offset={msg.offset}"
                    )

        except asyncio.CancelledError:
            # Expected when stop() is called - consumer.stop() breaks the async for loop
            logger.info("Consumer loop cancelled")
        except Exception as e:
            # Only log unexpected errors
            if self._running:
                logger.exception(f"Consumer error: {e}")
        finally:
            await self.stop()

    async def stop(self):
        """Stop the consumer and release resources."""
        self._running = False
        if self._consumer:
            try:
                await self._consumer.stop()
                logger.info("Kafka consumer stopped")
            except Exception as e:
                logger.exception(f"Error stopping consumer: {e}")
            finally:
                self._consumer = None

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.stop()

    def is_running(self) -> bool:
        """Check if consumer is running."""
        return self._running


__all__ = ["KafkaConsumer"]
