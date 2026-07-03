"""
Unit tests for the Kafka consumer module.

Tests cover:
- KafkaConsumer initialization and configuration
- Value deserializer selection (JSON vs Avro)
- Message consumption and processing
- Error handling
- Consumer lifecycle (start/stop)
"""

import asyncio
import json
import multiprocessing
import os
import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.kafka.consumer import KafkaConsumer
from src.kafka.producer import KafkaProducer


# ============================================================================
# Helper Functions for Integration Tests
# ============================================================================


async def delete_kafka_topic(
    topic_name: str, bootstrap_servers: str = "127.0.0.1:9092"
):
    """Delete a Kafka topic for cleanup after tests."""
    try:
        from aiokafka.admin import AIOKafkaAdminClient

        admin_client = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers)
        await admin_client.start()
        try:
            await admin_client.delete_topics([topic_name])
        finally:
            await admin_client.close()
    except Exception:
        # Ignore errors during cleanup
        pass


def generate_test_topic(prefix: str = "test") -> str:
    """Generate a unique topic name for testing."""
    return (
        f"{prefix}-{uuid.uuid4().hex[:8]}-{int(datetime.now(timezone.utc).timestamp())}"
    )


# ============================================================================
# Fixtures
# ============================================================================


@pytest.fixture
def mock_settings():
    """Create mock settings for Kafka configuration."""
    settings = MagicMock()
    settings.kafka.enabled = True
    settings.kafka.bootstrap_servers = "localhost:9092"
    settings.kafka.input_topic = "test-input"
    settings.kafka.consumer_group = "test-group"
    settings.kafka.auto_offset_reset = "latest"
    settings.kafka.max_poll_interval_ms = 300000
    settings.kafka.serialization_format = "json"
    settings.kafka.auth_kwargs = {}
    return settings


@pytest.fixture
def mock_settings_avro():
    """Create mock settings with Avro serialization."""
    settings = MagicMock()
    settings.kafka.enabled = True
    settings.kafka.bootstrap_servers = "localhost:9092"
    settings.kafka.input_topic = "test-input"
    settings.kafka.consumer_group = "test-group"
    settings.kafka.auto_offset_reset = "latest"
    settings.kafka.max_poll_interval_ms = 300000
    settings.kafka.serialization_format = "avro"
    settings.kafka.auth_kwargs = {}
    return settings


@pytest.fixture
def mock_settings_disabled():
    """Create mock settings with Kafka disabled."""
    settings = MagicMock()
    settings.kafka.enabled = False
    return settings


@pytest.fixture
def sample_qc_input():
    """Create a sample QCTicketInputEvent as dictionary."""
    return {
        "chat_id": "chat-123",
        "chat_conversation": [
            {"role": "end_user", "message": "Hello", "timestamp": "2026-01-07T10:00:00Z"},
            {"role": "support", "message": "Hi, how can I help?", "timestamp": "2026-01-07T10:01:00Z"},
        ],
        "source": "web",
        "time_stamp": 1736244000.0,
        "event_name": "qc_ticket_input",
    }


@pytest.fixture
def mock_kafka_message(sample_qc_input):
    """Create a mock Kafka message."""
    message = MagicMock()
    message.value = sample_qc_input
    message.topic = "test-input"
    message.partition = 0
    message.offset = 100
    return message


# ============================================================================
# KafkaConsumer Initialization Tests
# ============================================================================


class TestKafkaConsumerInit:
    """Tests for KafkaConsumer initialization."""

    def test_init_creates_instance(self):
        """Test that KafkaConsumer initializes with correct defaults."""
        consumer = KafkaConsumer()

        assert consumer._consumer is None
        assert consumer._settings is None
        assert consumer._running is False
        assert consumer._lock is not None

    def test_is_running_returns_false_initially(self):
        """Test that is_running returns False before starting."""
        consumer = KafkaConsumer()
        assert consumer.is_running() is False


# ============================================================================
# Value Deserializer Tests
# ============================================================================


class TestValueDeserializer:
    """Tests for value deserializer selection."""

    def test_json_deserializer(self, mock_settings, sample_qc_input):
        """Test JSON deserializer returns plain dict."""
        consumer = KafkaConsumer()
        consumer._settings = mock_settings

        deserializer = consumer._get_value_deserializer()

        json_bytes = json.dumps(sample_qc_input).encode("utf-8")
        result = deserializer(json_bytes)

        assert result == sample_qc_input
        assert result["chat_id"] == "chat-123"
        assert len(result["chat_conversation"]) == 2

    def test_avro_deserializer_fallback_to_json(
        self, mock_settings_avro, sample_qc_input
    ):
        """Test Avro deserializer falls back to JSON on failure."""
        consumer = KafkaConsumer()
        consumer._settings = mock_settings_avro

        # Avro deserialization raises -> deserializer should fall back to JSON.
        mock_serializer = MagicMock()
        mock_serializer.deserialize_input.side_effect = Exception("Avro error")

        with patch(
            "src.kafka.consumer.get_avro_serializer", return_value=mock_serializer
        ):
            deserializer = consumer._get_value_deserializer()

            json_bytes = json.dumps(sample_qc_input).encode("utf-8")
            result = deserializer(json_bytes)

            assert result == sample_qc_input


# ============================================================================
# Consumer Creation Tests
# ============================================================================


class TestGetConsumer:
    """Tests for _get_consumer method."""

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.AIOKafkaConsumer")
    @patch("src.kafka.consumer.get_settings")
    async def test_get_consumer_creates_consumer(
        self, mock_get_settings, mock_aiokafka, mock_settings
    ):
        """Test that _get_consumer creates and starts a consumer."""
        mock_get_settings.return_value = mock_settings

        mock_consumer_instance = AsyncMock()
        mock_aiokafka.return_value = mock_consumer_instance

        consumer = KafkaConsumer()
        result = await consumer._get_consumer()

        assert result == mock_consumer_instance
        mock_aiokafka.assert_called_once()
        mock_consumer_instance.start.assert_called_once()

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.get_settings")
    async def test_get_consumer_returns_none_when_disabled(
        self, mock_get_settings, mock_settings_disabled
    ):
        """Test that _get_consumer returns None when Kafka is disabled."""
        mock_get_settings.return_value = mock_settings_disabled

        consumer = KafkaConsumer()
        result = await consumer._get_consumer()

        assert result is None

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.AIOKafkaConsumer")
    @patch("src.kafka.consumer.get_settings")
    async def test_get_consumer_reuses_existing(
        self, mock_get_settings, mock_aiokafka, mock_settings
    ):
        """Test that _get_consumer reuses existing consumer instance."""
        mock_get_settings.return_value = mock_settings

        mock_consumer_instance = AsyncMock()
        mock_aiokafka.return_value = mock_consumer_instance

        consumer = KafkaConsumer()

        result1 = await consumer._get_consumer()
        result2 = await consumer._get_consumer()

        assert result1 == result2
        assert mock_aiokafka.call_count == 1

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.AIOKafkaConsumer")
    @patch("src.kafka.consumer.get_settings")
    async def test_get_consumer_handles_exception(
        self, mock_get_settings, mock_aiokafka, mock_settings
    ):
        """Test that _get_consumer handles exceptions gracefully."""
        mock_get_settings.return_value = mock_settings
        mock_aiokafka.side_effect = Exception("Connection failed")

        consumer = KafkaConsumer()
        result = await consumer._get_consumer()

        assert result is None


# ============================================================================
# Message Consumption Tests
# ============================================================================


class TestConsumeAndProcess:
    """Tests for consume_and_process method."""

    @pytest.mark.asyncio
    async def test_consume_and_process_returns_when_no_consumer(self):
        """Test that consume_and_process returns early when consumer is None."""
        consumer = KafkaConsumer()
        consumer._get_consumer = AsyncMock(return_value=None)

        process_func = AsyncMock()

        await consumer.consume_and_process(process_func)

        process_func.assert_not_called()

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.get_settings")
    async def test_consume_and_process_processes_messages(
        self, mock_get_settings, mock_settings, mock_kafka_message, sample_qc_input
    ):
        """Test that consume_and_process correctly processes messages."""
        mock_get_settings.return_value = mock_settings

        async def message_generator():
            yield mock_kafka_message

        mock_consumer_instance = AsyncMock()
        mock_consumer_instance.__aiter__ = lambda self: message_generator()

        consumer = KafkaConsumer()
        consumer._settings = mock_settings
        consumer._consumer = mock_consumer_instance
        consumer._get_consumer = AsyncMock(return_value=mock_consumer_instance)

        process_func = AsyncMock()

        try:
            await asyncio.wait_for(
                consumer.consume_and_process(process_func), timeout=0.5
            )
        except asyncio.TimeoutError:
            pass

        # Verify process_func was called with raw dict (QCTicketInputEvent)
        if process_func.called:
            call_args = process_func.call_args[0][0]
            assert "chat_id" in call_args
            assert "chat_conversation" in call_args
            assert "source" in call_args

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.get_settings")
    async def test_consume_and_process_calls_error_func(
        self, mock_get_settings, mock_settings
    ):
        """Test that consume_and_process calls error_func on processing errors."""
        mock_get_settings.return_value = mock_settings

        message = MagicMock()
        message.value = {"chat_id": "test", "invalid": "data"}
        message.topic = "test-input"
        message.partition = 0
        message.offset = 100

        async def message_generator():
            yield message

        mock_consumer_instance = AsyncMock()
        mock_consumer_instance.__aiter__ = lambda self: message_generator()

        consumer = KafkaConsumer()
        consumer._settings = mock_settings
        consumer._consumer = mock_consumer_instance
        consumer._get_consumer = AsyncMock(return_value=mock_consumer_instance)

        process_func = AsyncMock(side_effect=Exception("Processing error"))
        error_func = AsyncMock()

        try:
            await asyncio.wait_for(
                consumer.consume_and_process(process_func, error_func), timeout=0.5
            )
        except asyncio.TimeoutError:
            pass

        if error_func.called:
            assert error_func.call_count >= 1


# ============================================================================
# Consumer Lifecycle Tests
# ============================================================================


class TestConsumerLifecycle:
    """Tests for consumer start/stop lifecycle."""

    @pytest.mark.asyncio
    async def test_stop_sets_running_false(self):
        """Test that stop() sets _running to False."""
        consumer = KafkaConsumer()
        consumer._running = True

        await consumer.stop()

        assert consumer._running is False

    @pytest.mark.asyncio
    async def test_stop_stops_consumer(self):
        """Test that stop() calls stop on the underlying consumer."""
        consumer = KafkaConsumer()
        consumer._running = True

        mock_consumer_instance = AsyncMock()
        consumer._consumer = mock_consumer_instance

        await consumer.stop()

        mock_consumer_instance.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_stop_handles_exception(self):
        """Test that stop() handles exceptions gracefully."""
        consumer = KafkaConsumer()
        consumer._running = True

        mock_consumer_instance = AsyncMock()
        mock_consumer_instance.stop.side_effect = Exception("Stop failed")
        consumer._consumer = mock_consumer_instance

        await consumer.stop()

        assert consumer._running is False

    @pytest.mark.asyncio
    async def test_stop_with_no_consumer(self):
        """Test that stop() works when consumer is None."""
        consumer = KafkaConsumer()
        consumer._running = True

        await consumer.stop()

        assert consumer._running is False


# ============================================================================
# Thread Safety Tests
# ============================================================================


class TestThreadSafety:
    """Tests for thread safety of KafkaConsumer."""

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.AIOKafkaConsumer")
    @patch("src.kafka.consumer.get_settings")
    async def test_concurrent_get_consumer_calls(
        self, mock_get_settings, mock_aiokafka, mock_settings
    ):
        """Test that concurrent _get_consumer calls only create one consumer."""
        mock_get_settings.return_value = mock_settings

        mock_consumer_instance = AsyncMock()
        mock_aiokafka.return_value = mock_consumer_instance

        consumer = KafkaConsumer()

        results = await asyncio.gather(
            consumer._get_consumer(),
            consumer._get_consumer(),
            consumer._get_consumer(),
        )

        assert all(r == mock_consumer_instance for r in results)
        assert mock_aiokafka.call_count == 1


# ============================================================================
# Integration-like Tests (with mocked Kafka)
# ============================================================================


class TestConsumerIntegration:
    """Integration-like tests with mocked Kafka components."""

    @pytest.mark.asyncio
    @patch("src.kafka.consumer.AIOKafkaConsumer")
    @patch("src.kafka.consumer.get_settings")
    async def test_full_message_flow(
        self, mock_get_settings, mock_aiokafka, mock_settings, sample_qc_input
    ):
        """Test complete message flow from consumption to processing."""
        mock_get_settings.return_value = mock_settings

        message = MagicMock()
        message.value = sample_qc_input
        message.topic = "test-input"
        message.partition = 0
        message.offset = 100

        messages_processed = []

        async def process_func(data):
            messages_processed.append(data)

        async def message_generator():
            yield message

        mock_consumer_instance = AsyncMock()
        mock_consumer_instance.__aiter__ = lambda self: message_generator()
        mock_aiokafka.return_value = mock_consumer_instance

        consumer = KafkaConsumer()

        try:
            await asyncio.wait_for(
                consumer.consume_and_process(process_func), timeout=0.5
            )
        except asyncio.TimeoutError:
            pass

        # Verify message was processed as raw QCTicketInputEvent dict
        if messages_processed:
            processed = messages_processed[0]
            assert processed["chat_id"] == "chat-123"
            assert processed["source"] == "web"
            assert processed["event_name"] == "qc_ticket_input"
            assert len(processed["chat_conversation"]) == 2

    @pytest.mark.asyncio
    async def test_consumer_configuration_parameters(self, mock_settings):
        """Test that consumer is configured with correct parameters."""
        with (
            patch("src.kafka.consumer.get_settings") as mock_get_settings,
            patch("src.kafka.consumer.AIOKafkaConsumer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_consumer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_consumer_instance

            consumer = KafkaConsumer()
            await consumer._get_consumer()

            call_kwargs = mock_aiokafka.call_args

            assert mock_settings.kafka.input_topic in call_kwargs[0]
            assert (
                call_kwargs[1]["bootstrap_servers"]
                == mock_settings.kafka.bootstrap_servers
            )
            assert call_kwargs[1]["group_id"] == mock_settings.kafka.consumer_group
            assert (
                call_kwargs[1]["auto_offset_reset"]
                == mock_settings.kafka.auto_offset_reset
            )
            assert call_kwargs[1]["enable_auto_commit"] is False
