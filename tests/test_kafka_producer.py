"""
Unit tests for the Kafka producer module.

Tests cover:
- KafkaProducer initialization and configuration
- Value serializer selection (JSON vs Avro)
- Message publishing (success and error)
- Topic creation
- Producer lifecycle (start/stop)
- Integration tests with real Kafka
"""

import asyncio
import json
import os
import pytest
import uuid
from datetime import datetime, timezone
from unittest.mock import AsyncMock, MagicMock, patch

from src.kafka.producer import KafkaProducer
from src.kafka.models import (
    AgentEnvelope,
    MessageType,
    EnvelopeStatus,
)


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
    settings.kafka.results_topic = "test-results"
    settings.kafka.publish_results = True
    settings.kafka.serialization_format = "json"
    settings.kafka.producer_acks = "all"
    settings.kafka.producer_compression_type = "snappy"
    settings.kafka.topic_partitions = 3
    settings.kafka.topic_replication_factor = 1
    settings.kafka.auth_kwargs = {}
    return settings


@pytest.fixture
def mock_settings_avro():
    """Create mock settings with Avro serialization."""
    settings = MagicMock()
    settings.kafka.enabled = True
    settings.kafka.bootstrap_servers = "localhost:9092"
    settings.kafka.results_topic = "test-results"
    settings.kafka.publish_results = True
    settings.kafka.serialization_format = "avro"
    settings.kafka.producer_acks = "all"
    settings.kafka.producer_compression_type = "snappy"
    settings.kafka.topic_partitions = 3
    settings.kafka.topic_replication_factor = 1
    settings.kafka.auth_kwargs = {}
    return settings


@pytest.fixture
def mock_settings_disabled():
    """Create mock settings with Kafka disabled."""
    settings = MagicMock()
    settings.kafka.enabled = False
    return settings


@pytest.fixture
def mock_settings_publish_disabled():
    """Create mock settings with publishing disabled."""
    settings = MagicMock()
    settings.kafka.enabled = True
    settings.kafka.publish_results = False
    return settings


@pytest.fixture
def sample_payload():
    """Create a sample payload."""
    return {
        "chat_id": "chat-123",
        "chat_conversation": [
            {"role": "end_user", "message": "Hello"},
            {"role": "support", "message": "Hi, how can I help?"},
        ],
        "qc_score": 85,
    }


# ============================================================================
# KafkaProducer Initialization Tests
# ============================================================================


class TestKafkaProducerInit:
    """Tests for KafkaProducer initialization."""

    def test_init_creates_instance(self):
        """Test that KafkaProducer initializes with correct defaults."""
        producer = KafkaProducer()

        assert producer._producer is None
        assert producer._settings is None
        assert producer._created_topics == set()
        assert producer._lock is not None


# ============================================================================
# Value Serializer Tests
# ============================================================================


class TestValueSerializer:
    """Tests for value serialization methods."""

    def test_serialize_envelope_json(self, mock_settings):
        """Test JSON serialization of AgentEnvelope."""
        producer = KafkaProducer()
        producer._settings = mock_settings

        envelope = AgentEnvelope.create_success(
            source_agent="test-agent",
            correlation_id="test-123",
            payload={"data": "test"},
        )

        result = producer._serialize_envelope(envelope, "test-topic")

        assert isinstance(result, bytes)
        deserialized = json.loads(result.decode("utf-8"))
        assert deserialized["meta"]["source_agent"] == "test-agent"

    @patch("src.kafka.avro_serializer.get_avro_serializer")
    def test_serialize_envelope_avro(self, mock_get_serializer, mock_settings_avro):
        """Test Avro serialization of AgentEnvelope."""
        mock_serializer = MagicMock()
        mock_serializer.serialize.return_value = b"\x00\x01\x02"
        mock_get_serializer.return_value = mock_serializer

        producer = KafkaProducer()
        producer._settings = mock_settings_avro

        envelope = AgentEnvelope.create_success(
            source_agent="test-agent",
            correlation_id="test-123",
            payload={"data": "test"},
        )

        result = producer._serialize_envelope(envelope, "test-topic")

        mock_serializer.serialize.assert_called_once_with(envelope, "test-topic")
        assert result == b"\x00\x01\x02"

    def test_serialize_qc_result_json(self, mock_settings):
        """Test JSON serialization of QCTicketResultEvent."""
        from src.kafka.models import QCTicketResultEvent

        producer = KafkaProducer()
        producer._settings = mock_settings

        event = QCTicketResultEvent(
            chat_id="chat-123",
            main_problem="test problem",
            score="85",
            tone_score="90",
            empathy_score="80",
            solution_quality="75",
            clarity_score="88",
            key_observations=["obs1", "obs2"],
            reasons="test reasons",
            time_stamp=1234567890.0,
            event_name="qc_ticket_result",
        )

        result = producer._serialize_qc_result(event, "test-topic")

        assert isinstance(result, bytes)
        deserialized = json.loads(result.decode("utf-8"))
        assert deserialized["chat_id"] == "chat-123"
        assert deserialized["score"] == "85"
        assert deserialized["key_observations"] == ["obs1", "obs2"]

    @patch("src.kafka.avro_serializer.get_avro_serializer")
    def test_serialize_qc_result_avro(self, mock_get_serializer, mock_settings_avro):
        """Test Avro serialization of QCTicketResultEvent."""
        from src.kafka.models import QCTicketResultEvent

        mock_serializer = MagicMock()
        mock_serializer.serialize_qc_result.return_value = b"\x00\x01\x02"
        mock_get_serializer.return_value = mock_serializer

        producer = KafkaProducer()
        producer._settings = mock_settings_avro

        event = QCTicketResultEvent(
            chat_id="chat-123",
            score="85",
        )

        result = producer._serialize_qc_result(event, "test-topic")

        mock_serializer.serialize_qc_result.assert_called_once_with(event, "test-topic")
        assert result == b"\x00\x01\x02"


# ============================================================================
# Producer Creation Tests
# ============================================================================


class TestGetProducer:
    """Tests for _get_producer method."""

    @pytest.mark.asyncio
    async def test_get_producer_creates_producer(self, mock_settings):
        """Test that _get_producer creates and starts a producer."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()
            result = await producer._get_producer()

            assert result == mock_producer_instance
            mock_aiokafka.assert_called_once()
            mock_producer_instance.start.assert_called_once()

    @pytest.mark.asyncio
    async def test_get_producer_returns_none_when_disabled(
        self, mock_settings_disabled
    ):
        """Test that _get_producer returns None when Kafka is disabled."""
        with patch("src.kafka.producer.get_settings") as mock_get_settings:
            mock_get_settings.return_value = mock_settings_disabled

            producer = KafkaProducer()
            result = await producer._get_producer()

            assert result is None

    @pytest.mark.asyncio
    async def test_get_producer_reuses_existing(self, mock_settings):
        """Test that _get_producer reuses existing producer instance."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            # First call creates producer
            result1 = await producer._get_producer()
            # Second call should reuse
            result2 = await producer._get_producer()

            assert result1 == result2
            # AIOKafkaProducer should only be called once
            assert mock_aiokafka.call_count == 1

    @pytest.mark.asyncio
    async def test_get_producer_handles_exception(self, mock_settings):
        """Test that _get_producer handles exceptions gracefully."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_aiokafka.side_effect = Exception("Connection failed")

            producer = KafkaProducer()
            result = await producer._get_producer()

            assert result is None


# ============================================================================
# Topic Creation Tests
# ============================================================================


class TestEnsureTopicExists:
    """Tests for _ensure_topic_exists method."""

    @pytest.mark.asyncio
    async def test_ensure_topic_exists_skips_if_already_created(self, mock_settings):
        """Test that topic creation is skipped if already created."""
        producer = KafkaProducer()
        producer._settings = mock_settings
        producer._created_topics.add("test-topic")

        result = await producer._ensure_topic_exists("test-topic")

        assert result is True

    @pytest.mark.asyncio
    async def test_ensure_topic_exists_creates_topic(self, mock_settings):
        """Test that topic is created if it doesn't exist."""
        with patch("aiokafka.admin.AIOKafkaAdminClient") as mock_admin_client_class:
            mock_admin_instance = AsyncMock()
            mock_admin_client_class.return_value = mock_admin_instance

            producer = KafkaProducer()
            producer._settings = mock_settings

            result = await producer._ensure_topic_exists("new-topic")

            assert result is True
            assert "new-topic" in producer._created_topics
            mock_admin_instance.start.assert_called_once()
            mock_admin_instance.create_topics.assert_called_once()
            mock_admin_instance.close.assert_called_once()

    @pytest.mark.asyncio
    async def test_ensure_topic_exists_handles_already_exists_error(
        self, mock_settings
    ):
        """Test that TopicAlreadyExistsError is handled gracefully."""
        from aiokafka.errors import TopicAlreadyExistsError

        with patch("aiokafka.admin.AIOKafkaAdminClient") as mock_admin_client_class:
            mock_admin_instance = AsyncMock()
            mock_admin_instance.create_topics.side_effect = TopicAlreadyExistsError()
            mock_admin_client_class.return_value = mock_admin_instance

            producer = KafkaProducer()
            producer._settings = mock_settings

            result = await producer._ensure_topic_exists("existing-topic")

            assert result is True
            assert "existing-topic" in producer._created_topics


# ============================================================================
# Message Publishing Tests
# ============================================================================


class TestPublishResult:
    """Tests for publish_result method."""

    @pytest.fixture(autouse=True)
    def _stub_topic_creation(self):
        """Avoid real Kafka admin calls during unit tests."""
        with patch(
            "src.kafka.producer.KafkaProducer._ensure_topic_exists",
            new=AsyncMock(return_value=True),
        ):
            yield

    @pytest.mark.asyncio
    async def test_publish_result_success(self, mock_settings, sample_payload):
        """Test successful result publishing."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            result = await producer.publish_result(
                source_agent="test-agent",
                correlation_id="test-123",
                payload=sample_payload,
            )

            assert result is True
            mock_producer_instance.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_result_returns_false_when_disabled(
        self, mock_settings_disabled, sample_payload
    ):
        """Test that publish_result returns False when Kafka is disabled."""
        with patch("src.kafka.producer.get_settings") as mock_get_settings:
            mock_get_settings.return_value = mock_settings_disabled

            producer = KafkaProducer()

            result = await producer.publish_result(
                source_agent="test-agent",
                correlation_id="test-123",
                payload=sample_payload,
            )

            assert result is False

    @pytest.mark.asyncio
    async def test_publish_result_with_custom_topic(
        self, mock_settings, sample_payload
    ):
        """Test publishing to a custom topic."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            result = await producer.publish_result(
                source_agent="test-agent",
                correlation_id="test-123",
                payload=sample_payload,
                topic="custom-topic",
            )

            assert result is True
            call_kwargs = mock_producer_instance.send_and_wait.call_args[1]
            assert call_kwargs["topic"] == "custom-topic"

    @pytest.mark.asyncio
    async def test_publish_result_handles_exception(
        self, mock_settings, sample_payload
    ):
        """Test that publish_result handles exceptions gracefully."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_producer_instance.send_and_wait.side_effect = Exception("Send failed")
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            result = await producer.publish_result(
                source_agent="test-agent",
                correlation_id="test-123",
                payload=sample_payload,
            )

            assert result is False


class TestPublishError:
    """Tests for publish_error method."""

    @pytest.fixture(autouse=True)
    def _stub_topic_creation(self):
        """Avoid real Kafka admin calls during unit tests."""
        with patch(
            "src.kafka.producer.KafkaProducer._ensure_topic_exists",
            new=AsyncMock(return_value=True),
        ):
            yield

    @pytest.mark.asyncio
    async def test_publish_error_success(self, mock_settings):
        """Test successful error publishing."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            result = await producer.publish_error(
                source_agent="test-agent",
                correlation_id="test-123",
                error="Test error message",
            )

            assert result is True
            mock_producer_instance.send_and_wait.assert_called_once()

    @pytest.mark.asyncio
    async def test_publish_error_creates_failure_envelope(self, mock_settings):
        """Test that publish_error creates a FAILED envelope."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            await producer.publish_error(
                source_agent="test-agent",
                correlation_id="test-123",
                error="Test error",
            )

            # Verify the envelope passed to send_and_wait
            call_kwargs = mock_producer_instance.send_and_wait.call_args[1]
            envelope_dict = json.loads(call_kwargs["value"].decode("utf-8"))

            assert envelope_dict["status"] == "FAILED"
            assert envelope_dict["error"] == "Test error"
            assert envelope_dict["payload"] == {}


# ============================================================================
# Producer Lifecycle Tests
# ============================================================================


class TestProducerLifecycle:
    """Tests for producer lifecycle."""

    @pytest.mark.asyncio
    async def test_close_with_no_producer(self):
        """Test that close() works when producer is None."""
        producer = KafkaProducer()

        # Should not raise
        await producer.close()

    @pytest.mark.asyncio
    async def test_close_stops_producer(self):
        """Test that close() calls stop on the underlying producer."""
        producer = KafkaProducer()

        mock_producer_instance = AsyncMock()
        producer._producer = mock_producer_instance

        await producer.close()

        mock_producer_instance.stop.assert_called_once()

    @pytest.mark.asyncio
    async def test_close_handles_exception(self):
        """Test that close() handles exceptions gracefully."""
        producer = KafkaProducer()

        mock_producer_instance = AsyncMock()
        mock_producer_instance.stop.side_effect = Exception("Stop failed")
        producer._producer = mock_producer_instance

        # Should not raise
        await producer.close()


# ============================================================================
# Thread Safety Tests
# ============================================================================


class TestThreadSafety:
    """Tests for thread safety of KafkaProducer."""

    @pytest.mark.asyncio
    async def test_concurrent_get_producer_calls(self, mock_settings):
        """Test that concurrent _get_producer calls only create one producer."""
        with (
            patch("src.kafka.producer.get_settings") as mock_get_settings,
            patch("src.kafka.producer.AIOKafkaProducer") as mock_aiokafka,
        ):
            mock_get_settings.return_value = mock_settings
            mock_producer_instance = AsyncMock()
            mock_aiokafka.return_value = mock_producer_instance

            producer = KafkaProducer()

            # Make multiple concurrent calls
            results = await asyncio.gather(
                producer._get_producer(),
                producer._get_producer(),
                producer._get_producer(),
            )

            # All results should be the same instance
            assert all(r == mock_producer_instance for r in results)
            # Producer should only be created once
            assert mock_aiokafka.call_count == 1


# ============================================================================
# Integration Tests (Requires Running Kafka)
# ============================================================================


@pytest.mark.integration
class TestKafkaProducerIntegration:
    """
    Integration tests that require a running Kafka instance on 127.0.0.1:9092.

    Run these tests with: pytest -m integration tests/test_kafka_producer.py

    Prerequisites:
    - Kafka running on 127.0.0.1:9092
    - Topics will be auto-created if they don't exist
    """

    @pytest.mark.asyncio
    async def test_publish_json_result(self):
        """Test publishing a JSON result message to Kafka."""
        # Generate unique topic for this test run
        test_topic = generate_test_topic("producer-json-results")

        # Set environment variables for test
        os.environ["KAFKA__ENABLED"] = "true"
        os.environ["KAFKA__BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"
        os.environ["KAFKA__SERIALIZATION_FORMAT"] = "json"
        os.environ["KAFKA__RESULTS_TOPIC"] = test_topic
        os.environ["KAFKA__PUBLISH_RESULTS"] = "true"

        # Create producer
        producer = KafkaProducer()

        # Test data
        test_correlation_id = f"test-producer-{datetime.now(timezone.utc).timestamp()}"
        test_payload = {
            "chat_id": "chat-789",
            "qc_score": 92,
            "analysis": "Excellent customer service",
        }

        # Publish message
        result = await producer.publish_result(
            source_agent="test-producer-agent",
            correlation_id=test_correlation_id,
            payload=test_payload,
        )

        assert result is True, "Failed to publish JSON message to Kafka"

        # Cleanup
        await producer.close()
        await delete_kafka_topic(test_topic)

    @pytest.mark.asyncio
    async def test_publish_avro_result(self):
        """Test publishing an Avro result message to Kafka."""
        # Generate unique topic for this test run
        test_topic = generate_test_topic("producer-avro-results")

        # Set environment variables for test
        os.environ["KAFKA__ENABLED"] = "true"
        os.environ["KAFKA__BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"
        os.environ["KAFKA__SERIALIZATION_FORMAT"] = "avro"
        os.environ["KAFKA__RESULTS_TOPIC"] = test_topic
        os.environ["KAFKA__PUBLISH_RESULTS"] = "true"

        # Create producer
        producer = KafkaProducer()

        # Test data
        test_correlation_id = (
            f"test-producer-avro-{datetime.now(timezone.utc).timestamp()}"
        )
        test_payload = {
            "chat_id": "chat-avro-789",
            "qc_score": 88,
            "analysis": "Good response time",
        }

        # Publish message
        result = await producer.publish_result(
            source_agent="test-producer-avro-agent",
            correlation_id=test_correlation_id,
            payload=test_payload,
        )

        assert result is True, "Failed to publish Avro message to Kafka"

        # Cleanup
        await producer.close()
        await delete_kafka_topic(test_topic)

    @pytest.mark.asyncio
    async def test_publish_error_message(self):
        """Test publishing an error message to Kafka."""
        # Generate unique topic for this test run
        test_topic = generate_test_topic("producer-errors")

        # Set environment variables for test
        os.environ["KAFKA__ENABLED"] = "true"
        os.environ["KAFKA__BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"
        os.environ["KAFKA__SERIALIZATION_FORMAT"] = "json"
        os.environ["KAFKA__RESULTS_TOPIC"] = test_topic
        os.environ["KAFKA__PUBLISH_RESULTS"] = "true"

        # Create producer
        producer = KafkaProducer()

        # Test data
        test_correlation_id = (
            f"test-producer-error-{datetime.now(timezone.utc).timestamp()}"
        )
        test_error = "QC evaluation failed: Invalid ticket format"

        # Publish error message
        result = await producer.publish_error(
            source_agent="test-producer-error-agent",
            correlation_id=test_correlation_id,
            error=test_error,
        )

        assert result is True, "Failed to publish error message to Kafka"

        # Cleanup
        await producer.close()
        await delete_kafka_topic(test_topic)
        await delete_kafka_topic(test_topic)

    @pytest.mark.asyncio
    async def test_producer_reuse(self):
        """Test that producer instance is reused across multiple publishes."""
        # Generate unique topic for this test run
        test_topic = generate_test_topic("producer-reuse")

        # Set environment variables for test
        os.environ["KAFKA__ENABLED"] = "true"
        os.environ["KAFKA__BOOTSTRAP_SERVERS"] = "127.0.0.1:9092"
        os.environ["KAFKA__SERIALIZATION_FORMAT"] = "json"
        os.environ["KAFKA__RESULTS_TOPIC"] = test_topic
        os.environ["KAFKA__PUBLISH_RESULTS"] = "true"

        # Create producer
        producer = KafkaProducer()

        # Publish first message
        result1 = await producer.publish_result(
            source_agent="test-reuse-agent",
            correlation_id="test-reuse-1",
            payload={"message": 1},
        )

        # Get producer instance
        producer_instance_1 = producer._producer

        # Publish second message
        result2 = await producer.publish_result(
            source_agent="test-reuse-agent",
            correlation_id="test-reuse-2",
            payload={"message": 2},
        )

        # Get producer instance again
        producer_instance_2 = producer._producer

        # Verify both publishes succeeded
        assert result1 is True
        assert result2 is True

        # Verify same producer instance was reused
        assert producer_instance_1 is producer_instance_2

        # Cleanup
        await producer.close()
