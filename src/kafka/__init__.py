"""
Kafka integration module for the Agent Framework.

This module provides:
- KafkaConsumer: Consume messages from Kafka topics
- KafkaProducer: Publish messages to Kafka topics
- AgentKafkaService: Service layer that wraps any agent with Kafka I/O
- AgentEnvelope: Message envelope format for agent communication
- AvroSerializer: Avro serialization for efficient message encoding

Architecture:
    The Kafka I/O is handled at the infrastructure layer, separate from
    the agent business logic. This separation ensures:
    - Agent remains pure business logic
    - Kafka I/O is handled by the service layer
    - Easy to test agent without Kafka
    - Easy to swap Kafka for other message brokers

Serialization:
    The module supports both JSON and Avro serialization formats.
    Avro provides ~37% smaller message sizes compared to JSON.
    Configure via KAFKA__SERIALIZATION_FORMAT environment variable:
    - "json": JSON serialization (default for backward compatibility)
    - "avro": Avro binary serialization (recommended for production)

Usage:
    from src.kafka.service import AgentKafkaService

    service = AgentKafkaService(
        agent_config_path="agent_config/my_agent.yml",
        input_topic="agent-input",
        output_topic="agent-results"
    )
    await service.start()

    # Or use the convenience function
    from src.kafka.service import run_kafka_service
    await run_kafka_service()
"""

from src.kafka.consumer import KafkaConsumer
from src.kafka.producer import KafkaProducer
from src.kafka.models import AgentEnvelope, MessageType, EnvelopeStatus
from src.kafka.service import AgentKafkaService, run_kafka_service
from src.kafka.avro_serializer import AvroSerializer, get_avro_serializer

__all__ = [
    "KafkaConsumer",
    "KafkaProducer",
    "AgentEnvelope",
    "MessageType",
    "EnvelopeStatus",
    "AgentKafkaService",
    "run_kafka_service",
    "AvroSerializer",
    "get_avro_serializer",
]
