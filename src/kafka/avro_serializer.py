"""
Avro serializer for Kafka messages.

Uses Confluent Schema Registry for wire-format serialization (magic byte + schema ID),
enabling Kafka UI and standard tooling to auto-deserialize messages.
Falls back to fastavro schemaless encoding when Schema Registry is unavailable.
"""

import io
import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional, Callable

import fastavro

from .models import AgentEnvelope, EnvelopeMeta, MessageType, EnvelopeStatus, QCTicketResultEvent

logger = logging.getLogger(__name__)

# Schema cache
_schema_cache: Dict[str, dict] = {}
_schema_str_cache: Dict[str, str] = {}


def get_schema_path() -> Path:
    """Get the path to the Avro schema directory."""
    return Path(__file__).parent / "schemas"


def load_avro_schema(schema_name: str = "agent_envelope") -> dict:
    """
    Load an Avro schema from file.

    Args:
        schema_name: Name of the schema file (without .avsc extension)

    Returns:
        Parsed Avro schema as dictionary

    Raises:
        FileNotFoundError: If schema file doesn't exist
        ValueError: If schema is invalid
    """
    if schema_name in _schema_cache:
        return _schema_cache[schema_name]

    schema_path = get_schema_path() / f"{schema_name}.avsc"

    if not schema_path.exists():
        raise FileNotFoundError(f"Avro schema not found: {schema_path}")

    try:
        # Load and parse the schema
        schema: dict = fastavro.schema.load_schema(str(schema_path))  # type: ignore[assignment]
        _schema_cache[schema_name] = schema
        logger.debug(f"Loaded Avro schema: {schema_name}")
        return schema
    except Exception as e:
        raise ValueError(f"Invalid Avro schema {schema_name}: {e}")


def load_avro_schema_str(schema_name: str = "agent_envelope") -> str:
    """Load an Avro schema as a JSON string for Confluent serializers."""
    if schema_name in _schema_str_cache:
        return _schema_str_cache[schema_name]

    schema_path = get_schema_path() / f"{schema_name}.avsc"
    if not schema_path.exists():
        raise FileNotFoundError(f"Avro schema not found: {schema_path}")

    schema_str = schema_path.read_text()
    _schema_str_cache[schema_name] = schema_str
    return schema_str



def envelope_to_avro_dict(envelope: AgentEnvelope) -> Dict[str, Any]:
    """
    Convert AgentEnvelope to Avro-compatible dictionary.

    Args:
        envelope: AgentEnvelope instance

    Returns:
        Dictionary suitable for Avro serialization
    """
    return {
        "meta": {
            "message_type": envelope.meta.message_type.value,
            "source_agent": envelope.meta.source_agent,
            "correlation_id": envelope.meta.correlation_id,
            "timestamp": envelope.meta.timestamp,
        },
        "payload": json.dumps(envelope.payload),
        "status": envelope.status.value,
        "error": envelope.error,
    }


def avro_dict_to_envelope(data: Dict[str, Any]) -> AgentEnvelope:
    """
    Convert Avro-deserialized dictionary to AgentEnvelope.

    Args:
        data: Dictionary from Avro deserialization

    Returns:
        AgentEnvelope instance
    """
    meta = EnvelopeMeta(
        message_type=MessageType(data["meta"]["message_type"]),
        source_agent=data["meta"]["source_agent"],
        correlation_id=data["meta"]["correlation_id"],
        timestamp=data["meta"]["timestamp"],
    )

    payload_raw = data.get("payload", "{}")
    if isinstance(payload_raw, str):
        try:
            payload = json.loads(payload_raw)
        except (json.JSONDecodeError, TypeError):
            payload = {"raw": payload_raw}
    else:
        payload = payload_raw

    return AgentEnvelope(
        meta=meta,
        payload=payload if isinstance(payload, dict) else {"raw": payload},
        status=EnvelopeStatus(data["status"]),
        error=data.get("error"),
    )


def _try_import_confluent():
    """Lazily import Confluent Schema Registry modules. Returns None tuple on failure."""
    try:
        from confluent_kafka.schema_registry import SchemaRegistryClient
        from confluent_kafka.schema_registry.avro import (
            AvroSerializer as CSerializer,
            AvroDeserializer as CDeserializer,
        )
        from confluent_kafka.serialization import SerializationContext, MessageField
        return SchemaRegistryClient, CSerializer, CDeserializer, SerializationContext, MessageField
    except (ImportError, ModuleNotFoundError) as e:
        logger.warning(f"Confluent Schema Registry not available, will use fastavro fallback: {e}")
        return None, None, None, None, None


class AvroSerializer:
    """
    Avro serializer with Confluent Schema Registry support.

    Tries Confluent wire format (magic byte + schema ID) first for Kafka UI
    compatibility. Falls back to fastavro schemaless encoding when Schema
    Registry is unavailable.
    """

    def __init__(self, schema_name: str = "agent_envelope"):
        self.schema_name = schema_name
        self._schema: Optional[dict] = None
        self._qc_result_schema: Optional[dict] = None
        self._registry_client = None
        self._confluent_serializers: Dict[str, Any] = {}
        self._confluent_deserializers: Dict[str, Any] = {}
        self._confluent_available: Optional[bool] = None
        self._confluent_modules: Optional[tuple] = None

    def _ensure_confluent(self) -> bool:
        """Check if Confluent modules are importable and cache the result."""
        if self._confluent_available is None:
            modules = _try_import_confluent()
            if modules[0] is None:
                self._confluent_available = False
            else:
                self._confluent_available = True
                self._confluent_modules = modules
        return self._confluent_available

    def _get_registry_client(self):
        """Get or create the Schema Registry client."""
        if self._registry_client is None:
            from src.config.settings import get_settings

            SchemaRegistryClient = self._confluent_modules[0]
            settings = get_settings()
            conf = {"url": settings.kafka.schema_registry_url}
            if settings.kafka.schema_registry_username and settings.kafka.schema_registry_password:
                conf["basic.auth.user.info"] = (
                    f"{settings.kafka.schema_registry_username}:{settings.kafka.schema_registry_password}"
                )
            self._registry_client = SchemaRegistryClient(conf)
            logger.info(f"Schema Registry client created for {settings.kafka.schema_registry_url}")
        return self._registry_client

    def _get_confluent_serializer(self, schema_name: str):
        """Get or create a Confluent AvroSerializer for the given schema."""
        if schema_name not in self._confluent_serializers:
            CSerializer = self._confluent_modules[1]
            schema_str = load_avro_schema_str(schema_name)
            client = self._get_registry_client()
            self._confluent_serializers[schema_name] = CSerializer(
                client,
                schema_str,
                conf={"auto.register.schemas": True},
            )
            logger.debug(f"Created Confluent AvroSerializer for schema: {schema_name}")
        return self._confluent_serializers[schema_name]

    def _get_confluent_deserializer(self, schema_name: str):
        """Get or create a Confluent AvroDeserializer for the given schema."""
        if schema_name not in self._confluent_deserializers:
            CDeserializer = self._confluent_modules[2]
            schema_str = load_avro_schema_str(schema_name)
            client = self._get_registry_client()
            self._confluent_deserializers[schema_name] = CDeserializer(
                client,
                schema_str,
            )
            logger.debug(f"Created Confluent AvroDeserializer for schema: {schema_name}")
        return self._confluent_deserializers[schema_name]

    def _fastavro_serialize(self, schema: dict, avro_dict: Dict[str, Any]) -> bytes:
        """Fallback: serialize using fastavro schemaless writer."""
        buffer = io.BytesIO()
        fastavro.schemaless_writer(buffer, schema, avro_dict)
        return buffer.getvalue()

    def _fastavro_deserialize(self, schema: dict, data: bytes) -> Dict[str, Any]:
        """Fallback: deserialize using fastavro schemaless reader."""
        buffer = io.BytesIO(data)
        return fastavro.schemaless_reader(buffer, schema)  # type: ignore[return-value]

    @property
    def schema(self) -> dict:
        """Get the Avro schema, loading it if necessary."""
        if self._schema is None:
            self._schema = load_avro_schema(self.schema_name)
        return self._schema

    @property
    def qc_result_schema(self) -> dict:
        """Get the QCTicketResultEvent Avro schema, loading it if necessary."""
        if self._qc_result_schema is None:
            self._qc_result_schema = load_avro_schema("qc_ticket_result")
        return self._qc_result_schema

    def serialize_qc_result(self, event: QCTicketResultEvent, topic: str) -> bytes:
        """
        Serialize a QCTicketResultEvent. Uses Confluent wire format when Schema
        Registry is available, otherwise falls back to fastavro schemaless.

        Args:
            event: QCTicketResultEvent instance to serialize
            topic: Kafka topic name (required for Confluent SerializationContext)

        Returns:
            Avro-encoded bytes
        """
        avro_dict = event.to_avro_dict()

        if self._ensure_confluent():
            try:
                SerializationContext = self._confluent_modules[3]
                MessageField = self._confluent_modules[4]
                serializer = self._get_confluent_serializer("qc_ticket_result")
                ctx = SerializationContext(topic, MessageField.VALUE)
                return serializer(avro_dict, ctx)
            except Exception as e:
                logger.warning(f"Confluent serialization failed, falling back to fastavro: {e}")

        try:
            return self._fastavro_serialize(self.qc_result_schema, avro_dict)
        except Exception as e:
            logger.error(f"Failed to serialize QCTicketResultEvent: {e}")
            raise ValueError(f"Avro serialization failed: {e}")

    def serialize(self, envelope: AgentEnvelope, topic: str) -> bytes:
        """
        Serialize an AgentEnvelope. Uses Confluent wire format when Schema
        Registry is available, otherwise falls back to fastavro schemaless.

        Args:
            envelope: AgentEnvelope instance to serialize
            topic: Kafka topic name (required for Confluent SerializationContext)

        Returns:
            Avro-encoded bytes

        Raises:
            ValueError: If serialization fails
        """
        avro_dict = envelope_to_avro_dict(envelope)

        if self._ensure_confluent():
            try:
                SerializationContext = self._confluent_modules[3]
                MessageField = self._confluent_modules[4]
                serializer = self._get_confluent_serializer(self.schema_name)
                ctx = SerializationContext(topic, MessageField.VALUE)
                return serializer(avro_dict, ctx)
            except Exception as e:
                logger.warning(f"Confluent serialization failed, falling back to fastavro: {e}")

        try:
            return self._fastavro_serialize(self.schema, avro_dict)
        except Exception as e:
            logger.error(f"Failed to serialize envelope: {e}")
            raise ValueError(f"Avro serialization failed: {e}")

    def deserialize(self, data: bytes, topic: str) -> AgentEnvelope:
        """
        Deserialize data to AgentEnvelope. Tries Confluent wire format first,
        falls back to fastavro schemaless.

        Args:
            data: Avro-encoded bytes (Confluent wire format or raw schemaless)
            topic: Kafka topic name (required for Confluent SerializationContext)

        Returns:
            AgentEnvelope instance

        Raises:
            ValueError: If deserialization fails
        """
        if self._ensure_confluent():
            try:
                SerializationContext = self._confluent_modules[3]
                MessageField = self._confluent_modules[4]
                deserializer = self._get_confluent_deserializer(self.schema_name)
                ctx = SerializationContext(topic, MessageField.VALUE)
                avro_dict: Dict[str, Any] = deserializer(data, ctx)
                return avro_dict_to_envelope(avro_dict)
            except Exception as e:
                logger.warning(f"Confluent deserialization failed, falling back to fastavro: {e}")

        try:
            avro_dict = self._fastavro_deserialize(self.schema, data)
            return avro_dict_to_envelope(avro_dict)
        except Exception as e:
            logger.error(f"Failed to deserialize envelope: {e}")
            raise ValueError(f"Avro deserialization failed: {e}")

    def deserialize_input(self, data: bytes, topic: str) -> Dict[str, Any]:
        """
        Deserialize data to a plain dictionary. Tries Confluent wire format first,
        falls back to fastavro schemaless.

        Used for input messages (QCTicketInputEvent) that don't use AgentEnvelope.

        Args:
            data: Avro-encoded bytes (Confluent wire format or raw schemaless)
            topic: Kafka topic name (required for Confluent SerializationContext)

        Returns:
            Deserialized dictionary
        """
        if self._ensure_confluent():
            try:
                SerializationContext = self._confluent_modules[3]
                MessageField = self._confluent_modules[4]
                deserializer = self._get_confluent_deserializer("qc_ticket_input")
                ctx = SerializationContext(topic, MessageField.VALUE)
                return deserializer(data, ctx)
            except Exception as e:
                logger.warning(f"Confluent deserialization failed, falling back to fastavro: {e}")

        try:
            schema = load_avro_schema("qc_ticket_input")
            return self._fastavro_deserialize(schema, data)
        except Exception as e:
            logger.error(f"Failed to deserialize input message: {e}")
            raise ValueError(f"Avro deserialization failed: {e}")

    def get_serializer(self) -> Callable[[Dict[str, Any]], bytes]:
        """
        Get a serializer function for use with aiokafka producer.

        Returns:
            Serializer function that takes a dict and returns bytes
        """

        def serializer(value: Dict[str, Any]) -> bytes:
            # If value is already an AgentEnvelope, serialize directly
            if isinstance(value, AgentEnvelope):
                return self.serialize(value, "")

            # Otherwise, create envelope from dict and serialize
            envelope = AgentEnvelope.from_dict(value)
            return self.serialize(envelope, "")

        return serializer

    def get_deserializer(self) -> Callable[[bytes], Dict[str, Any]]:
        """
        Get a deserializer function for use with aiokafka consumer.

        Returns:
            Deserializer function that takes bytes and returns a dict
        """

        def deserializer(data: bytes) -> Dict[str, Any]:
            envelope = self.deserialize(data, "")
            return envelope.to_dict()

        return deserializer


# Global serializer instance
_serializer_instance: Optional[AvroSerializer] = None


def get_avro_serializer() -> AvroSerializer:
    """
    Get the global Avro serializer instance.

    Returns:
        AvroSerializer instance
    """
    global _serializer_instance
    if _serializer_instance is None:
        _serializer_instance = AvroSerializer()
    return _serializer_instance


__all__ = [
    "AvroSerializer",
    "get_avro_serializer",
    "load_avro_schema",
    "load_avro_schema_str",
    "envelope_to_avro_dict",
    "avro_dict_to_envelope",
]
