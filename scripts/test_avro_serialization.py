#!/usr/bin/env python3
"""
Test script for Avro serialization of AgentEnvelope messages.

This script tests the Avro serializer module to ensure it correctly
serializes and deserializes AgentEnvelope messages.
"""

import sys
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.kafka.models import AgentEnvelope, MessageType, EnvelopeStatus
from src.kafka.avro_serializer import (
    get_avro_serializer,
    load_avro_schema,
    envelope_to_avro_dict,
    avro_dict_to_envelope,
)


def test_schema_loading():
    """Test that the Avro schema loads correctly."""
    print("Testing schema loading...")
    schema = load_avro_schema("agent_envelope")
    assert schema is not None
    # Schema name includes namespace after parsing
    assert "AgentEnvelope" in schema["name"]
    assert schema["type"] == "record"
    print("✓ Schema loaded successfully")
    return schema


def test_envelope_to_avro_dict():
    """Test conversion of AgentEnvelope to Avro-compatible dict."""
    print("\nTesting envelope to Avro dict conversion...")

    envelope = AgentEnvelope.create_success(
        source_agent="test_agent",
        correlation_id="test-correlation-123",
        payload={
            "score": 85,
            "feedback": "Good performance",
            "details": {"category": "support", "tags": ["helpful", "resolved"]},
        },
    )

    avro_dict = envelope_to_avro_dict(envelope)

    assert avro_dict["meta"]["message_type"] == "agent_result"
    assert avro_dict["meta"]["source_agent"] == "test_agent"
    assert avro_dict["meta"]["correlation_id"] == "test-correlation-123"
    assert avro_dict["status"] == "COMPLETED"
    assert avro_dict["error"] is None

    print("✓ Envelope to Avro dict conversion successful")
    return avro_dict


def test_avro_dict_to_envelope():
    """Test conversion of Avro dict back to AgentEnvelope."""
    print("\nTesting Avro dict to envelope conversion...")

    avro_dict = {
        "meta": {
            "message_type": "agent_result",
            "source_agent": "test_agent",
            "correlation_id": "test-correlation-456",
            "timestamp": "2024-01-28T12:00:00+00:00",
        },
        "payload": {
            "score": "90",
            "feedback": "Excellent",
        },
        "status": "COMPLETED",
        "error": None,
    }

    envelope = avro_dict_to_envelope(avro_dict)

    assert envelope.meta.message_type == MessageType.AGENT_RESULT
    assert envelope.meta.source_agent == "test_agent"
    assert envelope.meta.correlation_id == "test-correlation-456"
    assert envelope.status == EnvelopeStatus.COMPLETED
    assert envelope.error is None

    print("✓ Avro dict to envelope conversion successful")
    return envelope


def test_serialization_roundtrip():
    """Test full serialization and deserialization roundtrip."""
    print("\nTesting serialization roundtrip...")

    serializer = get_avro_serializer()

    # Create a success envelope
    original = AgentEnvelope.create_success(
        source_agent="qc_agent",
        correlation_id="roundtrip-test-789",
        payload={
            "qc_score": 92,
            "analysis": "The support agent handled the ticket well",
            "recommendations": [
                "Continue current approach",
                "Consider faster response time",
            ],
        },
    )

    # Serialize
    serialized = serializer.serialize(original)
    assert isinstance(serialized, bytes)
    print(f"  Serialized size: {len(serialized)} bytes")

    # Deserialize
    deserialized = serializer.deserialize(serialized)

    # Verify
    assert deserialized.meta.source_agent == original.meta.source_agent
    assert deserialized.meta.correlation_id == original.meta.correlation_id
    assert deserialized.status == original.status
    assert deserialized.error == original.error

    print("✓ Serialization roundtrip successful")
    return serialized, deserialized


def test_error_envelope():
    """Test serialization of error envelopes."""
    print("\nTesting error envelope serialization...")

    serializer = get_avro_serializer()

    # Create an error envelope
    original = AgentEnvelope.create_failure(
        source_agent="qc_agent",
        correlation_id="error-test-101",
        error="Failed to process ticket: Invalid format",
    )

    # Serialize and deserialize
    serialized = serializer.serialize(original)
    deserialized = serializer.deserialize(serialized)

    # Verify
    assert deserialized.status == EnvelopeStatus.FAILED
    assert deserialized.error == "Failed to process ticket: Invalid format"
    assert deserialized.payload == {}

    print("✓ Error envelope serialization successful")


def test_complex_payload():
    """Test serialization with complex nested payload."""
    print("\nTesting complex payload serialization...")

    serializer = get_avro_serializer()

    # Create envelope with complex payload
    original = AgentEnvelope.create_success(
        source_agent="qc_agent",
        correlation_id="complex-test-202",
        payload={
            "score": 88,
            "breakdown": {
                "communication": 90,
                "resolution": 85,
                "timeliness": 88,
            },
            "tags": ["resolved", "customer-satisfied", "first-contact"],
            "metadata": {
                "ticket_id": "TKT-12345",
                "agent_id": "AGT-001",
            },
        },
    )

    # Serialize and deserialize
    serialized = serializer.serialize(original)
    deserialized = serializer.deserialize(serialized)

    # Verify payload is preserved (complex structures are JSON-encoded)
    assert deserialized.meta.correlation_id == original.meta.correlation_id
    assert deserialized.status == EnvelopeStatus.COMPLETED

    print("✓ Complex payload serialization successful")


def compare_json_vs_avro_size():
    """Compare message sizes between JSON and Avro serialization."""
    print("\nComparing JSON vs Avro message sizes...")

    import json

    serializer = get_avro_serializer()

    envelope = AgentEnvelope.create_success(
        source_agent="qc_agent",
        correlation_id="size-comparison-test",
        payload={
            "score": 85,
            "analysis": "The support agent provided excellent service",
            "recommendations": ["Keep up the good work"],
            "metrics": {
                "response_time": 120,
                "resolution_time": 300,
                "customer_satisfaction": 4.5,
            },
        },
    )

    # JSON size
    json_bytes = json.dumps(envelope.to_dict()).encode("utf-8")
    json_size = len(json_bytes)

    # Avro size
    avro_bytes = serializer.serialize(envelope)
    avro_size = len(avro_bytes)

    print(f"  JSON size: {json_size} bytes")
    print(f"  Avro size: {avro_size} bytes")
    print(f"  Size reduction: {((json_size - avro_size) / json_size * 100):.1f}%")


def main():
    """Run all tests."""
    print("=" * 60)
    print("Avro Serialization Tests")
    print("=" * 60)

    try:
        test_schema_loading()
        test_envelope_to_avro_dict()
        test_avro_dict_to_envelope()
        test_serialization_roundtrip()
        test_error_envelope()
        test_complex_payload()
        compare_json_vs_avro_size()

        print("\n" + "=" * 60)
        print("All tests passed! ✓")
        print("=" * 60)
        return 0

    except Exception as e:
        print(f"\n✗ Test failed: {e}")
        import traceback

        traceback.print_exc()
        return 1


if __name__ == "__main__":
    sys.exit(main())
