#!/usr/bin/env python3
"""
Script to read QC input messages from Kafka topic and print them.

Usage:
    python scripts/read_kafka_qc_input.py

Environment variables:
    KAFKA__ENABLED=true
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092
    KAFKA__INPUT_TOPIC=qc-input
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.kafka.consumer import KafkaConsumer
from src.config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


async def print_message(message: dict):
    """Print the Kafka message in a readable format."""
    print("\n" + "=" * 80)
    print("NEW MESSAGE RECEIVED")
    print("=" * 80)
    print(f"Chat ID: {message.get('chat_id', 'N/A')}")
    print(f"Source: {message.get('source', 'N/A')}")
    print(f"Event: {message.get('event_name', 'N/A')}")
    print(f"Timestamp: {message.get('time_stamp', 'N/A')}")
    print("-" * 40)
    print("Conversation:")
    print(json.dumps(message.get("chat_conversation", []), indent=2, default=str))
    print("=" * 80 + "\n")


async def handle_error(correlation_id: str, error: Exception):
    """Handle errors during message processing."""
    logger.error(f"Error processing message {correlation_id}: {error}")


async def main():
    """Main function to consume and print Kafka messages."""
    # Load settings
    settings = get_settings()

    # Check if Kafka is enabled
    if not settings.kafka.enabled:
        logger.error(
            "Kafka is not enabled. Set KAFKA__ENABLED=true in environment variables."
        )
        sys.exit(1)

    logger.info(f"Connecting to Kafka at {settings.kafka.bootstrap_servers}")
    logger.info(f"Subscribing to topic: {settings.kafka.input_topic}")

    # Create consumer
    consumer = KafkaConsumer()

    try:
        # Start consuming and processing messages
        await consumer.consume_and_process(
            process_func=print_message, error_func=handle_error
        )
    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, stopping consumer...")
    finally:
        await consumer.stop()
        logger.info("Consumer stopped.")


if __name__ == "__main__":
    asyncio.run(main())
