#!/usr/bin/env python3
"""
Test script for production Kafka deployment with Avro serialization.

This script sends test messages to the production Kafka cluster and monitors
the results topic for responses.

Usage:
    # Send a test message
    python scripts/test_production_kafka.py send

    # Monitor results topic
    python scripts/test_production_kafka.py monitor

    # Send and monitor
    python scripts/test_production_kafka.py send-and-monitor

Environment variables required:
    KAFKA__ENABLED=true
    KAFKA__BOOTSTRAP_SERVERS=kafka-kafka-bootstrap.kafka.svc.cluster.local:9092
    KAFKA__INPUT_TOPIC=ai.ticket-qc-input
    KAFKA__RESULTS_TOPIC=ai.ticket-qc-results
    KAFKA__SERIALIZATION_FORMAT=avro
"""

import asyncio
import json
import logging
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

# Load production env before importing settings
from dotenv import load_dotenv

load_dotenv(project_root / ".env.production", override=True)

from aiokafka import AIOKafkaConsumer

from src.kafka.producer import KafkaProducer
from src.kafka.models import AgentEnvelope
from src.kafka.avro_serializer import get_avro_serializer
from src.config.settings import get_settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Sample chat conversations for testing
SAMPLE_CHATS = {
    "good_support": [
        {
            "role": "customer",
            "message": "سلام، من ۲ روز پیش درخواست برداشت دادم ولی هنوز واریز نشده",
            "timestamp": "2026-01-07T10:00:00Z",
        },
        {
            "role": "agent",
            "message": "سلام! متاسفم برای تاخیر. بذارید بررسی کنم. شماره تراکنش رو دارید؟",
            "timestamp": "2026-01-07T10:01:00Z",
        },
        {
            "role": "customer",
            "message": "بله، TX12345",
            "timestamp": "2026-01-07T10:02:00Z",
        },
        {
            "role": "agent",
            "message": "پیدا کردم. در صف پردازشه و تا ۲۴ ساعت واریز میشه. بابت تاخیر عذرخواهی می‌کنم و ۵ USDT جبران اضافه کردم.",
            "timestamp": "2026-01-07T10:03:00Z",
        },
        {
            "role": "customer",
            "message": "ممنون",
            "timestamp": "2026-01-07T10:04:00Z"
        },
        {
            "role": "agent",
            "message": "خواهش می‌کنم! سوالی بود در خدمتم.",
            "timestamp": "2026-01-07T10:05:00Z",
        },
    ],
    "poor_support": [
        {
            "role": "customer",
            "message": "حساب من بلاک شده چرا؟",
            "timestamp": "2026-01-07T11:00:00Z",
        },
        {
            "role": "agent",
            "message": "نمیدونم",
            "timestamp": "2026-01-07T11:05:00Z",
        },
        {
            "role": "customer",
            "message": "چطور میتونم باز کنم؟",
            "timestamp": "2026-01-07T11:06:00Z",
        },
        {
            "role": "agent",
            "message": "باید صبر کنید",
            "timestamp": "2026-01-07T11:10:00Z",
        },
    ],
    "technical_issue": [
        {
            "role": "customer",
            "message": "سلام، نمیتونم وارد حسابم بشم. ارور میده",
            "timestamp": "2026-01-07T12:00:00Z",
        },
        {
            "role": "agent",
            "message": "سلام! چه ارور خاصی میده؟ میتونید اسکرین‌شات بفرستید؟",
            "timestamp": "2026-01-07T12:01:00Z",
        },
        {
            "role": "customer",
            "message": "میگه 'Authentication failed'. رمز عبورم رو درست وارد میکنم",
            "timestamp": "2026-01-07T12:03:00Z",
        },
        {
            "role": "agent",
            "message": "احتمالا حساب شما قفل شده. لطفا ایمیل تایید هویت رو چک کنید و مراحل رو طی کنید. اگر مشکل حل نشد، تیکت فنی بزنید.",
            "timestamp": "2026-01-07T12:05:00Z",
        },
        {
            "role": "customer",
            "message": "باشه ممنون",
            "timestamp": "2026-01-07T12:06:00Z",
        },
    ],
}


async def send_test_message(chat_type: str = "good_support") -> Optional[str]:
    """
    Send a test message to the production Kafka input topic.

    Uses KafkaProducer.publish_result() to send to the input topic,
    simulating an external system triggering the QC agent.
    """
    settings = get_settings()

    if not settings.kafka.enabled:
        logger.error("Kafka is not enabled. Set KAFKA__ENABLED=true")
        return None

    logger.info("Production Kafka Settings:")
    logger.info(f"   Bootstrap Servers: {settings.kafka.bootstrap_servers}")
    logger.info(f"   Input Topic: {settings.kafka.input_topic}")
    logger.info(f"   Results Topic: {settings.kafka.results_topic}")
    logger.info(f"   Serialization: {settings.kafka.serialization_format}")

    if chat_type not in SAMPLE_CHATS:
        logger.error(f"Unknown chat type: {chat_type}")
        logger.info(f"   Available types: {', '.join(SAMPLE_CHATS.keys())}")
        return None

    chat_conversation = SAMPLE_CHATS[chat_type]

    timestamp_str = datetime.now().strftime('%Y%m%d%H%M%S')
    chat_id = f"prod-test-{chat_type}-{timestamp_str}"
    correlation_id = f"corr-{chat_id}"

    payload = {
        "chat_id": chat_id,
        "chat_conversation": chat_conversation,
        "source": "production_test_script",
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "test_type": chat_type,
    }

    logger.info(f"Sending test message:")
    logger.info(f"   Chat Type: {chat_type}")
    logger.info(f"   Chat ID: {chat_id}")
    logger.info(f"   Correlation ID: {correlation_id}")
    logger.info(f"   Messages: {len(chat_conversation)}")

    producer = KafkaProducer()

    try:
        success = await producer.publish_result(
            source_agent="ProductionTestScript",
            correlation_id=correlation_id,
            payload=payload,
            topic=settings.kafka.input_topic,
        )

        if success:
            logger.info(f"Test message sent successfully!")
            logger.info(f"   Topic: {settings.kafka.input_topic}")
            logger.info(f"   Correlation ID: {correlation_id}")
            return correlation_id
        else:
            logger.error("Failed to send test message")
            return None

    except Exception as e:
        logger.error(f"Error sending message: {e}", exc_info=True)
        return None
    finally:
        await producer.close()


def _get_deserializer(settings):
    """Build a value deserializer matching the configured serialization format."""
    if settings.kafka.serialization_format == "avro":
        serializer = get_avro_serializer()

        def avro_deserializer(data: bytes) -> AgentEnvelope:
            try:
                return serializer.deserialize(data)
            except Exception:
                return AgentEnvelope.from_dict(json.loads(data.decode("utf-8")))

        return avro_deserializer
    else:
        return lambda data: AgentEnvelope.from_dict(json.loads(data.decode("utf-8")))


async def monitor_results(correlation_id: Optional[str] = None, timeout: int = 300):
    """
    Monitor the results topic for responses.

    Uses a raw AIOKafkaConsumer pointed at the results topic
    (KafkaConsumer is hardcoded to the input topic).
    """
    settings = get_settings()

    if not settings.kafka.enabled:
        logger.error("Kafka is not enabled. Set KAFKA__ENABLED=true")
        return

    results_topic = settings.kafka.results_topic
    logger.info(f"Monitoring results topic: {results_topic}")
    if correlation_id:
        logger.info(f"   Filtering for correlation ID: {correlation_id}")
    logger.info(f"   Timeout: {timeout}s")
    logger.info("   Press Ctrl+C to stop\n")

    # Use a unique group ID so we always read from latest
    group_id = f"test-monitor-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    deserializer = _get_deserializer(settings)

    consumer = AIOKafkaConsumer(
        results_topic,
        bootstrap_servers=settings.kafka.bootstrap_servers,
        group_id=group_id,
        auto_offset_reset="latest",
        value_deserializer=deserializer,
        enable_auto_commit=True,
    )

    message_count = 0
    start_time = datetime.now()

    try:
        await consumer.start()
        logger.info(f"Consumer started, listening on '{results_topic}'...")

        async def _consume():
            nonlocal message_count

            async for msg in consumer:
                envelope: AgentEnvelope = msg.value

                msg_corr_id = envelope.meta.correlation_id
                if correlation_id and msg_corr_id != correlation_id:
                    continue

                message_count += 1

                print("\n" + "=" * 80)
                print(f"RESULT MESSAGE #{message_count}")
                print("=" * 80)
                print(f"Correlation ID: {msg_corr_id}")
                print(f"Source Agent:   {envelope.meta.source_agent}")
                print(f"Message Type:   {envelope.meta.message_type.value}")
                print(f"Status:         {envelope.status.value}")
                print(f"Timestamp:      {envelope.meta.timestamp}")

                if envelope.error:
                    print(f"\nError: {envelope.error}")

                print("\n" + "-" * 40)
                print("Payload:")
                print(json.dumps(envelope.payload, indent=2, ensure_ascii=False, default=str))
                print("=" * 80 + "\n")

                if correlation_id and msg_corr_id == correlation_id:
                    logger.info("Received expected message, stopping monitor...")
                    return

        await asyncio.wait_for(_consume(), timeout=timeout)

    except asyncio.TimeoutError:
        elapsed = (datetime.now() - start_time).total_seconds()
        logger.warning(f"Timeout reached after {elapsed:.1f}s")
        logger.info(f"   Messages received: {message_count}")
    except KeyboardInterrupt:
        logger.info("Monitoring stopped by user")
        logger.info(f"   Messages received: {message_count}")
    finally:
        await consumer.stop()


async def send_and_monitor(chat_type: str = "good_support", timeout: int = 300):
    """Send a test message and monitor for the result."""
    correlation_id = await send_test_message(chat_type)

    if not correlation_id:
        logger.error("Failed to send message, aborting monitor")
        return

    logger.info("\nWaiting 5 seconds before starting monitor...")
    await asyncio.sleep(5)

    await monitor_results(correlation_id=correlation_id, timeout=timeout)


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test production Kafka deployment with Avro serialization"
    )
    parser.add_argument(
        "action",
        choices=["send", "monitor", "send-and-monitor"],
        help="Action to perform",
    )
    parser.add_argument(
        "--chat-type",
        type=str,
        default="good_support",
        choices=list(SAMPLE_CHATS.keys()),
        help="Type of chat conversation to send",
    )
    parser.add_argument(
        "--correlation-id",
        type=str,
        help="Specific correlation ID to monitor (for 'monitor' action)",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=300,
        help="Timeout in seconds for monitoring (default: 300)",
    )

    args = parser.parse_args()

    if args.action == "send":
        await send_test_message(args.chat_type)
    elif args.action == "monitor":
        await monitor_results(
            correlation_id=args.correlation_id,
            timeout=args.timeout,
        )
    elif args.action == "send-and-monitor":
        await send_and_monitor(
            chat_type=args.chat_type,
            timeout=args.timeout,
        )


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logger.info("\nGoodbye!")
        sys.exit(0)
