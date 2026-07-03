#!/usr/bin/env python3
"""
Send test message to Kafka for QC agent testing.

This script sends a sample chat conversation to the Kafka input topic
for the QC agent to consume and process.

Usage:
    python scripts/send_test_kafka_message.py

Environment variables required:
    KAFKA__ENABLED=true
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092
    KAFKA__INPUT_TOPIC=qc-input
    LLMS__OPENROUTER_API_KEY=sk-...
"""

import asyncio
import sys
from datetime import datetime, timezone
from pathlib import Path

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))


# Sample chat conversation (Persian customer support)
SAMPLE_CHAT = [
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
    {"role": "customer", "message": "ممنون", "timestamp": "2026-01-07T10:04:00Z"},
    {
        "role": "agent",
        "message": "خواهش می‌کنم! سوالی بود در خدمتم.",
        "timestamp": "2026-01-07T10:05:00Z",
    },
]


async def send_test_message():
    """Send a test message to Kafka."""
    from src.kafka.producer import KafkaProducer
    from src.config.settings import get_settings

    settings = get_settings()

    if not settings.kafka.enabled:
        print("❌ Kafka is not enabled. Set KAFKA__ENABLED=true in .env")
        return False

    print("📊 Kafka Settings:")
    print(f"   Bootstrap Servers: {settings.kafka.bootstrap_servers}")
    print(f"   Input Topic: {settings.kafka.input_topic}")
    print(f"   Results Topic: {settings.kafka.results_topic}")

    # Generate unique chat ID
    chat_id = f"test-{datetime.now().strftime('%Y%m%d%H%M%S')}"
    correlation_id = f"corr-{chat_id}"

    # Prepare payload
    payload = {
        "chat_id": chat_id,
        "chat_conversation": SAMPLE_CHAT,
        "source": "test_script",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print("\n📝 Sending test message:")
    print(f"   Chat ID: {chat_id}")
    print(f"   Correlation ID: {correlation_id}")
    print(f"   Messages: {len(SAMPLE_CHAT)}")

    # Create producer and send message
    producer = KafkaProducer()

    try:
        success = await producer.publish_result(
            source_agent="Test_Script",
            correlation_id=correlation_id,
            payload=payload,
            topic=settings.kafka.input_topic,
        )

        if success:
            print("\n✅ Test message sent successfully!")
            print(f"   Topic: {settings.kafka.input_topic}")
            print(f"   Correlation ID: {correlation_id}")
            print("\n💡 Now run the QC agent to process this message:")
            print("   python scripts/run_qc_agent_kafka.py --single")
        else:
            print("\n❌ Failed to send test message")

        await producer.close()
        return success

    except Exception as e:
        print(f"\n❌ Error sending message: {e}")
        await producer.close()
        return False


async def send_custom_message(chat_id: str, chat_conversation: list):
    """Send a custom message to Kafka."""
    from src.kafka.producer import KafkaProducer
    from src.config.settings import get_settings

    settings = get_settings()

    if not settings.kafka.enabled:
        print("❌ Kafka is not enabled. Set KAFKA__ENABLED=true in .env")
        return False

    correlation_id = f"corr-{chat_id}"

    payload = {
        "chat_id": chat_id,
        "chat_conversation": chat_conversation,
        "source": "test_script",
        "timestamp": datetime.now(timezone.utc).isoformat(),
    }

    print("\n📝 Sending custom message:")
    print(f"   Chat ID: {chat_id}")
    print(f"   Correlation ID: {correlation_id}")
    print(f"   Messages: {len(chat_conversation)}")

    producer = KafkaProducer()

    try:
        success = await producer.publish_result(
            source_agent="Test_Script",
            correlation_id=correlation_id,
            payload=payload,
            topic=settings.kafka.input_topic,
        )

        if success:
            print("\n✅ Message sent successfully!")
        else:
            print("\n❌ Failed to send message")

        await producer.close()
        return success

    except Exception as e:
        print(f"\n❌ Error sending message: {e}")
        await producer.close()
        return False


async def main():
    """Main entry point."""
    import argparse

    parser = argparse.ArgumentParser(
        description="Send test message to Kafka for QC agent"
    )
    parser.add_argument(
        "--chat-id", type=str, help="Custom chat ID (auto-generated if not provided)"
    )
    parser.add_argument(
        "--messages",
        type=str,
        help="JSON string of chat messages (overrides default sample)",
    )

    args = parser.parse_args()

    if args.messages:
        try:
            import json

            chat_conversation = json.loads(args.messages)
            chat_id = (
                args.chat_id or f"custom-{datetime.now().strftime('%Y%m%d%H%M%S')}"
            )
            success = await send_custom_message(chat_id, chat_conversation)
        except json.JSONDecodeError as e:
            print(f"❌ Invalid JSON messages: {e}")
            return 1
    else:
        success = await send_test_message()

    return 0 if success else 1


if __name__ == "__main__":
    import sys

    sys.exit(asyncio.run(main()))
