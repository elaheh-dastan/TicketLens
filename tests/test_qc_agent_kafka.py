"""
QC Agent Test Script with Kafka Publishing

This script tests the QC agent through:
1. Direct Kafka producer test
2. API server test
3. QCAgentKafkaService test

Usage:
    # First, ensure Kafka is running and .env has correct settings
    # Then run the API server: uvicorn src.api.server:app --reload

    # In another terminal, run this test:
    python test_qc_agent_kafka.py

    # Test specific components:
    python test_qc_agent_kafka.py --direct    # Test Kafka producer
    python test_qc_agent_kafka.py --api       # Test through API
    python test_qc_agent_kafka.py --service   # Test QCAgentKafkaService

Requirements in .env:
    KAFKA__ENABLED=true
    KAFKA__BOOTSTRAP_SERVERS=localhost:9092
    KAFKA__RESULTS_TOPIC=qc-results
"""

import asyncio
import json
import httpx
from datetime import datetime, timezone

# Sample chat conversation (Persian customer support)
SAMPLE_CHAT = [
    {
        "role": "end_user",
        "message": "سلام، من ۲ روز پیش درخواست برداشت دادم ولی هنوز واریز نشده",
        "timestamp": "2026-01-07T10:00:00Z",
    },
    {
        "role": "support",
        "message": "سلام! متاسفم برای تاخیر. بذارید بررسی کنم. شماره تراکنش رو دارید؟",
        "timestamp": "2026-01-07T10:01:00Z",
    },
    {
        "role": "end_user",
        "message": "بله، TX12345",
        "timestamp": "2026-01-07T10:02:00Z",
    },
    {
        "role": "support",
        "message": "پیدا کردم. در صف پردازشه و تا ۲۴ ساعت واریز میشه. بابت تاخیر عذرخواهی می‌کنم و ۵ USDT جبران اضافه کردم.",
        "timestamp": "2026-01-07T10:03:00Z",
    },
    {"role": "end_user", "message": "ممنون", "timestamp": "2026-01-07T10:04:00Z"},
    {
        "role": "support",
        "message": "خواهش می‌کنم! سوالی بود در خدمتم.",
        "timestamp": "2026-01-07T10:05:00Z",
    },
]

API_BASE_URL = "http://localhost:8000"


async def test_qc_agent_via_api():
    """Test QC agent through API server with Kafka publishing."""
    print("=" * 60)
    print("QC AGENT API TEST WITH KAFKA")
    print("=" * 60)

    chat_id = f"test-{datetime.now().strftime('%Y%m%d%H%M%S')}"

    # Prepare request payload
    payload = {"chat_id": chat_id, "chat_conversation": SAMPLE_CHAT}

    print(f"\n📝 Chat ID: {chat_id}")
    print(f"💬 Messages: {len(SAMPLE_CHAT)}")

    async with httpx.AsyncClient(timeout=30.0) as client:
        # Step 1: Submit QC evaluation task
        print("\n🚀 Submitting QC evaluation task...")
        try:
            response = await client.post(
                f"{API_BASE_URL}/api/v2/qc/evaluate", json=payload
            )

            if response.status_code == 202:
                result = response.json()
                task_id = result["task_id"]
                kafka_topic = result.get("kafka_topic", "qc-results")

                print("✅ Task submitted successfully!")
                print(f"   Task ID: {task_id}")
                print(f"   Kafka Topic: {kafka_topic}")

                # Step 2: Poll for task completion
                print("\n⏳ Waiting for task completion...")
                max_wait = 120  # 2 minutes
                wait_interval = 5  # 5 seconds
                waited = 0

                while waited < max_wait:
                    await asyncio.sleep(wait_interval)
                    waited += wait_interval

                    status_response = await client.get(
                        f"{API_BASE_URL}/tasks/{task_id}"
                    )

                    if status_response.status_code == 200:
                        status_data = status_response.json()
                        status = status_data["status"]
                        print(f"   Status: {status}")

                        if status == "completed":
                            print("\n✅ Task completed!")

                            # Print results
                            print("\n" + "=" * 60)
                            print("QC AGENT RESULTS")
                            print("=" * 60)

                            result_data = status_data.get("result", {})

                            if result_data.get("formatted_result"):
                                print("\n📋 Formatted Result:")
                                print(
                                    json.dumps(
                                        result_data["formatted_result"],
                                        indent=2,
                                        ensure_ascii=False,
                                    )
                                )

                            if result_data.get("qc_evaluation"):
                                print("\n🎯 QC Evaluation:")
                                for key, value in result_data["qc_evaluation"].items():
                                    print(f"   {key}: {value}")

                            print("\n" + "=" * 60)
                            print("✅ Test completed successfully!")
                            print(
                                f"📊 Results should be published to Kafka topic: {kafka_topic}"
                            )
                            print("=" * 60)
                            return True

                        elif status == "failed":
                            error = status_data.get("error", "Unknown error")
                            print(f"\n❌ Task failed: {error}")
                            return False

                    else:
                        print(f"   Status check failed: {status_response.status_code}")

                print(f"\n⏰ Task timed out after {max_wait} seconds")
                return False

            else:
                print(f"❌ Failed to submit task: {response.status_code}")
                print(f"   Response: {response.text}")
                return False

        except httpx.RequestError as e:
            print(f"\n❌ Request failed: {e}")
            print("   Make sure the API server is running!")
            print("   Run: uvicorn src.api.server:app --reload")
            return False


async def test_kafka_directly():
    """Test Kafka publishing directly without running the agent."""
    print("\n" + "=" * 60)
    print("DIRECT KAFKA TEST")
    print("=" * 60)

    try:
        from src.kafka.producer import KafkaProducer
        from src.config.settings import get_settings

        settings = get_settings()

        if not settings.kafka.enabled:
            print("\n❌ Kafka is not enabled in settings!")
            print("   Add to .env: KAFKA__ENABLED=true")
            return False

        print("\n📊 Kafka Settings:")
        print(f"   Bootstrap Servers: {settings.kafka.bootstrap_servers}")
        print(f"   Results Topic: {settings.kafka.results_topic}")
        print(f"   Publish Results: {settings.kafka.publish_results}")

        # Create producer and test publish
        producer = KafkaProducer()

        test_payload = {
            "test": True,
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "message": "Direct Kafka test from QC agent",
        }

        print("\n🚀 Publishing test message to Kafka...")
        success = await producer.publish_result(
            source_agent="QC_Agent_Test",
            correlation_id="test-direct-001",
            payload=test_payload,
            topic=settings.kafka.results_topic,
        )

        if success:
            print("✅ Test message published successfully!")
        else:
            print("❌ Failed to publish test message")

        await producer.close()
        return success

    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        print("   Make sure aiokafka is installed: pip install aiokafka")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        return False


async def test_qc_kafka_service():
    """Test the QCAgentKafkaService directly."""
    print("\n" + "=" * 60)
    print("QC AGENT KAFKA SERVICE TEST")
    print("=" * 60)

    try:
        from src.kafka.qc_service import QCAgentKafkaService
        from src.config.settings import get_settings

        settings = get_settings()

        if not settings.kafka.enabled:
            print("\n❌ Kafka is not enabled in settings!")
            print("   Add to .env: KAFKA__ENABLED=true")
            return False

        print("\n📊 Testing QCAgentKafkaService:")
        print("   Agent config: agent_config/qc_agent_dspy.yml")
        print(f"   Input topic: {settings.kafka.input_topic}")
        print(f"   Output topic: {settings.kafka.results_topic}")

        # Create service
        service = QCAgentKafkaService(
            agent_config_path="agent_config/qc_agent_dspy.yml",
        )

        # Initialize (but don't start consuming)
        print("\n🚀 Initializing service...")
        await service.initialize()
        print("✅ Service initialized successfully!")

        # Test processing a message directly
        print("\n📝 Testing message processing...")
        test_message = {
            "payload": {
                "chat_id": "test-service-001",
                "chat_conversation": SAMPLE_CHAT,
            },
            "correlation_id": "test-correlation-001",
        }

        # Process the message (this will invoke the agent)
        await service._process_message(test_message)

        # Get stats
        stats = service.get_stats()
        print("\n📊 Service Statistics:")
        print(f"   Messages processed: {stats['messages_processed']}")
        print(f"   Messages failed: {stats['messages_failed']}")
        print(f"   Messages published: {stats['messages_published']}")

        # Cleanup
        await service.stop()

        success = stats["messages_processed"] > 0
        if success:
            print("\n✅ QCAgentKafkaService test passed!")
        else:
            print("\n❌ QCAgentKafkaService test failed!")

        return success

    except ImportError as e:
        print(f"\n❌ Import error: {e}")
        return False
    except Exception as e:
        print(f"\n❌ Error: {e}")
        import traceback

        traceback.print_exc()
        return False


async def main():
    """Main test function."""
    import argparse

    parser = argparse.ArgumentParser(description="Test QC agent with Kafka")
    parser.add_argument(
        "--direct",
        action="store_true",
        help="Test Kafka connection directly without running agent",
    )
    parser.add_argument(
        "--api",
        action="store_true",
        help="Test through API server (requires running server)",
    )
    parser.add_argument(
        "--service", action="store_true", help="Test QCAgentKafkaService directly"
    )

    args = parser.parse_args()

    if args.direct:
        success = await test_kafka_directly()
    elif args.api:
        success = await test_qc_agent_via_api()
    elif args.service:
        success = await test_qc_kafka_service()
    else:
        # Run all tests
        print("\n🧪 Running direct Kafka test first...")
        kafka_success = await test_kafka_directly()

        print("\n🧪 Running QCAgentKafkaService test...")
        service_success = await test_qc_kafka_service()

        print("\n🧪 Running API test...")
        api_success = await test_qc_agent_via_api()

        success = kafka_success and service_success and api_success

    return 0 if success else 1


if __name__ == "__main__":
    import sys

    sys.exit(asyncio.run(main()))
