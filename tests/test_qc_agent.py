"""
QC Agent Test Script - Run with sample chat conversation

Usage:
    python test_qc_agent.py
"""

import asyncio
from src.agent.factory import AgentFactory

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


async def run_qc_agent():
    print("=" * 60)
    print("QC AGENT TEST")
    print("=" * 60)

    # Create agent from YAML config
    print("\n📦 Loading agent configuration...")
    factory = AgentFactory("agent_config/qc_agent.yml")
    graph = await factory.create()
    print("✅ Agent created successfully!")

    # Initial state
    initial_state = {"chat_id": "test-chat-001", "chat_conversation": SAMPLE_CHAT}

    print(f"\n📝 Chat ID: {initial_state['chat_id']}")
    print(f"💬 Messages: {len(SAMPLE_CHAT)}")

    # Run agent
    print("\n🚀 Running QC agent...")
    result = await graph.ainvoke(initial_state)
    print("✅ Agent execution complete!")

    # Print results
    print("\n" + "=" * 60)
    print("QC AGENT RESULTS")
    print("=" * 60)

    if result.get("analysis_result"):
        print("\n📊 Chat Analysis:")
        for key, value in result["analysis_result"].items():
            print(f"  {key}: {value}")

    if result.get("qc_evaluation"):
        print("\n🎯 QC Evaluation:")
        print(f"  Score: {result['qc_evaluation'].get('score', 'N/A')}")
        print(f"  Reasons: {result['qc_evaluation'].get('reasons', 'N/A')}")

    print("\n" + "=" * 60)
    print("✅ Test completed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    asyncio.run(run_qc_agent())
