"""
Evaluate CSV conversations using the QC DSPy agent.

Usage:
    uv run scripts/qc_sample_data.py -i data/training/qc_sample_data_50_v2.csv --limit 50 -o data/result/qc_sample_data_50_v2_results_1.json
"""

import argparse
import asyncio
import json
import logging
import sys
from pathlib import Path
from datetime import datetime

import pandas as pd

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.agent.factory import AgentFactory

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
logger = logging.getLogger(__name__)


def parse_conversation(conv_json: str) -> list:
    """Parse conversation JSON into the expected format."""
    try:
        messages = json.loads(conv_json)
        formatted = []
        for msg in messages:
            role = "customer" if msg.get("sender") == "USER" else "agent"
            formatted.append(
                {
                    "role": role,
                    "message": msg.get("message", ""),
                    "timestamp": msg.get("time", ""),
                }
            )
        return formatted
    except (json.JSONDecodeError, TypeError) as e:
        logger.warning(f"Failed to parse conversation: {e}")
        return []


async def evaluate_conversation(
    agent_factory: AgentFactory, chat_id: str, conversation: list
) -> dict:
    """Evaluate a single conversation using the QC agent."""
    # Build the graph
    graph = await agent_factory.build_graph()

    # Prepare initial state
    initial_state = {
        "chat_id": chat_id,
        "chat_conversation": conversation,
    }

    # Run the graph
    result = await graph.ainvoke(initial_state)

    return {
        "chat_id": chat_id,
        "main_problem": result.get("main_problem", "N/A"),
        "score": result.get("score", "N/A"),
        "tone_score": result.get("tone_score", "N/A"),
        "empathy_score": result.get("empathy_score", "N/A"),
        "solution_quality": result.get("solution_quality", "N/A"),
        "clarity_score": result.get("clarity_score", "N/A"),
        "key_observations": result.get("key_observations", "N/A"),
        "reasons": result.get("reasons", "N/A"),
    }


async def main():
    parser = argparse.ArgumentParser(
        description="Evaluate CSV conversations with QC agent"
    )
    parser.add_argument("--input", "-i", required=True, help="Path to CSV file")
    parser.add_argument(
        "--limit", "-l", type=int, default=50, help="Number of conversations to evaluate"
    )
    parser.add_argument(
        "--agent", "-a", default="qc_agent", help="Agent config name"
    )
    parser.add_argument("--output", "-o", help="Output JSON file (optional)")

    args = parser.parse_args()

    # Read CSV
    csv_path = Path(args.input).expanduser()
    if not csv_path.exists():
        logger.error(f"CSV file not found: {csv_path}")
        sys.exit(1)

    df = pd.read_csv(csv_path)
    logger.info(f"Loaded {len(df)} rows from {csv_path.name}")

    # Limit rows
    df = df.head(args.limit)

    # Create agent factory
    agent_config_path = Path(f"agent_config/{args.agent}.yml")
    if not agent_config_path.exists():
        logger.error(f"Agent config not found: {agent_config_path}")
        sys.exit(1)

    results = []

    print("\n" + "=" * 80)
    print(f"QC EVALUATION RESULTS - {csv_path.name}")
    print("=" * 80)

    for idx, row in df.iterrows():
        chat_id = str(row.get("Ticket_Number", f"row_{idx}"))
        category = row.get("category", "unknown")
        conv_history = row.get("Conversation_History", "[]")

        # Parse conversation
        conversation = parse_conversation(conv_history)
        if not conversation:
            logger.warning(f"Skipping {chat_id}: empty conversation")
            continue

        print(f"\n{'─' * 80}")
        print(f"📋 Ticket: {chat_id} | Category: {category}")
        print(f"{'─' * 80}")

        # Show conversation summary
        print("\n💬 Conversation Summary:")
        for msg in conversation[:3]:  # Show first 3 messages
            role_icon = "👤" if msg["role"] == "customer" else "🧑‍💼"
            text = (
                msg["message"][:100] + "..."
                if len(msg["message"]) > 100
                else msg["message"]
            )
            text = text.replace("\r\n", " ").replace("\n", " ")
            print(f"   {role_icon} {msg['role'].upper()}: {text}")
        if len(conversation) > 3:
            print(f"   ... ({len(conversation) - 3} more messages)")

        # Evaluate
        try:
            factory = AgentFactory(agent_config_path)
            result = await evaluate_conversation(factory, chat_id, conversation)
            results.append(result)

            print(f"\n📊 QC Analysis:")
            print(f"   🎯 Main Problem: {result['main_problem']}")
            print(f"   ⭐ Overall Score: {result['score']}")
            print(f"   🎭 Tone Score: {result['tone_score']}")
            print(f"   💚 Empathy Score: {result['empathy_score']}")
            print(f"   🔧 Solution Quality: {result['solution_quality']}")
            print(f"   📖 Clarity Score: {result['clarity_score']}")
            print(f"   🔍 Key Observations: {result['key_observations']}")
            print(f"   📝 Reasons: {result['reasons']}")

        except Exception as e:
            logger.error(f"Failed to evaluate {chat_id}: {e}")
            results.append(
                {
                    "chat_id": chat_id,
                    "error": str(e),
                }
            )

    print(f"\n{'=' * 80}")
    print(f"✅ Evaluated {len(results)} conversations")
    print("=" * 80)

    # Save results if output specified
    if args.output:
        output_path = Path(args.output)
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "source_file": str(csv_path),
                    "evaluated_at": datetime.now().isoformat(),
                    "results": results,
                },
                f,
                ensure_ascii=False,
                indent=2,
            )
        print(f"\n📁 Results saved to: {output_path}")


if __name__ == "__main__":
    asyncio.run(main())
