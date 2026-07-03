"""
Test script for Template-based QC Agent API

This script tests the template-based QC agent without DSPy nodes:
1. Calls the QC agent API for evaluation
2. Waits for the task to complete
3. Retrieves and displays the QC evaluation results

Usage:
    python scripts/test_qc_template.py
"""

import asyncio
import json
import logging
from typing import Dict, Any

import httpx

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)

# API URLs
QC_API_BASE = "http://localhost:8000"
QC_AGENT_CONFIG = "agent_config/qc_agent_template.yml"


async def call_qc_agent(qc_request: Dict[str, Any]) -> Dict[str, Any]:
    """Call the QC agent API for evaluation."""
    url = f"{QC_API_BASE}/api/v2/qc-simple/evaluate"

    headers = {"Content-Type": "application/json"}

    logger.info(f"Calling QC API for chat_id: {qc_request['chat_id']}")
    response = await httpx.AsyncClient().post(
        url, json=qc_request, headers=headers, timeout=600.0
    )
    response.raise_for_status()

    logger.info(f"✓ QC API call successful for chat_id: {qc_request['chat_id']}")
    return response.json()


async def wait_for_qc_completion(task_id: str) -> Dict[str, Any]:
    """Wait for QC evaluation task to complete."""
    import time

    start_time = time.time()

    while time.time() - start_time < 600:
        response = await httpx.AsyncClient().get(
            f"{QC_API_BASE}/tasks/{task_id}", timeout=30.0
        )
        response.raise_for_status()
        task_status = response.json()

        status = task_status.get("status")
        if status == "completed":
            logger.info(f"✓ Task {task_id} completed")
            return task_status
        elif status == "failed":
            raise Exception(f"Task {task_id} failed: {task_status.get('error')}")

        await asyncio.sleep(5)

    raise Exception(f"Task {task_id} timed out")


async def main():
    """Main function to test the template-based QC agent."""
    logger.info("=" * 60)
    logger.info("Testing Template-based QC Agent")
    logger.info("=" * 60)
    logger.info(f"Using agent config: {QC_AGENT_CONFIG}")

    # Test data - a simple customer support conversation
    qc_request = {
        "chat_id": "test-123",
        "chat_conversation": [
            {
                "role": "customer",
                "message": "I'm trying to withdraw money but it says I need to enter an email code. My internet is down, what should I do?",
                "timestamp": "2026-01-12T07:44:54.316864Z",
            },
            {
                "role": "agent",
                "message": "Good afternoon! If you don't mind, we can switch your login method to SMS and you'll receive the code via text message. Would you like us to do that for you? Thank you for being with us as always💚 Whenever you need guidance or support, the support team is by your side. We appreciate your patience and trust🌸🧡",
                "timestamp": "2026-01-12T07:44:54.331466Z",
            },
        ],
    }

    try:
        # Call QC agent API
        qc_response = await call_qc_agent(qc_request)

        # Wait for completion if background task
        if qc_response.get("status") == "pending":
            task_id = qc_response.get("task_id")
            logger.info(f"Waiting for task {task_id} to complete...")
            final_status = await wait_for_qc_completion(task_id)
            qc_result = final_status.get("result", {})
        else:
            qc_result = qc_response

        # Print results
        logger.info("=" * 60)
        logger.info("QC Evaluation Results")
        logger.info("=" * 60)
        print(json.dumps(qc_result, indent=2, ensure_ascii=False))
        logger.info("=" * 60)
        logger.info("✓ Test completed successfully")
        logger.info("=" * 60)

    except Exception as e:
        logger.error(f"✗ Test failed: {e}")
        import traceback

        logger.error(traceback.format_exc())
        return 1

    return 0


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    exit(exit_code)
