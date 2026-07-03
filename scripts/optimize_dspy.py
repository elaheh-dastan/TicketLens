"""
DSPy Optimization Script

This script runs DSPy optimization on agent prompts using training data.
It can be run manually or scheduled as a cron job.

Usage:
    python scripts/optimize_dspy.py --agent qc_agent --node chat_analyzer
    python scripts/optimize_dspy.py --all
"""

import argparse
import json
import logging
import sys
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Any

import dspy

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.config.app_config import DSPyTeleprompter
from src.nodes.dspy_node import DSPyNode
from src.llm.dspy_adapter import create_dspy_lm, configure_dspy_lm
from src.config.settings import get_settings

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def load_training_data(data_path: Path) -> List[Dict[str, Any]]:
    """Load training data from JSON file."""
    if not data_path.exists():
        logger.warning(f"Training data file not found: {data_path}")
        return []

    examples = []

    if data_path.is_file():
        with open(data_path, "r") as f:
            data = json.load(f)
        examples.extend(_extract_examples(data))
    else:
        for file_path in data_path.glob("*.json"):
            with open(file_path, "r") as f:
                data = json.load(f)
            examples.extend(_extract_examples(data))

    return examples


def _extract_examples(data: Any) -> List[Dict[str, Any]]:
    """Extract examples from raw data."""
    if isinstance(data, list):
        return data
    elif isinstance(data, dict) and "examples" in data:
        return data["examples"]
    elif isinstance(data, dict) and "evaluations" in data:
        return data["evaluations"]
    return [data] if isinstance(data, dict) else []


def create_metric(metric_type: str, output_fields: Dict[str, str]):
    """Create metric function."""
    if metric_type == "exact_match":

        def metric(example, prediction, *_, **__):
            field_names = (
                list(output_fields.keys())
                if hasattr(output_fields, "keys")
                else list(output_fields)
            )
            return all(
                getattr(prediction, k, None) == getattr(example, k, None)
                for k in field_names
            )

        return metric
    elif metric_type == "score_proximity":
        # For QC scoring: check if predicted score is within tolerance of expected
        def metric(example, prediction, *_, **__):
            try:
                expected_score = float(getattr(example, "score", 0))
                predicted_score = float(getattr(prediction, "score", 0))
                # Allow 1.5 point tolerance for score matching
                return abs(expected_score - predicted_score) <= 1.5
            except (ValueError, TypeError):
                return False

        return metric
    elif metric_type == "multi_score_proximity":
        # Composite metric for QC scoring with multiple score fields + text reasons
        # Pass if ≥60% of scores are within tolerance AND reasons has word overlap
        score_fields = ["score", "tone_score", "empathy_score", "solution_quality", "clarity_score"]
        text_fields = ["reasons"]
        tolerance = 1.5

        def metric(example, prediction, *_, **__):
            # Check numeric score fields
            scores_matched = 0
            scores_checked = 0
            for field in score_fields:
                expected_val = getattr(example, field, None)
                predicted_val = getattr(prediction, field, None)
                if expected_val is not None:
                    scores_checked += 1
                    try:
                        expected_score = float(expected_val)
                        predicted_score = float(predicted_val) if predicted_val else 0
                        if abs(expected_score - predicted_score) <= tolerance:
                            scores_matched += 1
                    except (ValueError, TypeError):
                        pass

            # Check if at least 60% of scores are within tolerance
            score_pass = scores_checked > 0 and (scores_matched / scores_checked) >= 0.6

            # Check text field (reasons) for word overlap
            text_pass = False
            for field in text_fields:
                expected = str(getattr(example, field, "") or "").lower()
                predicted = str(getattr(prediction, field, "") or "").lower()
                expected_words = set(expected.split())
                predicted_words = set(predicted.split())
                if expected_words & predicted_words:
                    text_pass = True
                    break

            # Pass if scores pass AND text has overlap (or no text field expected)
            if not any(getattr(example, f, None) for f in text_fields):
                return score_pass
            return score_pass and text_pass

        return metric
    elif metric_type == "partial_match":
        # For text fields: check if key terms overlap
        def metric(example, prediction, *_, **__):
            field_names = (
                list(output_fields.keys())
                if hasattr(output_fields, "keys")
                else list(output_fields)
            )
            matches = 0
            for k in field_names:
                expected = str(getattr(example, k, "") or "").lower()
                predicted = str(getattr(prediction, k, "") or "").lower()
                # Check if any significant word overlap
                expected_words = set(expected.split())
                predicted_words = set(predicted.split())
                if expected_words & predicted_words:
                    matches += 1
            return matches >= len(field_names) / 2

        return metric
    else:
        # Default: always return True (just train with few-shot examples)
        def metric(example, prediction, *_, **__):
            return True

        return metric


def create_teleprompter(teleprompter_type: str, metric_fn, config: Dict[str, Any]):
    """Create DSPy teleprompter."""
    if teleprompter_type == DSPyTeleprompter.BOOTSTRAP_FEW_SHOT.value:
        return dspy.BootstrapFewShot(
            metric=metric_fn,
            max_bootstrapped_demos=config.get("max_bootstrapped_demos", 8),
            max_labeled_demos=config.get("max_labeled_demos", 4),
        )
    elif teleprompter_type == DSPyTeleprompter.MIPRO.value:
        # Use MIPROv2 (MIPRO was renamed in newer DSPy versions)
        # MIPROv2 uses 'auto' parameter for automatic configuration
        return dspy.MIPROv2(
            metric=metric_fn,
            auto="medium",  # Use 'light', 'medium', or 'heavy' for automatic configuration
        )
    elif teleprompter_type == "COPRO":
        return dspy.COPRO(
            metric=metric_fn,
            breadth=config.get("breadth", 10),
            depth=config.get("depth", 3),
        )
    elif teleprompter_type == "LabeledFewShot":
        return dspy.LabeledFewShot(k=config.get("max_labeled_demos", 4))
    else:
        raise ValueError(f"Unsupported teleprompter: {teleprompter_type}")


def optimize_node(
    agent_config_path: str,
    node_name: str,
    training_data_path: str,
    output_path: str,
    keep_last: bool = False,
):
    """
    Optimize a single node's prompts.

    Args:
        agent_config_path: Path to agent YAML config
        node_name: Name of the node to optimize
        training_data_path: Path to training data
        output_path: Directory to save optimized prompts
        keep_last: If True, keep the last version as .last before saving new version
    """
    logger.info(f"Optimizing node: {node_name}")

    # Load agent config
    from src.config.app_config import load_config

    config = load_config(agent_config_path)

    # Find node config
    node_config = None
    for name, cfg in config.graph.nodes.items():
        if name == node_name:
            node_config = cfg
            break

    if not node_config:
        raise ValueError(f"Node '{node_name}' not found in config")

    if not node_config.dspy or not node_config.dspy.enabled:
        raise ValueError(f"DSPy not enabled for node '{node_name}'")

    # Configure DSPy LM - get model from agent config or node config
    settings = get_settings()
    provider = (
        settings.llms.provider
        if isinstance(settings.llms.provider, str)
        else str(settings.llms.provider)
    )

    # Get model from node's DSPy config, or fall back to agent's model config
    if node_config.dspy.model:
        model = node_config.dspy.model
    else:
        model = config.models.generator

    # Get provider from node's DSPy config, or fall back to settings
    if node_config.dspy.provider:
        provider = node_config.dspy.provider

    dspy_lm = create_dspy_lm(
        provider=provider,
        model=model,
        api_key=settings.llms.openrouter_api_key,
    )
    configure_dspy_lm(dspy_lm)

    # Create DSPy node
    dspy_node = DSPyNode(name=node_name, dspy_config=node_config.dspy)

    # Initialize (creates signature and module)
    import asyncio

    asyncio.run(dspy_node._initialize_dspy())

    # Load training data
    training_data = load_training_data(Path(training_data_path))

    if not training_data:
        raise ValueError(f"No training data found at {training_data_path}")

    logger.info(f"Loaded {len(training_data)} training examples")

    # Convert to DSPy examples
    input_fields = list(node_config.dspy.signature.input_fields.keys())
    output_fields = list(node_config.dspy.signature.output_fields.keys())

    examples = []
    for item in training_data:
        inputs = {k: item.get(k) for k in input_fields if k in item}
        outputs = {k: item.get(k) for k in output_fields if k in item}
        if inputs and outputs:
            example = dspy.Example(**inputs, **outputs).with_inputs(*input_fields)
            examples.append(example)

    # Split train/validation
    split_idx = int(len(examples) * 0.8)
    train_set = examples[:split_idx]
    val_set = examples[split_idx:]

    # Create metric and teleprompter
    opt_config = node_config.dspy.optimization
    metric_fn = create_metric(opt_config.metric.value, output_fields)
    teleprompter = create_teleprompter(
        opt_config.teleprompter.value,
        metric_fn,
        {
            "max_bootstrapped_demos": opt_config.max_bootstrapped_demos,
            "max_labeled_demos": opt_config.max_labeled_demos,
            "num_candidates": opt_config.num_candidates,
            "init_temperature": opt_config.init_temperature,
        },
    )

    # Run optimization
    logger.info(f"Running {opt_config.teleprompter.value} optimization...")

    # Different teleprompters have different compile signatures
    if opt_config.teleprompter.value in ["BootstrapFewShot", "LabeledFewShot"]:
        # BootstrapFewShot doesn't accept valset
        optimized_module = teleprompter.compile(
            dspy_node.dspy_module, trainset=train_set
        )
    else:
        # MIPROv2, COPRO etc. accept valset
        optimized_module = teleprompter.compile(
            dspy_node.dspy_module, trainset=train_set, valset=val_set
        )

    # Evaluate
    total_metric = 0
    for example in val_set:
        inputs = {k: getattr(example, k) for k in input_fields}
        prediction = optimized_module(**inputs)
        metric_score = metric_fn(example, prediction)
        total_metric += metric_score

    avg_metric = total_metric / len(val_set) if val_set else 0

    metrics = {
        "average_metric": avg_metric,
        "num_training": len(train_set),
        "num_validation": len(val_set),
        "timestamp": datetime.utcnow().isoformat(),
    }

    # Save optimized prompts
    output_dir = Path(output_path)
    output_dir.mkdir(parents=True, exist_ok=True)

    output_file = output_dir / f"{node_name}.json"
    last_file = output_dir / f"{node_name}.last.json"

    # If keep_last is True and existing file exists, save it as .last.json
    if keep_last and output_file.exists():
        logger.info(f"Backing up existing version to {last_file}")
        output_file.rename(last_file)

    prompts_data = {
        "node_name": node_name,
        "module_type": node_config.dspy.module.value,
        "module_state": optimized_module.dump_state(),
        "metrics": metrics,
        "version": datetime.utcnow().strftime("%Y%m%d_%H%M%S"),
        "optimized_at": datetime.utcnow().isoformat(),
    }

    with open(output_file, "w") as f:
        json.dump(prompts_data, f, indent=2)

    logger.info(f"Saved optimized prompts to {output_file}")

    if keep_last:
        logger.info(f"Last version saved to {last_file}")

    logger.info(f"Optimization complete. Average metric: {avg_metric:.2%}")

    return metrics


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(description="DSPy Optimization Script")
    parser.add_argument("--agent", "-a", help="Agent config file (without .yml)")
    parser.add_argument("--node", "-n", help="Node name to optimize")
    parser.add_argument("--training-data", "-t", help="Path to training data")
    parser.add_argument(
        "--output",
        "-o",
        default="data/optimized_prompts",
        help="Output directory for optimized prompts",
    )
    parser.add_argument("--all", action="store_true", help="Optimize all DSPy nodes")
    parser.add_argument(
        "--keep-last",
        "-k",
        action="store_true",
        help="Keep the last version as .last.json before saving new version",
    )

    args = parser.parse_args()

    if not args.agent and not args.all:
        parser.error("Either --agent or --all must be specified")

    if args.agent and not args.node:
        parser.error("--node is required when --agent is specified")

    try:
        if args.all:
            # TODO: Implement optimization for all agents
            logger.info("--all not implemented yet. Use --agent --node")
            sys.exit(1)
        else:
            optimize_node(
                agent_config_path=f"agent_config/{args.agent}.yml",
                node_name=args.node,
                training_data_path=args.training_data
                or f"data/training/{args.node}.json",
                output_path=args.output,
                keep_last=args.keep_last,
            )

    except Exception as e:
        logger.error(f"Optimization failed: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
