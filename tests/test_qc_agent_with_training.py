"""
Test QC Agent with Training Data for Prompt Optimization

This script tests the QC agent against training data to:
1. Evaluate current prompt performance
2. Identify areas for improvement
3. Generate optimization recommendations
4. Run DSPy optimization to improve prompts

Usage:
    uv run test_qc_agent_with_training.py
    uv run test_qc_agent_with_training.py --no-optimization
    uv run test_qc_agent_with_training.py --enable-communication
"""

import asyncio
import json
import os
import subprocess
from typing import Dict
import statistics
from datetime import datetime

from src.agent.factory import AgentFactory
from src.config.settings import Settings


class QCAgentTester:
    """Test QC agent against training data"""

    def __init__(self, disable_communication: bool = True):
        self.settings = Settings()
        self.agent_factory = AgentFactory()
        self.results = {"chat_analysis": [], "score_generation": [], "overall": []}
        self.disable_communication = disable_communication

    async def load_training_data(self, filepath: str) -> Dict:
        """Load training data from JSON file"""
        with open(filepath, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data

    async def test_chat_analysis(self, training_data: Dict) -> Dict:
        """Test chat analysis node against training data"""
        print("\n" + "=" * 80)
        print("TESTING CHAT ANALYSIS NODE")
        print("=" * 80)

        # Load QC agent
        await self.agent_factory.load_config("agent_config/qc_agent.yml")
        graph = await self.agent_factory.build_graph()

        results = []
        total_examples = len(training_data["examples"])

        for i, example in enumerate(training_data["examples"], 1):
            print(f"\n--- Example {i}/{total_examples} ---")

            # Parse chat conversation
            chat_conversation = json.loads(example["chat_conversation"])

            # Prepare input state
            initial_state = {
                "chat_id": f"test_{i}",
                "chat_conversation": chat_conversation,
            }

            try:
                # Execute agent
                result = await graph.ainvoke(initial_state)

                # Extract analysis result
                analysis = result.get("analysis_result", {})

                # Calculate metrics
                expected = {
                    "empathy_score": example["empathy_score"],
                    "professionalism_score": example["professionalism_score"],
                    "resolution_quality": example["resolution_quality"],
                    "response_time_appropriateness": example[
                        "response_time_appropriateness"
                    ],
                    "protocol_adherence": example["protocol_adherence"],
                }

                actual = {
                    "empathy_score": analysis.get("empathy_score", 0),
                    "professionalism_score": analysis.get("professionalism_score", 0),
                    "resolution_quality": analysis.get("resolution_quality", 0),
                    "response_time_appropriateness": analysis.get(
                        "response_time_appropriateness", 0
                    ),
                    "protocol_adherence": analysis.get("protocol_adherence", 0),
                }

                # Calculate absolute differences
                differences = {
                    key: abs(expected[key] - actual[key]) for key in expected.keys()
                }

                # Calculate mean absolute error
                mae = statistics.mean(differences.values())

                result_data = {
                    "example_id": i,
                    "expected": expected,
                    "actual": actual,
                    "differences": differences,
                    "mae": mae,
                    "key_observations_expected": example["key_observations"],
                    "key_observations_actual": analysis.get("key_observations", []),
                }

                results.append(result_data)

                print(f"Expected: {expected}")
                print(f"Actual: {actual}")
                print(f"MAE: {mae:.2f}")

            except Exception as e:
                print(f"Error processing example {i}: {str(e)}")
                results.append({"example_id": i, "error": str(e)})

        # Calculate aggregate metrics
        successful_results = [r for r in results if "error" not in r]

        if successful_results:
            avg_mae = statistics.mean([r["mae"] for r in successful_results])

            # Calculate per-metric average differences
            metric_errors = {}
            for metric in [
                "empathy_score",
                "professionalism_score",
                "resolution_quality",
                "response_time_appropriateness",
                "protocol_adherence",
            ]:
                metric_errors[metric] = statistics.mean(
                    [r["differences"][metric] for r in successful_results]
                )

            summary = {
                "total_examples": total_examples,
                "successful": len(successful_results),
                "failed": total_examples - len(successful_results),
                "average_mae": avg_mae,
                "metric_errors": metric_errors,
                "results": results,
            }

            print("\n--- CHAT ANALYSIS SUMMARY ---")
            print(f"Total Examples: {total_examples}")
            print(f"Successful: {len(successful_results)}")
            print(f"Failed: {total_examples - len(successful_results)}")
            print(f"Average MAE: {avg_mae:.2f}")
            print("\nPer-Metric Errors:")
            for metric, error in metric_errors.items():
                print(f"  {metric}: {error:.2f}")

            return summary

        return {"error": "No successful results"}

    async def test_score_generation(self, training_data: Dict) -> Dict:
        """Test score generation node against training data"""
        print("\n" + "=" * 80)
        print("TESTING SCORE GENERATION NODE")
        print("=" * 80)

        # Load QC agent
        await self.agent_factory.load_config("agent_config/qc_agent.yml")
        graph = await self.agent_factory.build_graph()

        results = []
        total_examples = len(training_data["examples"])

        for i, example in enumerate(training_data["examples"], 1):
            print(f"\n--- Example {i}/{total_examples} ---")

            # Parse chat conversation
            chat_conversation = json.loads(example["chat_conversation"])

            # Prepare input state
            initial_state = {
                "chat_id": f"test_{i}",
                "chat_conversation": chat_conversation,
            }

            try:
                # Execute agent
                result = await graph.ainvoke(initial_state)

                # Extract QC evaluation
                evaluation = result.get("qc_evaluation", {})

                # Calculate metrics
                expected_score = example["score"]
                actual_score = evaluation.get("score", 0)

                score_difference = abs(expected_score - actual_score)

                result_data = {
                    "example_id": i,
                    "expected_score": expected_score,
                    "actual_score": actual_score,
                    "score_difference": score_difference,
                    "reasons": evaluation.get("reasons", []),
                    "strengths": evaluation.get("strengths", []),
                    "areas_for_improvement": evaluation.get(
                        "areas_for_improvement", []
                    ),
                }

                results.append(result_data)

                print(f"Expected Score: {expected_score}")
                print(f"Actual Score: {actual_score}")
                print(f"Difference: {score_difference}")

            except Exception as e:
                print(f"Error processing example {i}: {str(e)}")
                results.append({"example_id": i, "error": str(e)})

        # Calculate aggregate metrics
        successful_results = [r for r in results if "error" not in r]

        if successful_results:
            avg_score_diff = statistics.mean(
                [r["score_difference"] for r in successful_results]
            )

            # Calculate accuracy within tolerance
            exact_matches = sum(
                1 for r in successful_results if r["score_difference"] == 0
            )
            within_1 = sum(1 for r in successful_results if r["score_difference"] <= 1)
            within_2 = sum(1 for r in successful_results if r["score_difference"] <= 2)

            summary = {
                "total_examples": total_examples,
                "successful": len(successful_results),
                "failed": total_examples - len(successful_results),
                "average_score_difference": avg_score_diff,
                "exact_matches": exact_matches,
                "within_1_point": within_1,
                "within_2_points": within_2,
                "exact_match_rate": exact_matches / len(successful_results) * 100,
                "within_1_rate": within_1 / len(successful_results) * 100,
                "within_2_rate": within_2 / len(successful_results) * 100,
                "results": results,
            }

            print("\n--- SCORE GENERATION SUMMARY ---")
            print(f"Total Examples: {total_examples}")
            print(f"Successful: {len(successful_results)}")
            print(f"Failed: {total_examples - len(successful_results)}")
            print(f"Average Score Difference: {avg_score_diff:.2f}")
            print(
                f"Exact Matches: {exact_matches} ({exact_matches / len(successful_results) * 100:.1f}%)"
            )
            print(
                f"Within 1 Point: {within_1} ({within_1 / len(successful_results) * 100:.1f}%)"
            )
            print(
                f"Within 2 Points: {within_2} ({within_2 / len(successful_results) * 100:.1f}%)"
            )

            return summary

        return {"error": "No successful results"}

    def generate_optimization_recommendations(
        self, chat_analysis_results: Dict, score_generation_results: Dict
    ) -> Dict:
        """Generate prompt optimization recommendations based on test results"""

        recommendations = {"chat_analysis": [], "score_generation": [], "overall": []}

        # Chat Analysis Recommendations
        if "metric_errors" in chat_analysis_results:
            metric_errors = chat_analysis_results["metric_errors"]

            for metric, error in metric_errors.items():
                if error > 1.5:
                    recommendations["chat_analysis"].append(
                        {
                            "metric": metric,
                            "issue": f"High error rate ({error:.2f})",
                            "severity": "HIGH",
                            "recommendation": f"Add more training examples focusing on {metric.replace('_', ' ')}",
                            "prompt_suggestion": f"Include specific examples and rubric for {metric.replace('_', ' ')} in the prompt",
                        }
                    )
                elif error > 1.0:
                    recommendations["chat_analysis"].append(
                        {
                            "metric": metric,
                            "issue": f"Moderate error rate ({error:.2f})",
                            "severity": "MEDIUM",
                            "recommendation": f"Refine scoring criteria for {metric.replace('_', ' ')}",
                            "prompt_suggestion": f"Add detailed explanation of {metric.replace('_', ' ')} evaluation criteria",
                        }
                    )

        # Score Generation Recommendations
        if "average_score_difference" in score_generation_results:
            avg_diff = score_generation_results["average_score_difference"]
            exact_rate = score_generation_results.get("exact_match_rate", 0)

            if avg_diff > 1.5:
                recommendations["score_generation"].append(
                    {
                        "issue": f"High score deviation ({avg_diff:.2f})",
                        "severity": "HIGH",
                        "recommendation": "Improve score aggregation logic",
                        "prompt_suggestion": "Add weighted scoring formula in the prompt",
                    }
                )

            if exact_rate < 50:
                recommendations["score_generation"].append(
                    {
                        "issue": f"Low exact match rate ({exact_rate:.1f}%)",
                        "severity": "HIGH",
                        "recommendation": "Add more diverse training examples",
                        "prompt_suggestion": "Include edge cases and boundary conditions in training data",
                    }
                )

        # Overall Recommendations
        if (
            chat_analysis_results.get("average_mae", 0) > 1.0
            or score_generation_results.get("average_score_difference", 0) > 1.0
        ):
            recommendations["overall"].append(
                {
                    "issue": "Overall performance needs improvement",
                    "severity": "MEDIUM",
                    "recommendation": "Consider using DSPy optimization with more training data",
                    "prompt_suggestion": "Enable DSPy optimization in agent configuration with MIPRO teleprompter",
                }
            )

        return recommendations

    def print_recommendations(self, recommendations: Dict):
        """Print optimization recommendations"""
        print("\n" + "=" * 80)
        print("OPTIMIZATION RECOMMENDATIONS")
        print("=" * 80)

        for category, recs in recommendations.items():
            if recs:
                print(f"\n{category.upper()}:")
                for i, rec in enumerate(recs, 1):
                    print(
                        f"\n{i}. [{rec.get('severity', 'INFO')}] {rec.get('issue', 'Unknown')}"
                    )
                    print(f"   Recommendation: {rec.get('recommendation', 'N/A')}")
                    if "prompt_suggestion" in rec:
                        print(f"   Prompt Suggestion: {rec['prompt_suggestion']}")
                    if "metric" in rec:
                        print(f"   Affected Metric: {rec['metric']}")

    def run_dspy_optimization(self, node_name: str, training_data_path: str) -> Dict:
        """Run DSPy optimization for a specific node"""
        print(f"\n{'=' * 80}")
        print(f"RUNNING DSPY OPTIMIZATION FOR NODE: {node_name}")
        print(f"{'=' * 80}")

        output_dir = "data/optimized_prompts"
        os.makedirs(output_dir, exist_ok=True)

        # Build command to run optimization script
        cmd = [
            "uv",
            "run",
            "scripts/optimize_dspy.py",
            "--agent",
            "qc_agent",
            "--node",
            node_name,
            "--training-data",
            training_data_path,
            "--output",
            output_dir,
            "--keep-last",
        ]

        print(f"Running command: {' '.join(cmd)}")

        try:
            # Run optimization
            result = subprocess.run(
                cmd,
                capture_output=True,
                text=True,
                timeout=600,  # 10 minute timeout
            )

            if result.returncode == 0:
                print("✓ Optimization completed successfully")
                print(f"Output:\n{result.stdout}")

                # Try to load the optimized prompts
                optimized_file = os.path.join(output_dir, f"{node_name}.json")
                if os.path.exists(optimized_file):
                    with open(optimized_file, "r") as f:
                        optimized_data = json.load(f)

                    return {
                        "success": True,
                        "optimized_file": optimized_file,
                        "metrics": optimized_data.get("metrics", {}),
                        "version": optimized_data.get("version", "unknown"),
                    }
                else:
                    return {
                        "success": True,
                        "message": "Optimization completed but output file not found",
                    }
            else:
                print(f"✗ Optimization failed with return code {result.returncode}")
                print(f"Error output:\n{result.stderr}")
                return {
                    "success": False,
                    "error": result.stderr,
                    "return_code": result.returncode,
                }

        except subprocess.TimeoutExpired:
            print("✗ Optimization timed out after 10 minutes")
            return {"success": False, "error": "Optimization timed out"}
        except Exception as e:
            print(f"✗ Optimization failed with exception: {str(e)}")
            return {"success": False, "error": str(e)}

    async def run_full_test(self, run_optimization: bool = True):
        """Run complete test suite"""
        print("=" * 80)
        print("QC AGENT TESTING WITH TRAINING DATA")
        print("=" * 80)
        print(f"Started at: {datetime.now().isoformat()}")

        if self.disable_communication:
            print(
                "\n⚠️  Communication disabled - No Kafka or external messages will be sent"
            )

        # Load training data
        print("\nLoading training data...")
        chat_analysis_data = await self.load_training_data(
            "data/training/qc_chat_analysis.json"
        )
        score_generation_data = await self.load_training_data(
            "data/training/qc_score_generation.json"
        )

        print(f"Loaded {len(chat_analysis_data['examples'])} chat analysis examples")
        print(
            f"Loaded {len(score_generation_data['examples'])} score generation examples"
        )

        # Run tests
        chat_analysis_results = await self.test_chat_analysis(chat_analysis_data)
        score_generation_results = await self.test_score_generation(
            score_generation_data
        )

        # Generate recommendations
        recommendations = self.generate_optimization_recommendations(
            chat_analysis_results, score_generation_results
        )

        # Print recommendations
        self.print_recommendations(recommendations)

        # Run DSPy optimization if requested
        optimization_results = {}
        if run_optimization:
            print("\n" + "=" * 80)
            print("RUNNING DSPY OPTIMIZATION")
            print("=" * 80)

            # Optimize chat_analyzer node
            chat_analyzer_opt = self.run_dspy_optimization(
                "chat_analyzer", "data/training/qc_chat_analysis.json"
            )
            optimization_results["chat_analyzer"] = chat_analyzer_opt

            # Optimize score_generator node
            score_generator_opt = self.run_dspy_optimization(
                "score_generator", "data/training/qc_score_generation.json"
            )
            optimization_results["score_generator"] = score_generator_opt

        # Save results
        output = {
            "test_run": {
                "timestamp": datetime.now().isoformat(),
                "communication_disabled": self.disable_communication,
                "chat_analysis_results": chat_analysis_results,
                "score_generation_results": score_generation_results,
                "recommendations": recommendations,
                "optimization_results": optimization_results
                if run_optimization
                else None,
            }
        }

        output_file = f"test_results/qc_agent_test_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
        os.makedirs("test_results", exist_ok=True)

        with open(output_file, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2, ensure_ascii=False)

        print("\n" + "=" * 80)
        print(f"Test results saved to: {output_file}")
        print(f"Completed at: {datetime.now().isoformat()}")
        print("=" * 80)

        return output


async def main():
    """Main entry point"""
    import argparse

    parser = argparse.ArgumentParser(
        description="Test QC Agent with Training Data and DSPy Optimization"
    )
    parser.add_argument(
        "--no-optimization", action="store_true", help="Skip DSPy optimization step"
    )
    parser.add_argument(
        "--enable-communication",
        action="store_true",
        help="Enable Kafka and external communication (disabled by default for testing)",
    )

    args = parser.parse_args()

    tester = QCAgentTester(disable_communication=not args.enable_communication)

    results = await tester.run_full_test(run_optimization=not args.no_optimization)

    return results


if __name__ == "__main__":
    asyncio.run(main())
