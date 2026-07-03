# QC Agent Evaluation Guide

This guide explains how to use the QC evaluation script to compare human scoring with agent scoring.

## Overview

The evaluation script ([`scripts/evaluate_qc_results.py`](../scripts/evaluate_qc_results.py)) compares:
- **Human Scores**: From `Ticket_QC.xlsx` (manual QC scoring)
- **Agent Scores**: From `results.json` (AI agent scoring)

It generates comprehensive metrics, visualizations, and reports to assess agent performance.

## Installation

Install required dependencies:

```bash
pip install pandas numpy openpyxl scikit-learn matplotlib seaborn
```

Or use the requirements file:

```bash
pip install -r requirements_evaluation.txt
```

## Usage

### Basic Usage

Run the evaluation script from the project root:

```bash
python scripts/evaluate_qc_results.py
```

### Input Files

The script expects two files in the project root:

1. **`Ticket_QC.xlsx`** - Excel file with human scoring containing:
   - `Ticket_Number`: Unique ticket identifier
   - `tone_score`: Human tone score (0-10)
   - `empathy_score`: Human empathy score (0-10)
   - `solution_quality`: Human solution quality score (0-10)
   - `clarity_score`: Human clarity score (0-10)
   - `final_score`: Human final score (0-10)
   - `reason`: Human reasoning (optional)

2. **`results.json`** - JSON file with agent scoring containing:
   ```json
   {
     "results": [
       {
         "chat_id": "ticket_id",
         "tone_score": "8",
         "empathy_score": "7",
         "solution_quality": "8",
         "clarity_score": "9",
         "score": "8",
         "main_problem": "...",
         "reasons": "..."
       }
     ]
   }
   ```

## Output Files

The script generates three types of outputs:

### 1. Text Report (`evaluation_report.txt`)

Comprehensive text report including:
- Overall metrics for each score dimension
- Mean Absolute Error (MAE)
- Root Mean Squared Error (RMSE)
- R² Score
- Correlation coefficients
- Agreement percentages (within 1 and 2 points)
- Top 10 disagreements
- Top 10 agreements

### 2. Visualization Plots (`evaluation_plots/`)

Directory containing:
- **Scatter plots**: Human vs Agent scores with regression lines
  - `tone_score_comparison.png`
  - `empathy_score_comparison.png`
  - `solution_quality_comparison.png`
  - `clarity_score_comparison.png`
  - `final_score_comparison.png`
- **Distribution plots**: `score_distributions.png`
- **Difference plots**: `difference_distributions.png`

### 3. Detailed Comparison (`detailed_comparison.xlsx`)

Excel file with row-by-row comparison:
- All human scores
- All agent scores
- Absolute differences
- Human and agent reasoning
- Sorted by disagreement magnitude

## Metrics Explained

### Mean Absolute Error (MAE)
Average absolute difference between human and agent scores. Lower is better.
- **Excellent**: < 0.5
- **Good**: 0.5 - 1.0
- **Fair**: 1.0 - 1.5
- **Poor**: > 1.5

### Root Mean Squared Error (RMSE)
Square root of average squared differences. Penalizes large errors more. Lower is better.

### R² Score
Coefficient of determination. Measures how well agent scores predict human scores.
- **Excellent**: > 0.9
- **Good**: 0.7 - 0.9
- **Fair**: 0.5 - 0.7
- **Poor**: < 0.5

### Correlation
Pearson correlation coefficient. Measures linear relationship.
- **Strong**: > 0.8
- **Moderate**: 0.5 - 0.8
- **Weak**: < 0.5

### Agreement Percentages
- **Within 1 point**: Percentage of scores where |human - agent| ≤ 1
- **Within 2 points**: Percentage of scores where |human - agent| ≤ 2

Target: >80% within 1 point, >95% within 2 points

## Example Output

```
EVALUATION SUMMARY
================================================================================

Final Score:
  MAE: 0.847 | RMSE: 1.123 | R²: 0.756
  Correlation: 0.872
  Within 1 point: 68.0% | Within 2 points: 92.0%

Tone Score:
  MAE: 0.623 | RMSE: 0.891 | R²: 0.812
  Correlation: 0.901
  Within 1 point: 78.0% | Within 2 points: 96.0%
```

## Interpreting Results

### Good Performance Indicators
- MAE < 1.0 for all dimensions
- R² > 0.7 for final score
- Correlation > 0.8
- >75% agreement within 1 point

### Areas for Improvement
- High MAE in specific dimensions (e.g., empathy)
- Low correlation in certain score types
- Systematic bias (agent consistently higher/lower)
- Large disagreements on specific ticket types

### Using Disagreements for Training
1. Review top disagreements in the report
2. Analyze human vs agent reasoning
3. Identify patterns in disagreements
4. Use as training examples for DSPy optimization
5. Update prompts based on insights

## Advanced Usage

### Custom Paths

Modify the script to use custom file paths:

```python
evaluator = QCEvaluator(
    excel_path='path/to/human_scores.xlsx',
    json_path='path/to/agent_scores.json'
)
```

### Programmatic Access

Use the evaluator in your own scripts:

```python
from scripts.evaluate_qc_results import QCEvaluator

evaluator = QCEvaluator('Ticket_QC.xlsx', 'results.json')
evaluator.load_data()
evaluator.merge_data()

# Get metrics
metrics = evaluator.calculate_metrics()
print(f"Final Score MAE: {metrics['Final Score']['MAE']:.3f}")

# Access merged data
df = evaluator.merged_data
high_disagreements = df[df['final_diff'] > 2]
```

## Troubleshooting

### Issue: "No matching tickets found"
- **Cause**: Ticket IDs don't match between files
- **Solution**: Ensure `Ticket_Number` in Excel matches `chat_id` in JSON

### Issue: "Column not found"
- **Cause**: Excel column names have extra spaces or different names
- **Solution**: Check column names in Excel, update `column_mapping` in script

### Issue: "Invalid score values"
- **Cause**: Non-numeric scores in data
- **Solution**: Clean data, ensure all scores are numeric (0-10)

### Issue: "Missing dependencies"
- **Cause**: Required packages not installed
- **Solution**: Run `pip install -r requirements_evaluation.txt`

## Integration with DSPy Optimization

Use evaluation results to improve agent performance:

1. **Identify weak dimensions**: Focus on scores with high MAE
2. **Extract training examples**: Use disagreements as training data
3. **Update prompts**: Refine based on reasoning differences
4. **Re-evaluate**: Run evaluation after each optimization iteration
5. **Track progress**: Compare metrics across versions

See [`DSPY_OPTIMIZATION_GUIDE.md`](DSPY_OPTIMIZATION_GUIDE.md) for optimization details.

## Best Practices

1. **Regular Evaluation**: Run after each model update
2. **Version Control**: Save reports with timestamps
3. **Track Metrics**: Monitor MAE and correlation trends
4. **Review Disagreements**: Manually review top 10-20 disagreements
5. **Balanced Dataset**: Ensure diverse ticket types in evaluation set
6. **Statistical Significance**: Use sufficient sample size (>50 tickets)

## Related Documentation

- [DSPy Optimization Guide](DSPY_OPTIMIZATION_GUIDE.md)
- [QC Agent Configuration](../agent_config/qc_agent.yml)
- [Training Data Format](../data/training/README.md)