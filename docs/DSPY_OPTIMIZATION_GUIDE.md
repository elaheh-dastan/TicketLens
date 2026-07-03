# DSPy Optimization Guide for QC Agent

This guide documents how to use human-scored Excel data to optimize DSPy prompts and evaluate customer support conversations.

---

## TL;DR - Quick Commands

```bash
# 1. Inspect your Excel file structure
uv run python scripts/convert_excel_to_dspy_training.py --input ~/Downloads/your_file.xlsx --inspect

# 2. Convert Excel to DSPy training format
uv run python scripts/convert_excel_to_dspy_training.py \
  --input ~/Downloads/sample_chat_to_improve_qc.xlsx \
  --output data/training/qc_training_from_excel.json

# 3. Optimize chat_analyzer node (MIPRO + partial_match)
uv run python scripts/optimize_dspy.py \
  --agent qc_agent_dspy \
  --node chat_analyzer \
  --training-data data/training/qc_training_from_excel.json \
  --output data/optimized_prompts \
  --keep-last

# 4. Optimize score_generator node (MIPRO + multi_score_proximity)
uv run python scripts/optimize_dspy.py \
  --agent qc_agent_dspy \
  --node score_generator \
  --training-data data/training/qc_training_from_excel.json \
  --output data/optimized_prompts \
  --keep-last

# 5. Evaluate conversations from CSV
uv run python scripts/qc_sample_data.py \
  --input ~/Downloads/masked_conversation.csv \
  --limit 10

# 6. Start the API server
uv run uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000
```

---

## Overview

The QC Agent uses DSPy (Declarative Self-improving Language Programs) to:

1. **Analyze chat conversations** - Identify the main problem
2. **Generate quality scores** - Rate agent performance with reasoning

By training on human-scored examples, the model learns to mimic expert QC evaluations.

---

## Step 1: Prepare Your Excel Training Data

### Required Columns

Your Excel file should contain these columns (Persian or English):

| Persian Column        | English Mapping    | Description                 |
| --------------------- | ------------------ | --------------------------- |
| متن چت                | chat_conversation  | Full chat transcript        |
| موضوع چت              | main_problem       | Main issue identified       |
| توضیحات               | reasons            | Expert explanation/feedback |
| امتیاز نهایی          | score              | Final score (1-10)          |
| لحن                   | tone_score         | Tone score                  |
| همدلی و ایجاد اطمینان | empathy_score      | Empathy score               |
| تشخیص و حل مسئله      | resolution_quality | Problem resolution score    |
| وضوح و شفافیت         | clarity_score      | Clarity score               |
| خطای شناسایی شده ۱-۳  | key_observations   | Identified errors           |

### Inspect Your Excel Structure

```bash
uv run python scripts/convert_excel_to_dspy_training.py \
  --input ~/Downloads/your_file.xlsx \
  --inspect
```

This shows all columns, data types, and sample values.

---

## Step 2: Convert Excel to DSPy Training Format

### Basic Conversion (Auto-detect Persian columns)

```bash
uv run python scripts/convert_excel_to_dspy_training.py \
  --input ~/Downloads/sample_chat_to_improve_qc.xlsx \
  --output data/training/qc_training_from_excel.json
```

### Custom Column Mapping

If your columns don't match the defaults:

```bash
uv run python scripts/convert_excel_to_dspy_training.py \
  --input ~/Downloads/your_file.xlsx \
  --output data/training/my_training.json \
  --mapping '{"YourChatColumn": "chat_conversation", "YourScoreColumn": "score"}'
```

### Output Format

The script generates a JSON file with this structure:

```json
{
  "dataset_name": "qc_training_from_excel_20260131",
  "created_at": "2026-01-31T12:00:00Z",
  "total_rows": 48,
  "converted_examples": 48,
  "examples": [
    {
      "chat_conversation": [...],
      "main_problem": "لود نشدن سایت",
      "score": 2.5,
      "reasons": "هیچ راه حل مناسبی به کاربر نداد",
      "key_observations": ["عدم توانایی در تشخیص مسئله"]
    }
  ]
}
```

---

## Step 3: Run DSPy Optimization

### Optimize Chat Analyzer Node

Uses **MIPRO** teleprompter with `partial_match` metric:

```bash
uv run python scripts/optimize_dspy.py \
  --agent qc_agent_dspy \
  --node chat_analyzer \
  --training-data data/training/qc_training_from_excel.json \
  --output data/optimized_prompts \
  --keep-last
```

### Optimize Score Generator Node

Uses **MIPRO** teleprompter with `multi_score_proximity` metric (≥60% of scores within ±1.5 tolerance + reasons word overlap):

```bash
uv run python scripts/optimize_dspy.py \
  --agent qc_agent_dspy \
  --node score_generator \
  --training-data data/training/qc_training_from_excel.json \
  --output data/optimized_prompts \
  --keep-last
```

### Available Metrics

| Metric                  | Use Case                    | Description                                                      |
| ----------------------- | --------------------------- | ---------------------------------------------------------------- |
| `exact_match`           | Categorical outputs         | Requires exact string match                                      |
| `partial_match`         | Free-text fields            | Checks word overlap between expected/predicted                   |
| `score_proximity`       | Single numeric score        | Allows ±1.5 point tolerance on `score` field                     |
| `multi_score_proximity` | Multiple scores + text      | ≥60% of scores within ±1.5 tolerance AND reasons have word overlap |

### Available Teleprompters

| Teleprompter       | Best For           | Description                                           |
| ------------------ | ------------------ | ----------------------------------------------------- |
| `BootstrapFewShot` | Quick optimization | Bootstraps successful examples as few-shots           |
| `MIPRO`            | Better quality     | Uses Bayesian optimization for instruction + examples |
| `COPRO`            | Instruction tuning | Focuses on prompt instruction optimization            |

---

## Step 4: Verify Optimized Prompts

Check the generated files:

```bash
ls -la data/optimized_prompts/
```

Expected output:

```
chat_analyzer.json       # Optimized chat analysis prompts
chat_analyzer.last.json  # Backup of previous version
score_generator.json     # Optimized scoring prompts
```

The prompts are **automatically loaded** when `use_optimized: true` in the agent config.

---

## Step 5: Evaluate New Conversations

### From CSV Files

```bash
uv run python scripts/qc_sample_data.py \
  --input ~/Downloads/masked_conversation.csv \
  --limit 5 \
  --output results.json
```

### CSV Format Expected

```csv
Ticket_Number,category,Conversation_History
990981180,orders_and_matches,"[{""sender"": ""USER"", ""message"": ""...""}]"
```

### Output Example

```
📋 Ticket: 990981180 | Category: orders_and_matches

💬 Conversation Summary:
   👤 CUSTOMER: سلام وقت به خیر در مورد نقره دیجیتال...
   🧑‍💼 AGENT: سلام وقت بخیر توی بخش آسان...

📊 QC Analysis:
   🎯 Main Problem: مشتری فاصله قیمت خرید/فروش را نامنصفانه می‌داند
   ⭐ Score: 5
   📝 Reasons:
      1. Incomplete Problem Resolution
      2. Lack of empathy
      3. Did not address customer's specific request
```

---

## Step 6: Start the API Server

```bash
uv run uvicorn src.api.server:app --reload --host 0.0.0.0 --port 8000
```

### API Endpoint

**POST** `/api/v2/qc/evaluate`

```json
{
  "chat_id": "12345",
  "chat_conversation": [
    {
      "role": "customer",
      "message": "سلام",
      "timestamp": "2026-01-31T10:00:00"
    },
    {
      "role": "agent",
      "message": "سلام وقت بخیر",
      "timestamp": "2026-01-31T10:01:00"
    }
  ]
}
```

---

## Optimization Results Summary

From training on 48 human-scored examples:

| Node              | Metric          | Score   |
| ----------------- | --------------- | ------- |
| `chat_analyzer`   | partial_match   | **50%** |
| `score_generator` | score_proximity | **70%** |

---

## Sample Evaluation Results

### General Support Conversations

| Ticket    | Category           | Score | Key Issues                                                |
| --------- | ------------------ | ----- | --------------------------------------------------------- |
| 990981180 | orders_and_matches | 5/10  | ❌ Incomplete resolution, ❌ Lack of empathy              |
| 226472757 | deposit_crypto     | 7/10  | ✅ Clear instructions, ⚠️ Could be more beginner-friendly |
| 703442646 | other              | 5/10  | ❌ Confusing time formats, ❌ Didn't acknowledge urgency  |

### Card Deposit Conversations

| Ticket    | Category             | Score | Key Issues                                     |
| --------- | -------------------- | ----- | ---------------------------------------------- |
| 315494184 | card_to_card_deposit | 7/10  | ✅ Clear info, ⚠️ No personalized status check |
| 750603616 | card_to_card_deposit | 8/10  | ✅ Informative, ✅ Reassuring                  |
| 142045310 | card_to_card_deposit | 5/10  | ❌ No empathy, ❌ Didn't address delay         |

### Common Patterns Identified

**Strengths:**

- Clear explanations of processes
- Professional tone
- Quick responses

**Areas for Improvement:**

- **Empathy**: Agents often don't acknowledge customer frustration
- **Personalization**: Generic responses instead of checking specific status
- **Follow-up**: No proactive escalation when issues persist

---

## Configuration Reference

### Agent Config: `agent_config/qc_agent_dspy.yml`

```yaml
agent:
  agent_name: "QC_Agent_DSPy"

  graph:
    entry_point: "input_validator"

    nodes:
      chat_analyzer:
        type: "dspy"
        dspy:
          enabled: true
          module: "ChainOfThought"
          signature:
            input_fields:
              chat_conversation: "Complete customer support chat..."
            output_fields:
              main_problem: "The main problem the customer is experiencing..."
          optimization:
            teleprompter: "MIPRO"
            metric: "partial_match"
            training_data_path: "data/training/qc_training_from_excel.json"
        next: "score_generator"

      score_generator:
        type: "dspy"
        dspy:
          enabled: true
          module: "ChainOfThought"
          signature:
            input_fields:
              chat_conversation: "..."
              main_problem: "..."
            output_fields:
              score: "Overall quality score from 1-10"
              tone_score: "Tone score from 1-10"
              empathy_score: "Empathy score from 1-10"
              solution_quality: "Solution quality score from 1-10"
              clarity_score: "Clarity score from 1-10"
              reasons: "List of 3-5 main reasons for the score"
          optimization:
            teleprompter: "MIPRO"
            metric: "multi_score_proximity"
        next: "__end__"
```

---

## Troubleshooting

### "No optimized prompts found"

The agent will fall back to baseline prompts. Run optimization first:

```bash
uv run python scripts/optimize_dspy.py --agent qc_agent_dspy --node chat_analyzer ...
```

### "0% metric during optimization"

The metric is too strict. Use:

- `partial_match` for text fields
- `score_proximity` for numeric scores

### "BootstrapFewShot.compile() got unexpected argument 'valset'"

Fixed in the updated script. BootstrapFewShot only takes `trainset`.

---

## Files Reference

| File                                        | Purpose                           |
| ------------------------------------------- | --------------------------------- |
| `scripts/convert_excel_to_dspy_training.py` | Convert Excel to DSPy JSON format |
| `scripts/optimize_dspy.py`                  | Run DSPy optimization             |
| `scripts/evaluate_csv.py`                   | Evaluate CSV conversations        |
| `agent_config/qc_agent_dspy.yml`            | Agent configuration               |
| `data/training/qc_training_from_excel.json` | Training data                     |
| `data/optimized_prompts/`                   | Optimized prompt storage          |






