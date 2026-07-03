"""
Excel to DSPy Training Data Converter

This script converts Excel training data to the JSON format required by DSPy.
It reads the Excel file with live chat conversations and expert ratings,
then outputs a JSON file compatible with the DSPy optimization workflow.

Usage:
    python scripts/convert_excel_to_dspy_training.py --input data/training/sample_chat_to_improve_qc.xlsx --output data/training/qc_training_from_excel.json
    python scripts/convert_excel_to_dspy_training.py --inspect  # Just inspect the Excel structure
"""

import argparse
import json
import logging
import re
import sys
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, List, Optional

import pandas as pd

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)


# Column mapping for the QC Excel file (Persian column names to DSPy fields)
QC_COLUMN_MAPPING = {
    "متن چت": "chat_conversation",  # Chat transcript
    "موضوع چت": "main_problem",  # Chat topic/main issue
    "توضیحات": "reasons",  # Expert explanation/reasons
    "امتیاز نهایی": "score",  # Final score
    "لحن": "tone_score",  # Tone score
    "همدلی و ایجاد اطمینان": "empathy_score",  # Empathy score
    "تشخیص و حل مسئله": "solution_quality",  # Problem solution score
    "وضوح و شفافیت": "clarity_score",  # Clarity score
}

# Error columns to combine into key_observations
ERROR_COLUMNS = [
    "خطای شناسایی شده ۱",
    "خطای شناسایی شده ۲",
    "خطای شناسایی شده ۳",
]


def inspect_excel_file(file_path: Path) -> Dict[str, Any]:
    """
    Inspect the Excel file structure and return information about it.

    Args:
        file_path: Path to the Excel file

    Returns:
        Dictionary with file structure information
    """
    logger.info(f"Inspecting Excel file: {file_path}")

    # Read all sheets
    excel_file = pd.ExcelFile(file_path)
    sheet_names = excel_file.sheet_names

    info = {"file_path": str(file_path), "sheet_names": sheet_names, "sheets": {}}

    for sheet_name in sheet_names:
        df = pd.read_excel(file_path, sheet_name=sheet_name)

        sheet_info = {
            "columns": list(df.columns),
            "row_count": len(df),
            "column_types": {col: str(df[col].dtype) for col in df.columns},
            "sample_values": {},
        }

        # Get sample values for each column (first non-null value)
        for col in df.columns:
            non_null = df[col].dropna()
            if len(non_null) > 0:
                sample = non_null.iloc[0]
                # Convert to string for display, truncate if too long
                sample_str = str(sample)
                if len(sample_str) > 200:
                    sample_str = sample_str[:200] + "..."
                sheet_info["sample_values"][col] = sample_str

        info["sheets"][sheet_name] = sheet_info

    return info


def parse_chat_transcript(transcript: str) -> List[Dict[str, Any]]:
    """
    Parse a LiveChat transcript into structured message format.

    The transcript format is:
    LiveChat conversation transcript:
    ----------
    نام و نام خانوادگی ...
    انتخاب موضوع ...
    ----------
    AgentName (Mon, 12/8/2025, 03:02:28 pm Asia/Tehran)
    Message content...

    CustomerName (Mon, 12/8/2025, 03:03:00 pm Asia/Tehran)
    Message content...

    Args:
        transcript: Raw transcript string

    Returns:
        List of message dictionaries with role, message, timestamp, and sender
    """
    if not transcript or not isinstance(transcript, str):
        return []

    messages = []

    # Pattern to match message headers: Name (Day, Date, Time Timezone)
    # Example: امیرمحمد (Mon, 12/8/2025, 03:02:28 pm Asia/Tehran)
    header_pattern = (
        r"^(.+?)\s*\((\w+,\s*\d+/\d+/\d+,\s*\d+:\d+:\d+\s*[ap]m\s*\w+/\w+)\)\s*$"
    )

    lines = transcript.split("\n")
    current_sender = None
    current_timestamp = None
    current_message_lines = []

    # Parse messages
    for line in lines:
        line = line.strip()

        # Skip empty lines and separator lines
        if not line or line == "----------":
            continue

        # Skip header lines
        if line.startswith("LiveChat conversation transcript"):
            continue
        if line.startswith("نام و نام خانوادگی"):
            continue
        if line.startswith("انتخاب موضوع"):
            continue

        # Check if this is a message header
        match = re.match(header_pattern, line, re.IGNORECASE)
        if match:
            # Save previous message if exists
            if current_sender and current_message_lines:
                message_text = "\n".join(current_message_lines).strip()
                if message_text:
                    messages.append({"message": message_text})

            # Start new message
            current_sender = match.group(1).strip()
            current_timestamp = match.group(2).strip()
            current_message_lines = []
        else:
            # This is message content
            if current_sender:
                current_message_lines.append(line)

    # Don't forget the last message
    if current_sender and current_message_lines:
        message_text = "\n".join(current_message_lines).strip()
        if message_text:
            messages.append({"message": message_text})

    return messages


def print_inspection_results(info: Dict[str, Any]) -> None:
    """Print inspection results in a readable format."""
    print("\n" + "=" * 80)
    print(f"Excel File: {info['file_path']}")
    print(f"Sheets: {', '.join(info['sheet_names'])}")
    print("=" * 80)

    for sheet_name, sheet_info in info["sheets"].items():
        print(f"\n📊 Sheet: {sheet_name}")
        print(f"   Rows: {sheet_info['row_count']}")
        print(f"   Columns ({len(sheet_info['columns'])}):")

        for col in sheet_info["columns"]:
            col_type = sheet_info["column_types"][col]
            sample = sheet_info["sample_values"].get(col, "N/A")
            print(f"      - {col} ({col_type})")
            print(f"        Sample: {sample}")
        print()


def convert_excel_to_dspy_format(
    file_path: Path,
    sheet_name: Optional[str] = None,
    column_mapping: Optional[Dict[str, str]] = None,
    parse_transcripts: bool = True,
) -> Dict[str, Any]:
    """
    Convert Excel data to DSPy training format.

    Args:
        file_path: Path to the Excel file
        sheet_name: Name of the sheet to read (default: first sheet)
        column_mapping: Mapping from Excel columns to DSPy fields
                       e.g., {"conversation_column": "chat_conversation", "score_column": "score"}
        parse_transcripts: Whether to parse chat transcripts into structured format

    Returns:
        Dictionary in DSPy training format
    """
    logger.info(f"Converting Excel file: {file_path}")

    # Read the Excel file
    if sheet_name:
        df = pd.read_excel(file_path, sheet_name=sheet_name)
    else:
        df = pd.read_excel(file_path)

    logger.info(f"Read {len(df)} rows from Excel")

    # Use QC column mapping if no custom mapping provided
    if column_mapping is None:
        # Check if this looks like a QC Excel file
        if "متن چت" in df.columns:
            column_mapping = QC_COLUMN_MAPPING
            logger.info("Using QC column mapping (Persian columns detected)")
        else:
            column_mapping = auto_detect_column_mapping(df)
            logger.info(f"Auto-detected column mapping: {column_mapping}")

    # Convert each row to an example
    examples = []
    skipped = 0

    for idx, row in df.iterrows():
        example = {}

        # Process main column mapping
        for excel_col, dspy_field in column_mapping.items():
            if excel_col in df.columns:
                value = row[excel_col]

                # Handle NaN values
                if pd.isna(value):
                    continue

                # Convert to appropriate type based on field
                if dspy_field == "chat_conversation":
                    if parse_transcripts and isinstance(value, str):
                        # Parse the transcript into structured format
                        parsed = parse_chat_transcript(value)
                        if parsed:
                            value = parsed
                        else:
                            # Keep as string if parsing fails
                            value = value
                    elif isinstance(value, str):
                        # Try to parse as JSON if it's already structured
                        try:
                            value = json.loads(value)
                        except json.JSONDecodeError:
                            pass

                elif dspy_field in [
                    "score",
                    "empathy_score",
                    "tone_score",
                    "solution_quality",
                    "clarity_score",
                ]:
                    # Convert to float for scores
                    try:
                        value = float(value)
                    except (ValueError, TypeError):
                        pass

                elif dspy_field == "reasons":
                    # Keep as string for expert explanations
                    value = str(value).strip()

                example[dspy_field] = value

        # Process error columns into key_observations
        key_observations = []
        for error_col in ERROR_COLUMNS:
            if error_col in df.columns:
                error_value = row[error_col]
                if not pd.isna(error_value) and str(error_value).strip():
                    key_observations.append(str(error_value).strip())

        if key_observations:
            example["key_observations"] = key_observations

        # Validate example has required fields
        if "chat_conversation" in example and example["chat_conversation"]:
            examples.append(example)
        else:
            skipped += 1
            logger.debug(f"Skipped row {idx}: missing chat_conversation")

    if skipped > 0:
        logger.warning(f"Skipped {skipped} rows due to missing chat_conversation")

    # Create the output structure
    output = {
        "dataset_name": f"qc_training_from_excel_{datetime.now().strftime('%Y%m%d')}",
        "created_at": datetime.now().isoformat() + "Z",
        "source_file": str(file_path),
        "total_rows": len(df),
        "converted_examples": len(examples),
        "skipped_rows": skipped,
        "examples": examples,
    }

    logger.info(f"Converted {len(examples)} examples (skipped {skipped})")
    return output


def auto_detect_column_mapping(df: pd.DataFrame) -> Dict[str, str]:
    """
    Auto-detect column mapping based on column names.

    Args:
        df: DataFrame to analyze

    Returns:
        Dictionary mapping Excel columns to DSPy fields
    """
    mapping = {}
    columns_lower = {col.lower(): col for col in df.columns}

    # Common patterns for chat conversation columns
    conversation_patterns = [
        "chat",
        "conversation",
        "messages",
        "dialog",
        "dialogue",
        "گفتگو",
        "مکالمه",
    ]
    for pattern in conversation_patterns:
        for col_lower, col in columns_lower.items():
            if pattern in col_lower:
                mapping[col] = "chat_conversation"
                break
        if "chat_conversation" in mapping.values():
            break

    # Common patterns for score columns
    score_patterns = ["score", "rate", "rating", "امتیاز", "نمره"]
    for pattern in score_patterns:
        for col_lower, col in columns_lower.items():
            if pattern in col_lower and col not in mapping:
                mapping[col] = "score"
                break
        if "score" in mapping.values():
            break

    # Common patterns for explanation/reason columns
    reason_patterns = ["reason", "explanation", "comment", "توضیح", "دلیل", "نظر"]
    for pattern in reason_patterns:
        for col_lower, col in columns_lower.items():
            if pattern in col_lower and col not in mapping:
                mapping[col] = "reasons"
                break
        if "reasons" in mapping.values():
            break

    # Common patterns for problem/issue columns
    problem_patterns = ["problem", "issue", "main", "مشکل", "موضوع"]
    for pattern in problem_patterns:
        for col_lower, col in columns_lower.items():
            if pattern in col_lower and col not in mapping:
                mapping[col] = "main_problem"
                break
        if "main_problem" in mapping.values():
            break

    return mapping


def save_training_data(data: Dict[str, Any], output_path: Path) -> None:
    """Save training data to JSON file."""
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    logger.info(f"Saved training data to: {output_path}")


def main():
    parser = argparse.ArgumentParser(
        description="Convert Excel training data to DSPy JSON format"
    )
    parser.add_argument(
        "--input",
        "-i",
        type=str,
        default="data/training/sample_chat_to_improve_qc.xlsx",
        help="Path to input Excel file",
    )
    parser.add_argument(
        "--output",
        "-o",
        type=str,
        default="data/training/qc_training_from_excel.json",
        help="Path to output JSON file",
    )
    parser.add_argument(
        "--sheet",
        "-s",
        type=str,
        default=None,
        help="Sheet name to read (default: first sheet)",
    )
    parser.add_argument(
        "--inspect",
        action="store_true",
        help="Only inspect the Excel file structure, don't convert",
    )
    parser.add_argument(
        "--mapping",
        type=str,
        default=None,
        help='JSON string with column mapping, e.g., \'{"Col1": "chat_conversation", "Col2": "score"}\'',
    )

    args = parser.parse_args()

    input_path = Path(args.input)

    if not input_path.exists():
        logger.error(f"Input file not found: {input_path}")
        sys.exit(1)

    # Inspect mode
    if args.inspect:
        info = inspect_excel_file(input_path)
        print_inspection_results(info)

        # Also save inspection results to JSON for reference
        inspect_output = input_path.with_suffix(".inspection.json")
        with open(inspect_output, "w", encoding="utf-8") as f:
            json.dump(info, f, ensure_ascii=False, indent=2)
        logger.info(f"Inspection results saved to: {inspect_output}")
        return

    # Parse column mapping if provided
    column_mapping = None
    if args.mapping:
        try:
            column_mapping = json.loads(args.mapping)
        except json.JSONDecodeError as e:
            logger.error(f"Invalid JSON in --mapping: {e}")
            sys.exit(1)

    # Convert Excel to DSPy format
    data = convert_excel_to_dspy_format(
        input_path, sheet_name=args.sheet, column_mapping=column_mapping
    )

    # Save output
    output_path = Path(args.output)
    save_training_data(data, output_path)

    # Print summary
    print("\n" + "=" * 80)
    print("Conversion Complete!")
    print("=" * 80)
    print(f"Input:  {input_path}")
    print(f"Output: {output_path}")
    print(f"Examples converted: {len(data['examples'])}")

    if data["examples"]:
        print("\nSample example fields:")
        for field in data["examples"][0].keys():
            print(f"  - {field}")


if __name__ == "__main__":
    main()
