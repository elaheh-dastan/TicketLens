"""
QC Agent Evaluation Script

Compares human scoring from Ticket_QC.xlsx with agent scoring from results.json
and generates comprehensive evaluation metrics.
"""

import argparse
import json
import sys
import pandas as pd
import numpy as np
from pathlib import Path
from typing import Dict
from datetime import datetime
from sklearn.metrics import mean_absolute_error, mean_squared_error, r2_score
import matplotlib.pyplot as plt
import seaborn as sns


SCORE_COLUMNS = ['tone_score', 'empathy_score', 'solution_quality', 'clarity_score', 'final_score']


class QCEvaluator:
    """Evaluates QC agent performance against human scoring."""

    def __init__(self, excel_path: str, json_path: str, demo_report_path: str | None = None):
        self.excel_path = Path(excel_path)
        self.json_path = Path(json_path)
        self.demo_report_path = Path(demo_report_path) if demo_report_path else None
        self.human_scores: pd.DataFrame | None = None
        self.agent_scores: pd.DataFrame | None = None
        self.merged_data: pd.DataFrame | None = None
        self.conversations: pd.DataFrame | None = None

    def load_data(self) -> None:
        """Load and prepare data from both sources."""
        self._load_human_scores()
        self._load_agent_scores()
        self._load_conversations()
        print(f"Loaded {len(self.human_scores)} human scores and {len(self.agent_scores)} agent scores")

    def _load_human_scores(self) -> None:
        """Load human scores from Excel."""
        if not self.excel_path.exists():
            raise FileNotFoundError(f"Excel file not found: {self.excel_path}")

        print(f"Loading human scores from {self.excel_path}...")
        self.human_scores = pd.read_excel(self.excel_path)

        # Clean column names (remove leading/trailing spaces) BEFORE mapping
        self.human_scores.columns = self.human_scores.columns.str.strip()

        # Rename columns for consistency (keys must match already-stripped names)
        column_mapping = {
            'Ticket_Number': 'ticket_id',
        }
        self.human_scores.rename(columns=column_mapping, inplace=True)

        if 'ticket_id' not in self.human_scores.columns:
            raise ValueError(
                f"Excel file must contain a 'Ticket_Number' column. "
                f"Found columns: {list(self.human_scores.columns)}"
            )

        missing = [c for c in SCORE_COLUMNS if c not in self.human_scores.columns]
        if missing:
            raise ValueError(f"Excel file missing score columns: {missing}")

        # Excel reads numeric IDs as float (e.g. 575755106.0) — convert to int first
        # to avoid ".0" suffix when casting to string
        self.human_scores['ticket_id'] = (
            pd.to_numeric(self.human_scores['ticket_id'], errors='coerce')
            .astype('Int64')
            .astype(str)
            .str.strip()
        )

        # Coerce score columns to numeric
        for col in SCORE_COLUMNS:
            self.human_scores[col] = pd.to_numeric(self.human_scores[col], errors='coerce')

    def _load_conversations(self) -> None:
        """Load conversation history from demo_report.xlsx."""
        if self.demo_report_path is None or not self.demo_report_path.exists():
            self.conversations = None
            return

        print(f"Loading conversations from {self.demo_report_path}...")
        df = pd.read_excel(self.demo_report_path)
        df.columns = df.columns.str.strip()

        if 'Ticket_Number' not in df.columns or 'Conversation_History' not in df.columns:
            print(f"  Warning: demo_report missing required columns. Found: {list(df.columns)}")
            self.conversations = None
            return

        df['ticket_id'] = (
            pd.to_numeric(df['Ticket_Number'], errors='coerce')
            .astype('Int64')
            .astype(str)
            .str.strip()
        )
        self.conversations = df[['ticket_id', 'Conversation_History']].copy()
        print(f"  Loaded {len(self.conversations)} conversations")

    def _load_agent_scores(self) -> None:
        """Load agent scores from JSON."""
        if not self.json_path.exists():
            raise FileNotFoundError(f"JSON file not found: {self.json_path}")

        print(f"Loading agent scores from {self.json_path}...")
        with open(self.json_path, 'r', encoding='utf-8') as f:
            agent_data = json.load(f)

        if 'results' not in agent_data or not agent_data['results']:
            raise ValueError("JSON file must contain a non-empty 'results' array")

        agent_results = []
        for i, result in enumerate(agent_data['results']):
            try:
                agent_results.append({
                    'ticket_id': str(result['chat_id']).strip(),
                    'agent_tone_score': float(result['tone_score']),
                    'agent_empathy_score': float(result['empathy_score']),
                    'agent_solution_quality': float(result['solution_quality']),
                    'agent_clarity_score': float(result['clarity_score']),
                    'agent_final_score': float(result['score']),
                    'agent_main_problem': str(result.get('main_problem', '')),
                    'agent_reasons': str(result.get('reasons', '')),
                })
            except (KeyError, ValueError) as e:
                print(f"  Warning: skipping result #{i} (chat_id={result.get('chat_id', '?')}): {e}")

        if not agent_results:
            raise ValueError("No valid agent results could be parsed from JSON")

        self.agent_scores = pd.DataFrame(agent_results)
        
    def merge_data(self) -> pd.DataFrame:
        """Merge human and agent scores on ticket_id."""
        if self.human_scores is None or self.agent_scores is None:
            raise RuntimeError("Call load_data() before merge_data()")

        print("Merging human and agent scores...")

        self.merged_data = pd.merge(
            self.human_scores,
            self.agent_scores,
            on='ticket_id',
            how='inner',
        )

        if self.merged_data.empty:
            # Show sample IDs to help the user debug
            human_sample = list(self.human_scores['ticket_id'].head(5))
            agent_sample = list(self.agent_scores['ticket_id'].head(5))
            raise ValueError(
                f"No matching tickets found between Excel and JSON.\n"
                f"  Sample human ticket_ids: {human_sample}\n"
                f"  Sample agent ticket_ids: {agent_sample}"
            )

        # Drop rows where any score is NaN (from coercion or missing data)
        agent_cols = ['agent_tone_score', 'agent_empathy_score', 'agent_solution_quality',
                      'agent_clarity_score', 'agent_final_score']
        all_score_cols = SCORE_COLUMNS + agent_cols
        before = len(self.merged_data)
        self.merged_data.dropna(subset=all_score_cols, inplace=True)
        dropped = before - len(self.merged_data)
        if dropped:
            print(f"  Dropped {dropped} rows with missing score values")

        print(f"Successfully merged {len(self.merged_data)} matching tickets")

        # Calculate absolute differences
        diff_pairs = [
            ('tone_diff', 'tone_score', 'agent_tone_score'),
            ('empathy_diff', 'empathy_score', 'agent_empathy_score'),
            ('solution_diff', 'solution_quality', 'agent_solution_quality'),
            ('clarity_diff', 'clarity_score', 'agent_clarity_score'),
            ('final_diff', 'final_score', 'agent_final_score'),
        ]
        for diff_col, human_col, agent_col in diff_pairs:
            self.merged_data[diff_col] = (self.merged_data[human_col] - self.merged_data[agent_col]).abs()

        # Join conversation history if available
        if self.conversations is not None:
            self.merged_data = pd.merge(
                self.merged_data,
                self.conversations,
                on='ticket_id',
                how='left',
            )

        return self.merged_data
    
    def calculate_metrics(self) -> Dict[str, Dict[str, float]]:
        """Calculate evaluation metrics for each score dimension."""
        if self.merged_data is None:
            self.merge_data()

        score_pairs = [
            ('tone_score', 'agent_tone_score', 'Tone'),
            ('empathy_score', 'agent_empathy_score', 'Empathy'),
            ('solution_quality', 'agent_solution_quality', 'Solution Quality'),
            ('clarity_score', 'agent_clarity_score', 'Clarity'),
            ('final_score', 'agent_final_score', 'Final Score'),
        ]

        metrics = {}
        for human_col, agent_col, label in score_pairs:
            human_vals = self.merged_data[human_col].values.astype(float)
            agent_vals = self.merged_data[agent_col].values.astype(float)
            diff = np.abs(human_vals - agent_vals)

            # Correlation: guard against constant arrays (std=0)
            if np.std(human_vals) == 0 or np.std(agent_vals) == 0:
                corr = float('nan')
            else:
                corr = np.corrcoef(human_vals, agent_vals)[0, 1]

            metrics[label] = {
                'MAE': mean_absolute_error(human_vals, agent_vals),
                'RMSE': np.sqrt(mean_squared_error(human_vals, agent_vals)),
                'R²': r2_score(human_vals, agent_vals) if np.std(human_vals) > 0 else float('nan'),
                'Correlation': corr,
                'Mean Human': np.mean(human_vals),
                'Mean Agent': np.mean(agent_vals),
                'Std Human': np.std(human_vals),
                'Std Agent': np.std(agent_vals),
                'Mean Bias': np.mean(agent_vals - human_vals),
                'Within 1 point': np.mean(diff <= 1) * 100,
                'Within 2 points': np.mean(diff <= 2) * 100,
            }

        return metrics
    
    def generate_report(self, output_path: str = 'evaluation_report.txt') -> None:
        """Generate comprehensive evaluation report."""
        if self.merged_data is None:
            self.merge_data()
        
        metrics = self.calculate_metrics()
        
        with open(output_path, 'w', encoding='utf-8') as f:
            f.write("=" * 80 + "\n")
            f.write("QC AGENT EVALUATION REPORT\n")
            f.write("=" * 80 + "\n\n")
            
            f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            f.write(f"Human Scores: {self.excel_path}\n")
            f.write(f"Agent Scores: {self.json_path}\n")
            f.write(f"Matched Tickets: {len(self.merged_data)}\n\n")
            
            f.write("=" * 80 + "\n")
            f.write("OVERALL METRICS\n")
            f.write("=" * 80 + "\n\n")
            
            for dimension, values in metrics.items():
                f.write(f"\n{dimension}:\n")
                f.write("-" * 40 + "\n")
                f.write(f"  Mean Absolute Error (MAE):      {values['MAE']:.3f}\n")
                f.write(f"  Root Mean Squared Error (RMSE): {values['RMSE']:.3f}\n")
                f.write(f"  R² Score:                       {values['R²']:.3f}\n")
                f.write(f"  Correlation:                    {values['Correlation']:.3f}\n")
                f.write(f"  Mean Human Score:               {values['Mean Human']:.2f}\n")
                f.write(f"  Mean Agent Score:               {values['Mean Agent']:.2f}\n")
                f.write(f"  Mean Bias (Agent - Human):      {values['Mean Bias']:+.2f}\n")
                f.write(f"  Std Human Score:                {values['Std Human']:.2f}\n")
                f.write(f"  Std Agent Score:                {values['Std Agent']:.2f}\n")
                f.write(f"  Within 1 point:                 {values['Within 1 point']:.1f}%\n")
                f.write(f"  Within 2 points:                {values['Within 2 points']:.1f}%\n")
            
            # Top disagreements
            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 10 DISAGREEMENTS (by Final Score)\n")
            f.write("=" * 80 + "\n\n")
            
            top_disagreements = self.merged_data.nlargest(10, 'final_diff')
            for _, row in top_disagreements.iterrows():
                f.write(f"\nTicket ID: {row['ticket_id']}\n")
                f.write(f"  Human Final Score:  {row['final_score']:.1f}\n")
                f.write(f"  Agent Final Score:  {row['agent_final_score']:.1f}\n")
                f.write(f"  Difference:         {row['final_diff']:.1f}\n")
                human_reason = row.get('reason') if 'reason' in row.index else 'N/A'
                if pd.isna(human_reason):
                    human_reason = 'N/A'
                f.write(f"  Human Reason:       {human_reason}\n")
                agent_reason = str(row.get('agent_reasons', ''))
                f.write(f"  Agent Reason:       {agent_reason[:200]}{'...' if len(agent_reason) > 200 else ''}\n")
                if 'Conversation_History' in row.index and pd.notna(row.get('Conversation_History')):
                    f.write(f"\n  --- Conversation ---\n")
                    conversation = str(row['Conversation_History'])
                    try:
                        messages = json.loads(conversation)
                        for msg in messages:
                            sender = msg.get('sender', '?')
                            time = msg.get('time', '')
                            text = msg.get('message', '')
                            f.write(f"  [{time}] {sender}: {text}\n")
                    except (json.JSONDecodeError, TypeError):
                        f.write(f"  {conversation[:500]}{'...' if len(conversation) > 500 else ''}\n")
                    f.write(f"  --- End Conversation ---\n")
            
            # Best agreements
            f.write("\n" + "=" * 80 + "\n")
            f.write("TOP 10 AGREEMENTS (by Final Score)\n")
            f.write("=" * 80 + "\n\n")
            
            top_agreements = self.merged_data.nsmallest(10, 'final_diff')
            for _, row in top_agreements.iterrows():
                f.write(f"\nTicket ID: {row['ticket_id']}\n")
                f.write(f"  Human Final Score:  {row['final_score']:.1f}\n")
                f.write(f"  Agent Final Score:  {row['agent_final_score']:.1f}\n")
                f.write(f"  Difference:         {row['final_diff']:.1f}\n")
        
        print(f"\nReport saved to {output_path}")
    
    def plot_comparisons(self, output_dir: str = 'evaluation_plots') -> None:
        """Generate comparison plots."""
        if self.merged_data is None:
            self.merge_data()
        
        output_path = Path(output_dir)
        output_path.mkdir(exist_ok=True)
        
        # Set style
        sns.set_style("whitegrid")
        plt.rcParams['figure.figsize'] = (12, 8)
        
        score_pairs = [
            ('tone_score', 'agent_tone_score', 'Tone Score'),
            ('empathy_score', 'agent_empathy_score', 'Empathy Score'),
            ('solution_quality', 'agent_solution_quality', 'Solution Quality'),
            ('clarity_score', 'agent_clarity_score', 'Clarity Score'),
            ('final_score', 'agent_final_score', 'Final Score')
        ]
        
        # 1. Scatter plots with regression line
        for human_col, agent_col, title in score_pairs:
            fig, ax = plt.subplots(figsize=(10, 8))
            
            x = self.merged_data[human_col]
            y = self.merged_data[agent_col]
            
            # Scatter plot
            ax.scatter(x, y, alpha=0.6, s=100)
            
            # Perfect agreement line
            min_val = min(x.min(), y.min())
            max_val = max(x.max(), y.max())
            ax.plot([min_val, max_val], [min_val, max_val], 'r--', label='Perfect Agreement', linewidth=2)
            
            # Regression line (sort x so the line doesn't zigzag)
            z = np.polyfit(x, y, 1)
            p = np.poly1d(z)
            x_sorted = np.sort(x)
            ax.plot(x_sorted, p(x_sorted), 'b-', label=f'Regression (y={z[0]:.2f}x+{z[1]:.2f})', linewidth=2)
            
            ax.set_xlabel('Human Score', fontsize=12)
            ax.set_ylabel('Agent Score', fontsize=12)
            ax.set_title(f'{title} Comparison', fontsize=14, fontweight='bold')
            ax.legend(fontsize=10)
            ax.grid(True, alpha=0.3)
            
            # Add correlation text
            if np.std(x) == 0 or np.std(y) == 0:
                corr_text = 'Correlation: N/A (constant values)'
            else:
                corr = np.corrcoef(x, y)[0, 1]
                corr_text = f'Correlation: {corr:.3f}'
            ax.text(0.05, 0.95, corr_text,
                    transform=ax.transAxes, fontsize=11,
                    verticalalignment='top',
                    bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))
            
            plt.tight_layout()
            plt.savefig(output_path / f'{human_col}_comparison.png', dpi=300, bbox_inches='tight')
            plt.close()
        
        # 2. Distribution comparison
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        for idx, (human_col, agent_col, title) in enumerate(score_pairs):
            ax = axes[idx]
            
            ax.hist(self.merged_data[human_col], alpha=0.5, label='Human', bins=10, color='blue')
            ax.hist(self.merged_data[agent_col], alpha=0.5, label='Agent', bins=10, color='orange')
            
            ax.set_xlabel('Score', fontsize=10)
            ax.set_ylabel('Frequency', fontsize=10)
            ax.set_title(title, fontsize=11, fontweight='bold')
            ax.legend(fontsize=9)
            ax.grid(True, alpha=0.3)
        
        # Remove extra subplot
        fig.delaxes(axes[5])
        
        plt.tight_layout()
        plt.savefig(output_path / 'score_distributions.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        # 3. Difference distribution
        fig, axes = plt.subplots(2, 3, figsize=(18, 12))
        axes = axes.flatten()
        
        diff_cols = ['tone_diff', 'empathy_diff', 'solution_diff', 'clarity_diff', 'final_diff']
        titles = ['Tone', 'Empathy', 'Solution Quality', 'Clarity', 'Final Score']
        
        for idx, (diff_col, title) in enumerate(zip(diff_cols, titles)):
            ax = axes[idx]
            
            ax.hist(self.merged_data[diff_col], bins=15, color='green', alpha=0.7, edgecolor='black')
            
            mean_diff = self.merged_data[diff_col].mean()
            ax.axvline(mean_diff, color='red', linestyle='--', linewidth=2, label=f'Mean: {mean_diff:.2f}')
            
            ax.set_xlabel('Absolute Difference', fontsize=10)
            ax.set_ylabel('Frequency', fontsize=10)
            ax.set_title(f'{title} Difference Distribution', fontsize=11, fontweight='bold')
            ax.legend(fontsize=9)
            ax.grid(True, alpha=0.3)
        
        # Remove extra subplot
        fig.delaxes(axes[5])
        
        plt.tight_layout()
        plt.savefig(output_path / 'difference_distributions.png', dpi=300, bbox_inches='tight')
        plt.close()
        
        print(f"\nPlots saved to {output_dir}/")
    
    def export_detailed_comparison(self, output_path: str = 'detailed_comparison.xlsx') -> None:
        """Export detailed comparison to Excel."""
        if self.merged_data is None:
            self.merge_data()
        
        # Select relevant columns
        export_cols = [
            'ticket_id',
            'tone_score', 'agent_tone_score', 'tone_diff',
            'empathy_score', 'agent_empathy_score', 'empathy_diff',
            'solution_quality', 'agent_solution_quality', 'solution_diff',
            'clarity_score', 'agent_clarity_score', 'clarity_diff',
            'final_score', 'agent_final_score', 'final_diff',
            'reason', 'agent_reasons'
        ]
        
        # Filter columns that exist
        export_cols = [col for col in export_cols if col in self.merged_data.columns]
        
        export_data = self.merged_data[export_cols].copy()
        
        # Sort by final_diff descending
        export_data = export_data.sort_values('final_diff', ascending=False)
        
        # Export to Excel
        export_data.to_excel(output_path, index=False, engine='openpyxl')
        
        print(f"\nDetailed comparison exported to {output_path}")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='QC Agent Evaluation Script')
    parser.add_argument('--excel', default='TicketـQC.xlsx',
                        help='Path to Excel file with human scoring (default: TicketـQC.xlsx)')
    parser.add_argument('--json', default='results_v2.json',
                        help='Path to JSON file with agent scoring (default: results_v2.json)')
    parser.add_argument('--output-dir', default='.',
                        help='Directory for output files (default: current directory)')
    parser.add_argument('--demo-report', default='demo_report.xlsx',
                        help='Path to demo_report.xlsx for conversation history (default: demo_report.xlsx)')
    parser.add_argument('--no-plots', action='store_true',
                        help='Skip generating plots')
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    print("QC Agent Evaluation Script")
    print("=" * 80)

    evaluator = QCEvaluator(
        excel_path=args.excel,
        json_path=args.json,
        demo_report_path=args.demo_report,
    )

    try:
        evaluator.load_data()
        evaluator.merge_data()
    except (FileNotFoundError, ValueError) as e:
        print(f"\nError: {e}", file=sys.stderr)
        sys.exit(1)

    metrics = evaluator.calculate_metrics()

    # Print summary to console
    print("\n" + "=" * 80)
    print("EVALUATION SUMMARY")
    print("=" * 80)

    for dimension, values in metrics.items():
        print(f"\n{dimension}:")
        print(f"  MAE: {values['MAE']:.3f} | RMSE: {values['RMSE']:.3f} | R²: {values['R²']:.3f}")
        print(f"  Correlation: {values['Correlation']:.3f} | Bias: {values['Mean Bias']:+.2f}")
        print(f"  Within 1 point: {values['Within 1 point']:.1f}% | Within 2 points: {values['Within 2 points']:.1f}%")

    # Generate outputs
    print("\n" + "=" * 80)
    print("GENERATING OUTPUTS")
    print("=" * 80)

    report_path = output_dir / 'evaluation_report.txt'
    evaluator.generate_report(str(report_path))

    if not args.no_plots:
        evaluator.plot_comparisons(str(output_dir / 'evaluation_plots'))

    comparison_path = output_dir / 'detailed_comparison.xlsx'
    evaluator.export_detailed_comparison(str(comparison_path))

    print("\n" + "=" * 80)
    print("EVALUATION COMPLETE!")
    print("=" * 80)
    print(f"\nGenerated files in {output_dir}/:")
    print(f"  - {report_path} (comprehensive text report)")
    if not args.no_plots:
        print(f"  - {output_dir / 'evaluation_plots'}/ (visualization plots)")
    print(f"  - {comparison_path} (detailed Excel comparison)")


if __name__ == '__main__':
    main()