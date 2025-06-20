#!/usr/bin/env python3
"""
Analysis script to summarize detection performance per attack scenario.
Reads ground_truth.csv, rule_metrics.csv, and ml_metrics.csv from metrics_output/.
Prints a Markdown table of detection rates per scenario.
"""
import os
import pandas as pd

def main():
    base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
    gt_path = os.path.join(base_dir, 'metrics_output', 'ground_truth.csv')
    rule_path = os.path.join(base_dir, 'metrics_output', 'rule_metrics.csv')
    ml_path = os.path.join(base_dir, 'metrics_output', 'ml_metrics.csv')

    gt = pd.read_csv(gt_path)
    rule = pd.read_csv(rule_path)
    ml = pd.read_csv(ml_path)

    scenarios = gt['scenario'].unique()
    summary = []
    for scenario in scenarios:
        total = len(gt[gt['scenario'] == scenario])
        detected_rule = len(rule[rule['scenario'] == scenario]['burst_id'].unique())
        detected_ml = len(ml[ml['scenario'] == scenario]['burst_id'].unique())
        summary.append({
            'scenario': scenario,
            'total_bursts': total,
            'rule_detected': detected_rule,
            'rule_rate': f"{detected_rule/total:.2f}" if total else '0.00',
            'ml_detected': detected_ml,
            'ml_rate': f"{detected_ml/total:.2f}" if total else '0.00'
        })

    df = pd.DataFrame(summary)
    print(df.to_markdown(index=False))

if __name__ == '__main__':
    main()
