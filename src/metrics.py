#!/usr/bin/env python3
"""
MetricsLogger: collect detection metrics for consumers.
"""
import os
import csv
import time
import statistics
import math

class MetricsLogger:
    def __init__(self, name):
        self.name = name
        self.records = []
        self.burst_counts = {}
        self.start_time = time.time()
        # output directory in project root
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        self.output_dir = os.path.join(base_dir, 'metrics_output')
        os.makedirs(self.output_dir, exist_ok=True)
        self.csv_path = os.path.join(self.output_dir, f'{self.name}_metrics.csv')
        # write header
        with open(self.csv_path, 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(['burst_id', 'scenario', 'event_timestamp', 'detected_at', 'latency_ms'])

    def record(self, event, detected_at):
        # compute latency
        event_ts = time.mktime(time.strptime(event['timestamp'], '%Y-%m-%dT%H:%M:%SZ'))
        detected_ts = time.mktime(time.strptime(detected_at, '%Y-%m-%dT%H:%M:%SZ'))
        latency_ms = (detected_ts - event_ts) * 1000
        burst_id = event.get('burst_id')
        self.records.append(latency_ms)
        self.burst_counts[burst_id] = self.burst_counts.get(burst_id, 0) + 1
        # append to CSV
        with open(self.csv_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([burst_id, event.get('scenario'), event['timestamp'], detected_at, f'{latency_ms:.2f}'])

    def finish(self):
        total_time = time.time() - self.start_time
        total_detections = len(self.records)
        throughput = total_detections / total_time if total_time > 0 else 0
        if self.records:
            mean = statistics.mean(self.records)
            median = statistics.median(self.records)
            p95 = self.percentile(self.records, 95)
        else:
            mean = median = p95 = 0
        # write summary
        summary_path = os.path.join(self.output_dir, f'{self.name}_summary.txt')
        with open(summary_path, 'w') as f:
            f.write(f'Total detections: {total_detections}\n')
            f.write(f'Throughput (det/sec): {throughput:.2f}\n')
            f.write(f'Latency mean(ms): {mean:.2f}\n')
            f.write(f'Latency median(ms): {median:.2f}\n')
            f.write(f'Latency 95th percentile(ms): {p95:.2f}\n')
            f.write(f'Unique bursts detected: {len(self.burst_counts)}\n')
            avg_per_burst = (total_detections / len(self.burst_counts)) if self.burst_counts else 0
            f.write(f'Average detections per burst: {avg_per_burst:.2f}\n')
        print(f'Metrics written to {self.csv_path} and {summary_path}')

    @staticmethod
    def percentile(data, percent):
        data_sorted = sorted(data)
        k = (len(data_sorted) - 1) * percent / 100
        f_idx = math.floor(k)
        c_idx = math.ceil(k)
        if f_idx == c_idx:
            return data_sorted[int(k)]
        d0 = data_sorted[f_idx] * (c_idx - k)
        d1 = data_sorted[c_idx] * (k - f_idx)
        return d0 + d1
