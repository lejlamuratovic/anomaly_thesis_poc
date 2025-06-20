# Anomaly Detection POC

This POC runs two parallel Kafka consumers to detect synthetic burst anomalies in authentication events.

## Setup

```bash
# Navigate into project directory
cd anomaly_poc

# 1. Start Kafka & Zookeeper
docker-compose up -d

# 2. Create & activate Python virtualenv
python3 -m venv venv
source venv/bin/activate

# 3. Install dependencies
pip install -r requirements.txt
```

## Components

- **producer.py**: Generates synthetic burst events into `auth_events`.
- **rule_consumer.py**: Rule-based detector (threshold on failed attempts).
- **ml_consumer.py**: IsolationForest-based detector.
- **metrics.py**: Shared metrics collection (latency, throughput, precision/recall).
- **summarize.py**: Analysis script to summarize detection performance per attack scenario.

## Run

```bash
# In Terminal 1: run producer
source venv/bin/activate
python3 src/producer.py

# In Terminal 2: run rule-based detector
source venv/bin/activate
python3 src/rule_consumer.py

# In Terminal 3: run ML-based detector
source venv/bin/activate
python3 src/ml_consumer.py

# After sufficient runtime, stop all (Ctrl+C) and generate report:
python3 analysis/summarize.py

# Generate metrics report
python3 src/metrics.py
```

Metrics will be output into CSV files in `metrics_output/`.
