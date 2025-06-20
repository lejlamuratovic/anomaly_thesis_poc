import json
import time
from collections import defaultdict, deque
import time
from confluent_kafka import Consumer, Producer
from sklearn.ensemble import IsolationForest
import numpy as np
from metrics import MetricsLogger

BOOTSTRAP_SERVERS = '127.0.0.1:9092'
INPUT_TOPIC = 'auth_events'
OUTPUT_TOPIC = 'ml_anomalies'
TRAIN_SIZE = 100
CONTAMINATION = 0.1
SCORE_THRESHOLD = -0.2
WINDOW_SECONDS = 5


def to_features(event, window_count):
    ts = int(time.mktime(time.strptime(event['timestamp'], '%Y-%m-%dT%H:%M:%SZ')))
    octet = int(event['ip_address'].split('.')[-1])

    return [ts, octet, window_count]
    return [ts, octet]


def main():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'ml_detector_group',
        'auto.offset.reset': 'earliest',
        'broker.address.family': 'v4'
    })

    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'broker.address.family': 'v4'})
    consumer.subscribe([INPUT_TOPIC])
    metrics = MetricsLogger('ml')

    # track event counts per IP in sliding window
    ip_deques = defaultdict(deque)
    X_train = []
    model = None
    threshold = None
    print("ML consumer started. Gathering training data...")
    try:

        while True:
            msg = consumer.poll(1.0)

            if msg and not msg.error():
                event = json.loads(msg.value().decode('utf-8'))
                # update sliding window count
                now_ts = time.time()
                dq = ip_deques[event['ip_address']]
                dq.append(now_ts)

                while dq and dq[0] < now_ts - WINDOW_SECONDS:
                    dq.popleft()
                window_count = len(dq)
                features = to_features(event, window_count)

                if model is None:
                    X_train.append(features)

                    if len(X_train) >= TRAIN_SIZE:
                        model = IsolationForest(contamination=CONTAMINATION, random_state=42)
                        model.fit(np.array(X_train))

                        # compute dynamic threshold from training scores
                        train_scores = model.decision_function(np.array(X_train))
                        threshold = np.percentile(train_scores, CONTAMINATION * 100)
                        print(f"Dynamic threshold set at {threshold}")
                        print(f"IsolationForest trained on {TRAIN_SIZE} events")
                    continue

                score = model.decision_function([features])[0]
                print(f"[ML Debug] burst={event.get('burst_id')} score={score:.2f} threshold={threshold:.2f}")

                if threshold is not None and score < threshold:
                    detected_at = time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
                    anomaly = {
                        **event,
                        'anomaly_score': float(score),
                        'detected_at': detected_at
                    }
                    
                    producer.produce(OUTPUT_TOPIC, key=event['ip_address'], value=json.dumps(anomaly))
                    producer.flush()
                    metrics.record(event, detected_at)
    finally:
        metrics.finish()
        consumer.close()


if __name__ == '__main__':
    main()
