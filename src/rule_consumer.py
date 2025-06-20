import json
import time
from confluent_kafka import Consumer, Producer
from collections import defaultdict, deque
from metrics import MetricsLogger

BOOTSTRAP_SERVERS = '127.0.0.1:9092'
INPUT_TOPIC = 'auth_events'
OUTPUT_TOPIC = 'rule_anomalies'

THRESHOLD = 6
WINDOW_SECONDS = 60

class RuleDetector:
    def __init__(self, metrics):
        self.metrics = metrics
        self.failures = defaultdict(deque)
        self.producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS, 'broker.address.family': 'v4'})

    def process(self, event):
        ip = event['ip_address']
        ts = time.time()

        if event['event_type'] == 'FailedLogin':
            dq = self.failures[ip]
            dq.append(ts)

            # evict old
            while dq and dq[0] < ts - WINDOW_SECONDS:
                dq.popleft()

            if len(dq) > THRESHOLD:
                anomaly = {**event, 'detected_at': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())}
                self.producer.produce(OUTPUT_TOPIC, key=ip, value=json.dumps(anomaly))
                self.producer.flush()
                self.metrics.record(event, anomaly['detected_at'])

                dq.clear()


def main():
    consumer = Consumer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'group.id': 'rule_detector_group',
        'auto.offset.reset': 'earliest',
        'broker.address.family': 'v4'
    })

    consumer.subscribe([INPUT_TOPIC])
    metrics = MetricsLogger('rule')
    detector = RuleDetector(metrics)
    print("Rule consumer started.")
    
    try:
        while True:
            msg = consumer.poll(1.0)
            if msg and not msg.error():
                event = json.loads(msg.value().decode('utf-8'))
                detector.process(event)
    finally:
        metrics.finish()
        consumer.close()


if __name__ == '__main__':
    main()
