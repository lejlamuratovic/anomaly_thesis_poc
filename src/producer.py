import json
import os
import csv
from confluent_kafka import Producer
import time
import random

BOOTSTRAP_SERVERS = '127.0.0.1:9092'
TOPIC = 'auth_events'

def create_producer():
    return Producer({
        'bootstrap.servers': BOOTSTRAP_SERVERS,
        'broker.address.family': 'v4'
    })


def generate_normal_events(count=100):
    events = []
    for _ in range(count):
        events.append({
            'burst_id': None, 'scenario': 'normal',
            'username': f'user_{random.randint(1,1000)}',
            'ip_address': f'192.0.2.{random.randint(1,254)}',
            'event_type': 'SuccessfulLogin',
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        })
    return events


def generate_synthetic_burst(burst_id, size=10):
    # generate diverse attack scenarios for FP/FN
    scenario = random.choice(['large', 'small', 'multi_ip', 'noise'])
    
    if scenario == 'large':
        # large burst: full size from single attacker
        attacker_ip = f'192.0.2.{random.randint(1,254)}'
        attacker_user = f'user_{random.randint(1,1000)}'
        n = size
    elif scenario == 'small':
        # small burst: below rule threshold
        attacker_ip = f'192.0.2.{random.randint(1,254)}'
        attacker_user = f'user_{random.randint(1,1000)}'
        n = random.randint(1, max(1, size-1))
    elif scenario == 'multi_ip':
        # multi-IP burst: spread failures across IPs
        events = []
        for _ in range(size):
            events.append({
                'burst_id': burst_id, 'scenario': scenario,
                'username': f'user_{random.randint(1,1000)}',
                'ip_address': f'192.0.2.{random.randint(1,254)}',
                'event_type': 'FailedLogin',
                'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
            })
        return events
    else:
        # noise: isolated single failure
        return [{
            'burst_id': burst_id, 'scenario': scenario,
            'username': f'user_{random.randint(1,1000)}',
            'ip_address': f'192.0.2.{random.randint(1,254)}',
            'event_type': 'FailedLogin',
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        }]

    events = []
    for _ in range(n):
        events.append({
            'burst_id': burst_id, 'scenario': scenario,
            'username': attacker_user,
            'ip_address': attacker_ip,
            'event_type': 'FailedLogin',
            'timestamp': time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())
        })
    return events


def main():
    print(f'Starting producer...')
    producer = create_producer()
    os.makedirs('metrics_output', exist_ok=True)
    gt_path = os.path.join('metrics_output', 'ground_truth.csv')
    # write ground truth header
    with open(gt_path, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['burst_id','scenario','size','timestamp'])

    # produce normal events for training
    normals = generate_normal_events(100)
    
    for event in normals:
        producer.produce(TOPIC, key=event['ip_address'], value=json.dumps(event))
    remaining = producer.flush(timeout=10)   # wait at most 10 s

    if remaining:
        print(f"⚠️  {remaining} messages not delivered")

    print(f'Produced normal events (count={len(normals)})')

    burst_id = 1

    while True:
        events = generate_synthetic_burst(burst_id)
        # log ground truth for this burst
        scenario = events[0].get('scenario', 'unknown')
        with open(gt_path, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow([burst_id, scenario, len(events), time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime())])
        
        for event in events:
            producer.produce(TOPIC, key=event['ip_address'], value=json.dumps(event))

        remaining = producer.flush(timeout=10)
        if remaining:
            print(f"⚠️  {remaining} messages not delivered in burst {burst_id}")

        print(f"Produced burst {burst_id} (size={len(events)})")

        burst_id += 1
        time.sleep(5)


if __name__ == '__main__':
    main()
