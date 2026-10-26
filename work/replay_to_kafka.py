"""Replay generator output into Kafka as raw strings (dirty rows included).

  docker compose exec spark python3 replay_to_kafka.py --file /data/meter_readings.jsonl --topic meter.readings
"""
import argparse, os, time
from kafka import KafkaProducer

ap = argparse.ArgumentParser()
ap.add_argument("--file", required=True)
ap.add_argument("--topic", required=True)
ap.add_argument("--limit", type=int, default=0, help="0 = replay everything")
ap.add_argument("--rate", type=int, default=0, help="msgs/sec, 0 = as fast as possible")
a = ap.parse_args()

p = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "localhost:29092"),
                  linger_ms=20, batch_size=256 * 1024)
sent = 0
with open(a.file, "rb") as f:
    for line in f:
        p.send(a.topic, value=line.rstrip(b"\n"))   # raw bytes: malformed rows stay malformed
        sent += 1
        if a.rate and sent % a.rate == 0:
            p.flush(); time.sleep(1)
        if a.limit and sent >= a.limit:
            break
p.flush()
print(f"sent {sent} messages to {a.topic}")