"""Publish a slice of generated data to a real broker and read it back.

Usage (broker already running, e.g. docker compose --profile kafka up -d):
  python scripts/kafka_smoke_test.py --bootstrap localhost:9092 --count 200 --format esri

Exit code 0 only if every published record is consumed back and, for
--format esri, every record is flat JSON with the expected time/geometry fields.
"""
import argparse
import json
import sys
import time
import uuid

from confluent_kafka import Consumer, TopicPartition
from confluent_kafka.admin import AdminClient, NewTopic

from airport_eye.config import load_config
from airport_eye.esri import ZoneLocator
from airport_eye.models import EventEnvelope
from airport_eye.publishers.kafka import KafkaPublisher


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--bootstrap", default="localhost:9092")
    ap.add_argument("--file", default="generated_data/passenger_touchpoint.jsonl")
    ap.add_argument("--config", default="config/poc.json")
    ap.add_argument("--zones", default="config/terminal_layout.json")
    ap.add_argument("--format", choices=["envelope", "esri"], default="esri")
    ap.add_argument("--count", type=int, default=200)
    args = ap.parse_args()

    kafka_cfg = load_config(args.config).kafka.model_copy(update={"topic_prefix": f"smoke-{uuid.uuid4().hex[:8]}"})
    pub = KafkaPublisher(kafka_cfg, fmt=args.format, locator=ZoneLocator.from_file(args.zones),
                         overrides={"bootstrap.servers": args.bootstrap})
    events = []
    with open(args.file, encoding="utf-8") as fh:
        for line in fh:
            if len(events) >= args.count:
                break
            events.append(EventEnvelope.model_validate_json(line))
    topic = pub.topic_for(events[0])

    admin = AdminClient({"bootstrap.servers": args.bootstrap})
    for fut in admin.create_topics([NewTopic(topic, num_partitions=3, replication_factor=1)]).values():
        fut.result(timeout=15)

    for ev in events:
        pub.publish(ev)
    pub.flush(30)
    print("published:", pub.stats())

    consumer = Consumer({"bootstrap.servers": args.bootstrap, "group.id": f"smoke-{uuid.uuid4().hex[:6]}",
                         "auto.offset.reset": "earliest", "enable.auto.commit": False})
    consumer.subscribe([topic])
    got, deadline = [], time.time() + 30
    while len(got) < len(events) and time.time() < deadline:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            got.append(json.loads(msg.value()))
    consumer.close()
    print(f"consumed {len(got)}/{len(events)} from {topic}")

    ok = len(got) == len(events)
    if args.format == "esri":
        for rec in got:
            need = ("event_time_epoch_ms", "entity_id", "longitude", "latitude")
            if "payload" in rec or any(k not in rec for k in need):
                ok = False
                print("bad record:", rec)
                break
    print("PASS" if ok else "FAIL")
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
