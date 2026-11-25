"""Observe real Kafka message arrival/event-age; measure, never fabricate."""
import argparse
import csv
import json
import time
from pathlib import Path
from kafka import KafkaConsumer
from common import KAFKA, utc_now_ms
from practical3_optimization.core import summarize


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--topic",choices=["energy.raw","energy.anomalies"],default="energy.raw")
    ap.add_argument("--count",type=int,default=200)
    ap.add_argument("--timeout-ms",type=int,default=15000)
    ap.add_argument("--output",default="results/p4_probe.csv")
    args=ap.parse_args()
    if args.count < 2 or args.timeout_ms <= 0:
        ap.error("count must be at least 2 and timeout-ms must be positive")
    consumer=KafkaConsumer(args.topic,bootstrap_servers=KAFKA,auto_offset_reset="latest",
        group_id=f"p4-probe-{int(time.time()*1000)}",enable_auto_commit=False,
        consumer_timeout_ms=args.timeout_ms)
    data=[]
    first_received = None
    last_received = None
    try:
        for message in consumer:
            last_received = time.perf_counter()
            if first_received is None:
                first_received = last_received
            event=json.loads(message.value.decode())
            arrival=utc_now_ms()
            event_age=max(0,arrival-int(event["timestamp"]))
            data.append([args.topic,message.partition,message.offset,event_age])
            if len(data)>=args.count:
                break
    finally:
        consumer.close()
    if len(data) < 2:
        raise RuntimeError("Too few live messages; start the probe before the simulator")
    elapsed = last_received - first_received
    stats=summarize([x[3] for x in data])
    rate = (len(data) - 1) / elapsed
    print(f"{args.topic}: {len(data)} of {args.count} messages, first to last in {elapsed:.3f} s, {rate:.1f} events/s")
    print("Observed event age in Kafka consumer (NOT full end-to-end Cassandra latency):",stats)
    out=Path(args.output)
    out.parent.mkdir(parents=True,exist_ok=True)
    with out.open("w",encoding="utf-8",newline="") as f:
        w=csv.writer(f)
        w.writerow(["topic","partition","offset","consumer_event_age_ms"])
        w.writerows(data)
    print("Actual samples saved:",out)

if __name__=="__main__":
    main()
