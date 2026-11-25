"""P4: simulated smart meters with deliberate out-of-range events."""
import argparse
import json
import random
import time
from collections import deque
from kafka import KafkaProducer
from common import KAFKA, utc_now_ms
from practical4_streaming.meter_logic import make_meters, generate_reading



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--meters", type=int, default=20)
    ap.add_argument("--duration", type=float, default=135)
    ap.add_argument("--interval", type=float, default=1.0)
    ap.add_argument("--anomaly-rate", type=float, default=0.10)
    ap.add_argument("--seed", type=int, default=2026)
    ap.add_argument("--event-time-offset-seconds", type=int, default=0,
                    help="Shift event timestamps for an event-time window test")
    args=ap.parse_args()
    if args.meters <= 0 or args.duration <= 0 or args.interval <= 0 or not 0 <= args.anomaly_rate <= 1:
        ap.error("Invalid meters/duration/interval/anomaly-rate")
    rng=random.Random(args.seed)
    meters=make_meters(args.meters,rng)
    producer=KafkaProducer(bootstrap_servers=KAFKA, acks="all",retries=5,
                           key_serializer=lambda k:k.encode(),
                           value_serializer=lambda v:json.dumps(v).encode())
    sent=0
    injected=0
    deliveries=deque()
    start=time.monotonic()
    next_tick=start
    try:
        while time.monotonic()-start < args.duration:
            tick=utc_now_ms() + args.event_time_offset_seconds * 1000
            for meter in meters:
                anomaly = rng.random() < args.anomaly_rate
                event=generate_reading(meter,tick,rng,args.interval,anomaly)
                deliveries.append(producer.send("energy.raw",key=meter["meter_id"],value=event))
                if len(deliveries) >= 4000:
                    deliveries.popleft().get(timeout=45)
                sent += 1
                injected += int(anomaly)
            if sent % (args.meters*10) == 0:
                print(f"IoT: sent={sent}, deliberately injected={injected}")
            next_tick += args.interval
            time.sleep(max(0.0,next_tick-time.monotonic()))
        producer.flush(timeout=45)
        for delivery in deliveries:
            delivery.get(timeout=45)
        print(f"Simulator completed: {sent} messages, injected={injected}, {args.meters} meters")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
