"""P1: Energy-station producer with stable message key (station_id)."""
import argparse
import json
import random
import time
from datetime import datetime, timezone
from kafka import KafkaProducer
from common import KAFKA, P1_TOPIC

STATIONS = (
    ("TPP_KYIV_01", "thermal", 1250.0, 220.0),
    ("HPP_DNIPRO_01", "hydro", 810.0, 400.0),
    ("SOLAR_NIKOPOL_01", "solar", 240.0, 35.0),
)


def reading(seq, rng):
    name, station_type, base_power, base_voltage = STATIONS[seq % len(STATIONS)]
    return {
        "station_id": name,
        "station_type": station_type,
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "power_output_mw": round(max(0.0, rng.gauss(base_power, base_power * 0.025)), 2),
        "voltage_kv": round(rng.gauss(base_voltage, 0.25), 2),
        "frequency_hz": round(rng.gauss(50.0, 0.09), 3),
        "sequence": seq,
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--count", type=int, default=150)
    ap.add_argument("--interval", type=float, default=0.03)
    args = ap.parse_args()
    if args.count <= 0 or args.interval < 0:
        ap.error("count must be positive and interval must be nonnegative")
    producer = KafkaProducer(bootstrap_servers=KAFKA, client_id="energy-producer-zorenko",
                             acks="all", retries=5,
                             key_serializer=lambda x: x.encode("utf-8"),
                             value_serializer=lambda x: json.dumps(x, ensure_ascii=False).encode("utf-8"))
    rng = random.Random(42)
    deliveries = []
    try:
        for i in range(args.count):
            event = reading(i, rng)
            deliveries.append(producer.send(P1_TOPIC, key=event["station_id"], value=event))
            if i < 5 or (i + 1) % 100 == 0:
                print(f"sent #{i+1}: {event}")
            if args.interval > 0:
                time.sleep(args.interval)
        producer.flush(timeout=30)
        for delivery in deliveries:
            delivery.get(timeout=30)
        print(f"Producer finished: {args.count} records -> {P1_TOPIC}")
    finally:
        producer.close()

if __name__ == "__main__":
    main()
