"""P1: JSON consumer; manual offset commit after handling each record."""
import argparse
import json
from kafka import KafkaConsumer
from common import KAFKA, P1_TOPIC


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-messages", type=int, default=20)
    ap.add_argument("--group", default="energy-monitor-zorenko")
    ap.add_argument("--reset", choices=["earliest", "latest"], default="earliest")
    ap.add_argument("--timeout-ms", type=int, default=15000)
    args = ap.parse_args()
    consumer = KafkaConsumer(P1_TOPIC, bootstrap_servers=KAFKA,
                             group_id=args.group, auto_offset_reset=args.reset,
                             enable_auto_commit=False, consumer_timeout_ms=args.timeout_ms,
                             value_deserializer=lambda b: json.loads(b.decode("utf-8")))
    received = 0
    try:
        for msg in consumer:
            event = msg.value
            alarm = (event["frequency_hz"] < 49.8 or event["frequency_hz"] > 50.2)
            print(f"partition={msg.partition} offset={msg.offset} "
                  f"station={event['station_id']} power={event['power_output_mw']} MW "
                  f"freq={event['frequency_hz']} Hz alarm={alarm}")
            consumer.commit()
            received += 1
            if received >= args.max_messages:
                break
    finally:
        consumer.close()
    print(f"Consumed: {received}. Rerun with another --group for independent replay.")

if __name__ == "__main__":
    main()
