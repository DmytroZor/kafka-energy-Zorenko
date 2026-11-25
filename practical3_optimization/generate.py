"""P3: same reproducible time-series inserted into both physical schemas."""
import argparse
import json
import time
from pathlib import Path
from datetime import datetime, timezone
from collections import deque
from cassandra.cluster import Cluster
from common import CASSANDRA, CASSANDRA_PORT, bucket_hour
from practical3_optimization.core import points

DEVICE = "DEMO_SOLAR_001"




def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=3)
    ap.add_argument("--step-minutes", type=int, default=1)
    ap.add_argument("--inflight", type=int, default=80)
    ap.add_argument("--device", default=DEVICE)
    args = ap.parse_args()
    if args.days <= 0 or args.step_minutes <= 0 or args.inflight <= 0:
        ap.error("days, step-minutes and inflight must be > 0")
    end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    cluster = Cluster([CASSANDRA], port=CASSANDRA_PORT)
    try:
        session = cluster.connect("demo_practice")
        bad = session.prepare("INSERT INTO telemetry_bad (device_id,recorded_at,power_output,voltage) VALUES (?,?,?,?)")
        good = session.prepare("INSERT INTO telemetry_hourly (device_id,bucket_hour,recorded_at,power_output,voltage) VALUES (?,?,?,?,?)")
        q = deque()
        count = 0
        started = time.perf_counter()
        for dt, power, voltage in points(args.days, args.step_minutes, end=end):
            q.append(session.execute_async(bad, (args.device, dt, power, voltage)))
            q.append(session.execute_async(good, (args.device, bucket_hour(dt), dt, power, voltage)))
            while len(q) >= args.inflight:
                q.popleft().result()  # surfaces CQL/network failures
            count += 1
            if count % 3000 == 0:
                print(f"Inserted {count} rows in EACH schema")
        while q:
            q.popleft().result()
        elapsed = time.perf_counter() - started
        expected = args.days * 24 * 60 // args.step_minutes
        print(f"Completed: {count} rows/table, {count*2} inserts, expected={expected}")
        print(f"Confirmed writes: {count * 2 / elapsed:.1f} inserts/s across both tables ({elapsed:.3f} s)")
        print(f"Bad partition: one growing partition per device")
        print(f"Good partitions: about {args.days*24} hourly buckets per device")
        path=Path("results/p3_generation_metadata.json")
        path.parent.mkdir(exist_ok=True, parents=True)
        path.write_text(json.dumps({"days":args.days,"step_minutes":args.step_minutes,
                                    "device":args.device,"end_hour_utc":end.isoformat(),
                                    "rows_per_table":count},indent=2),encoding="utf-8")
        print(f"Benchmark reference hour saved: {path}")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
