"""P3 comparable bounded one-hour scans; CSV saves real observed timings."""
import argparse
import json
import csv
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from cassandra.cluster import Cluster
from common import CASSANDRA, CASSANDRA_PORT
from practical3_optimization.core import percentile, summarize




def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--iterations", type=int, default=50)
    ap.add_argument("--device", default="DEMO_SOLAR_001")
    ap.add_argument("--outfile", default="results/p3_benchmark.csv")
    args = ap.parse_args()
    metadata = Path("results/p3_generation_metadata.json")
    if not metadata.exists():
        raise RuntimeError("Missing generation metadata; run generate.py first")
    info = json.loads(metadata.read_text(encoding="utf-8"))
    end = datetime.fromisoformat(info["end_hour_utc"])
    hour = end - timedelta(hours=1)
    if args.device != info["device"]:
        print(f"Warning: benchmark device {args.device} differs from seed device {info['device']}")
    cluster = Cluster([CASSANDRA], port=CASSANDRA_PORT)
    try:
        session = cluster.connect("demo_practice")
        # Identical logical rows: previous complete hour, up to 60 at 1/min.
        cases = {
            "bad": (session.prepare("SELECT recorded_at,power_output,voltage FROM telemetry_bad "
                      "WHERE device_id=? AND recorded_at>=? AND recorded_at<? LIMIT 60"),
                    (args.device, hour, end)),
            "hourly": (session.prepare("SELECT recorded_at,power_output,voltage FROM telemetry_hourly "
                         "WHERE device_id=? AND bucket_hour=? AND recorded_at>=? AND recorded_at<? LIMIT 60"),
                       (args.device,hour,hour,end)),
        }
        records = []
        for label, (stmt, params) in cases.items():
            # Warm up each case: cold vs warm cache are different experiments.
            for _ in range(3):
                list(session.execute(stmt, params))
            times = []
            for idx in range(args.iterations):
                started = time.perf_counter()
                rows = list(session.execute(stmt, params))
                duration_ms = (time.perf_counter() - started) * 1000
                if len(rows) == 0:
                    raise RuntimeError(f"No data for {label} at {hour}; run generate.py first")
                times.append(duration_ms)
                records.append([label,idx+1,len(rows),round(duration_ms,4),hour.isoformat()])
            print(f"{label}: rows={len(rows)}: {summarize(times)}")
        outfile = Path(args.outfile)
        outfile.parent.mkdir(parents=True,exist_ok=True)
        with outfile.open("w",encoding="utf-8",newline="") as fh:
            writer = csv.writer(fh)
            writer.writerow(["schema","iteration","rows_read","duration_ms","hour_utc"])
            writer.writerows(records)
        print(f"Measured benchmark saved: {outfile}")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
