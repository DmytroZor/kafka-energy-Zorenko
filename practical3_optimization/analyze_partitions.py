"""P3: extract nodetool tablestats and write a real comparison CSV.

No statistics are invented: missing Cassandra fields are written as
"not available" and must be investigated on the target system.
"""
import argparse
import csv
import re
from pathlib import Path

FIELDS = ["SSTable count", "Number of partitions (estimate)",
          "Compacted partition minimum bytes", "Compacted partition mean bytes",
          "Compacted partition maximum bytes", "Space used (live)"]


def extract(log_text):
    ret = {}
    for field in FIELDS:
        m = re.search(r"^\s*" + re.escape(field) + r"\s*:\s*(.+?)\s*$", log_text, re.MULTILINE)
        ret[field] = m.group(1) if m else "not available"
    return ret


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bad", default="results/p3_bad_tablestats.txt")
    parser.add_argument("--good", default="results/p3_good_tablestats.txt")
    parser.add_argument("--outfile", default="results/p3_partition_comparison.csv")
    args = parser.parse_args()
    bad = extract(Path(args.bad).read_text(encoding="utf-8-sig"))
    good = extract(Path(args.good).read_text(encoding="utf-8-sig"))
    print(f"{'Metric':42} {'WIDE ROW':22} {'HOURLY'}")
    for field in FIELDS:
        print(f"{field:42} {bad[field]:22} {good[field]}")
    destination = Path(args.outfile)
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("w", encoding="utf-8", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["metric", "telemetry_bad", "telemetry_hourly"])
        writer.writerows((field, bad[field], good[field]) for field in FIELDS)
    print(f"Cassandra observed partition statistics saved: {destination}")


if __name__ == "__main__":
    main()
