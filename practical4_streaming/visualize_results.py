"""Render charts only from REAL Cassandra records. Never invent lab results."""
import argparse
from pathlib import Path
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
from cassandra.cluster import Cluster
from common import CASSANDRA,CASSANDRA_PORT


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("--region",default="Kyiv")
    ap.add_argument("--output",default="results/energy_metrics.png")
    args=ap.parse_args()
    cluster=Cluster([CASSANDRA],port=CASSANDRA_PORT)
    try:
        sess=cluster.connect("energy_monitoring4")
        stmt=sess.prepare("SELECT window_time,avg_voltage,total_power FROM regional_metrics WHERE region=? LIMIT 200")
        rows=sorted(sess.execute(stmt,(args.region,)),key=lambda row: row.window_time)
    finally:
        cluster.shutdown()
    if not rows:
        raise RuntimeError("No real aggregates in Cassandra yet. Run simulator + processor + agg writer.")
    times=[r.window_time for r in rows]
    voltage=[r.avg_voltage for r in rows]
    power=[r.total_power for r in rows]
    fig,axes=plt.subplots(2,1,figsize=(11,7),sharex=True)
    axes[0].plot(times,voltage,marker="o")
    axes[0].set_title(f"Average voltage — {args.region}")
    axes[0].set_ylabel("V")
    axes[0].grid(True)
    axes[1].plot(times,power,marker="o")
    axes[1].set_title("Sum of sampled power values (not energy)")
    axes[1].set_ylabel("W (summed readings)")
    axes[1].set_xlabel("UTC time")
    axes[1].grid(True)
    fig.autofmt_xdate()
    fig.tight_layout()
    out=Path(args.output)
    out.parent.mkdir(parents=True,exist_ok=True)
    fig.savefig(out,dpi=180)
    print(f"Saved real-data chart with {len(rows)} windows: {out}")

if __name__=="__main__":
    main()
