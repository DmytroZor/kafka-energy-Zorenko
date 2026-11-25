"""P2: demonstrate an UPDATE using the real existing alert UUID, not now()."""
import argparse
from datetime import date
from uuid import UUID
from cassandra.cluster import Cluster
from common import CASSANDRA, CASSANDRA_PORT

STATION = "123e4567-e89b-12d3-a456-426614174000"

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--resolve", action="store_true", help="actually resolve the alert; default preview")
    args = ap.parse_args()
    cluster = Cluster([CASSANDRA], port=CASSANDRA_PORT)
    try:
        session = cluster.connect("energy_monitoring")
        query = session.prepare(
            "SELECT alert_id,severity,acknowledged,resolved FROM station_alerts "
            "WHERE station_id=? AND alert_date=?")
        station = UUID(STATION)
        day = date.fromisoformat("2025-09-17")
        rows = list(session.execute(query, (station, day)))
        for row in rows:
            print(f"Alert {row.alert_id}, severity={row.severity}, resolved={row.resolved}")
        if args.resolve:
            stmt = session.prepare("UPDATE station_alerts SET acknowledged=true, resolved=true "
                                   "WHERE station_id=? AND alert_date=? AND alert_id=?")
            for row in rows:
                session.execute(stmt, (station, day, row.alert_id))
                print(f"Updated existing alert {row.alert_id}")
        else:
            print("Use --resolve to UPDATE exact existing alert IDs.")
    finally:
        cluster.shutdown()

if __name__ == "__main__":
    main()
