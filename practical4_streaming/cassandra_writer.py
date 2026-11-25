"""P4: Kafka -> Cassandra, poll in groups, write idempotent primary keys, then commit.

A Cassandra cross-partition BEGIN BATCH is deliberately NOT used: it is often
slower and can create coordinator hotspots. Polling/flush here is batch-wise,
while CQL writes are per primary-key row.
"""
import argparse
import json
import time
from datetime import date
from kafka import KafkaConsumer
from cassandra.cluster import Cluster
from common import KAFKA, CASSANDRA, CASSANDRA_PORT, ts_utc, bucket_hour

TOPIC_BY_MODE={"raw":"energy.raw","agg":"energy.aggregated",
               "anomaly":"energy.anomalies","hourly":"energy.hourly"}


class CassandraWriter:
    def __init__(self, session):
        self.session=session
        self.raw=session.prepare("""INSERT INTO raw_readings
            (meter_id,hour_bucket,reading_time,region,voltage,current,power,frequency,temperature)
            VALUES (?,?,?,?,?,?,?,?,?)""")
        self.agg=session.prepare("""INSERT INTO regional_metrics
            (region,window_time,avg_voltage,max_voltage,min_voltage,total_power,active_meters,readings_count)
            VALUES (?,?,?,?,?,?,?,?)""")
        self.anomaly=session.prepare("""INSERT INTO anomalies
            (region,hour_bucket,detected_at,meter_id,anomaly_type,severity,voltage,current,frequency,deviation_score)
            VALUES (?,?,?,?,?,?,?,?,?,?)""")
        self.by_severity=session.prepare("""INSERT INTO anomalies_by_severity
            (severity,hour_bucket,detected_at,region,meter_id,anomaly_type,voltage,current,deviation_score)
            VALUES (?,?,?,?,?,?,?,?,?)""")
        self.hourly=session.prepare("""INSERT INTO hourly_stats
            (region,date,hour,avg_voltage,avg_power,max_power,total_energy,total_readings)
            VALUES (?,?,?,?,?,?,?,?)""")

    def write(self,mode,event):
        if mode=="raw":
            instant=ts_utc(event["timestamp"])
            self.session.execute(self.raw,(event["meter_id"],bucket_hour(instant),instant,
                                          event["region"],event["voltage"],event["current"],
                                          event["power"],event["frequency"],event.get("temperature",25)))
        elif mode=="agg":
            self.session.execute(self.agg,(event["region"],ts_utc(event["window_start"]),
                                          event["avg_voltage"],event["max_voltage"],
                                          event["min_voltage"],event["total_power"],
                                          event["active_meters"],event["readings_count"]))
        elif mode=="anomaly":
            instant=ts_utc(event["timestamp"])
            hour=bucket_hour(instant)
            base=(event["region"],hour,instant,event["meter_id"],event["anomaly_type"],
                  event["severity"],event["voltage"],event["current"],
                  event.get("frequency",50),event.get("deviation_score",0))
            self.session.execute(self.anomaly,base)
            self.session.execute(self.by_severity,
                (event["severity"],hour,instant,event["region"],event["meter_id"],
                 event["anomaly_type"],event["voltage"],event["current"],
                 event.get("deviation_score",0)))
        elif mode=="hourly":
            self.session.execute(self.hourly,(event["region"],date.fromisoformat(event["date"]),
                                             event["hour"],event["avg_voltage"],
                                             event["avg_power"],event["max_power"],
                                             event["total_energy"],event["total_readings"]))
        else:
            raise ValueError(mode)


def main():
    ap=argparse.ArgumentParser()
    ap.add_argument("mode",choices=list(TOPIC_BY_MODE))
    ap.add_argument("--max-records",type=int,default=0,help="0: continuous")
    ap.add_argument("--idle-seconds",type=float,default=0,help="0: continuous")
    args=ap.parse_args()
    topic=TOPIC_BY_MODE[args.mode]
    consumer=KafkaConsumer(topic,bootstrap_servers=KAFKA,
        group_id=f"cass-energy-{args.mode}-zorenko-v1",auto_offset_reset="earliest",
        enable_auto_commit=False,max_poll_records=100,
        value_deserializer=lambda b:json.loads(b.decode("utf-8")))
    cluster=Cluster([CASSANDRA],port=CASSANDRA_PORT)
    count=0
    last_data=time.monotonic()
    try:
        writer=CassandraWriter(cluster.connect("energy_monitoring4"))
        print(f"Writer mode={args.mode}, topic={topic}; press Ctrl+C to finish")
        while True:
            batches=consumer.poll(timeout_ms=1000,max_records=100)
            messages=[msg for batch in batches.values() for msg in batch]
            if not messages:
                if args.idle_seconds and time.monotonic()-last_data >= args.idle_seconds:
                    print("No more events; stopped on idle timeout")
                    break
                continue
            for msg in messages:
                writer.write(args.mode,msg.value)  # failure -> no offset commit
                count+=1
            consumer.commit()  # only commit fully persisted batch
            last_data=time.monotonic()
            print(f"{args.mode}: committed {len(messages)} Kafka messages; total={count}")
            if args.max_records and count >= args.max_records:
                break
    except KeyboardInterrupt:
        print("User interrupted writer")
    finally:
        consumer.close()
        cluster.shutdown()
        print(f"Writer ended: {count} records written to Cassandra")

if __name__=="__main__":
    main()
