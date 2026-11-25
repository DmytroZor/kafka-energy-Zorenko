"""P4 Bytewax 0.21.1: keyed event-time tumbling windows and anomaly routing.

Run with: python -m bytewax.run practical4_streaming.energy_stream_processor:flow
Do NOT run as `python energy_stream_processor.py` (that only defines flow).
"""
import json
from datetime import datetime, timedelta, timezone
from bytewax.dataflow import Dataflow
from bytewax import operators as op
from bytewax.connectors.kafka import KafkaSource, KafkaSink, KafkaSinkMessage
from bytewax.operators.windowing import EventClock, TumblingWindower, fold_window
from common import KAFKA
from practical4_streaming.energy_logic import (
    decode_message,validate_reading,detect_anomaly,new_accumulator,
    fold_reading,merge_accumulators,format_aggregate,format_hourly,event_datetime
)


def kafka_payload(event):
    return KafkaSinkMessage(key=str(event.get("region","")).encode("utf-8"),
                            value=json.dumps(event,ensure_ascii=False).encode("utf-8"))


flow=Dataflow("energy-realtime-zorenko")
source=op.input("raw-kafka",flow,KafkaSource([KAFKA],["energy.raw"]))
readings=op.map("decode-json",source,decode_message)
readings=op.filter("validate",readings,validate_reading)

anomalies=op.map("detect-anomaly",readings,detect_anomaly)
anomalies=op.filter("filter-anomalies",anomalies,lambda x:x is not None)
op.output("send-anomalies",op.map("encode-anomalies",anomalies,kafka_payload),
          KafkaSink([KAFKA],"energy.anomalies"))

by_region=op.key_on("key-by-region",readings,lambda x:x["region"])

clock_min=EventClock(ts_getter=event_datetime, wait_for_system_duration=timedelta(seconds=5))
minute_windower=TumblingWindower(length=timedelta(seconds=60),
                                   align_to=datetime(1970,1,1,tzinfo=timezone.utc))
minute_windows=fold_window("regional-minute-window",by_region,clock_min,minute_windower,
                           builder=new_accumulator,folder=fold_reading,
                           merger=merge_accumulators)
aggregates=op.map("calc-1min-regional",minute_windows.down,format_aggregate)
op.output("send-aggregates",op.map("encode-aggregates",aggregates,kafka_payload),
          KafkaSink([KAFKA],"energy.aggregated"))

clock_hour=EventClock(ts_getter=event_datetime, wait_for_system_duration=timedelta(seconds=5))
hour_windower=TumblingWindower(length=timedelta(hours=1),
                                 align_to=datetime(1970,1,1,tzinfo=timezone.utc))
hour_windows=fold_window("regional-hour-window",by_region,clock_hour,hour_windower,
                         builder=new_accumulator,folder=fold_reading,
                         merger=merge_accumulators)
hourly=op.map("calc-1h-regional",hour_windows.down,format_hourly)
op.output("send-hourly",op.map("encode-hourly",hourly,kafka_payload),
          KafkaSink([KAFKA],"energy.hourly"))
