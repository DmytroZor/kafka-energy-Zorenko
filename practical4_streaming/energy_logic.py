"""Pure, independently testable functions used by Bytewax flow (no services)."""
import json
from datetime import datetime, timezone


def validate_reading(r):
    required = ("meter_id", "timestamp", "region", "voltage", "current", "power", "frequency")
    return isinstance(r, dict) and all(field in r for field in required)


def decode_message(message):
    return json.loads(message.value.decode("utf-8"))


def detect_anomaly(reading):
    if not validate_reading(reading):
        return None
    voltage = float(reading["voltage"])
    current = float(reading["current"])
    frequency = float(reading["frequency"])
    kind = severity = None
    score = 0.0
    if voltage < 200 or voltage > 240:
        kind = "voltage_anomaly"
        severity = "critical" if voltage < 190 or voltage > 250 else "warning"
        score = abs(voltage - 220.0) / 20.0  # defined teaching deviation metric, not a Z-score
    elif current > 60:
        kind = "current_surge"
        severity = "critical" if current > 90 else "warning"
        score = current / 60.0
    elif frequency < 49.5 or frequency > 50.5:
        kind = "frequency_anomaly"
        severity = "warning"
        score = abs(frequency - 50) / 0.5
    if kind is None:
        return None
    return {**reading, "anomaly_type": kind, "severity": severity,
            "deviation_score": round(score, 3),
            "detected_at": datetime.now(timezone.utc).isoformat()}


def new_accumulator():
    return {"count":0,"sum_voltage":0.0,"min_voltage":float("inf"),
            "max_voltage":float("-inf"),"sum_power":0.0,
            "max_power":float("-inf"),"energy_kwh":0.0,
            "meters":set(),"first_ms":None}


def fold_reading(acc, reading):
    v = float(reading["voltage"])
    p = float(reading["power"])
    t = int(reading["timestamp"])
    acc["count"] += 1
    acc["sum_voltage"] += v
    acc["min_voltage"] = min(acc["min_voltage"],v)
    acc["max_voltage"] = max(acc["max_voltage"],v)
    acc["sum_power"] += p
    acc["max_power"] = max(acc["max_power"],p)
    # Energy: W * seconds / 3,600,000 = kWh; assumes sampled power held until next sample.
    acc["energy_kwh"] += p * float(reading.get("sample_interval_sec",1.0)) / 3_600_000.0
    acc["meters"].add(reading["meter_id"])
    acc["first_ms"] = t if acc["first_ms"] is None else min(acc["first_ms"],t)
    return acc


def merge_accumulators(a,b):
    a["count"] += b["count"]
    a["sum_voltage"] += b["sum_voltage"]
    a["min_voltage"] = min(a["min_voltage"],b["min_voltage"])
    a["max_voltage"] = max(a["max_voltage"],b["max_voltage"])
    a["sum_power"] += b["sum_power"]
    a["max_power"] = max(a["max_power"],b["max_power"])
    a["energy_kwh"] += b["energy_kwh"]
    a["meters"].update(b["meters"])
    if b["first_ms"] is not None:
        a["first_ms"] = b["first_ms"] if a["first_ms"] is None else min(a["first_ms"],b["first_ms"])
    return a


def format_aggregate(item, window_seconds=60):
    region, (_, acc) = item
    start = acc["first_ms"] // (window_seconds * 1000) * (window_seconds * 1000)
    return {"region":region,"window_start":start,"window_end":start+window_seconds*1000,
            "avg_voltage":round(acc["sum_voltage"]/acc["count"],3),
            "max_voltage":round(acc["max_voltage"],3),
            "min_voltage":round(acc["min_voltage"],3),
            "total_power":round(acc["sum_power"],3),
            "avg_power":round(acc["sum_power"]/acc["count"],3),
            "active_meters":len(acc["meters"]),"readings_count":acc["count"]}


def format_hourly(item, window_seconds=3600):
    region, (_,acc) = item
    start_ms = acc["first_ms"] // (window_seconds*1000) * (window_seconds*1000)
    dt = datetime.fromtimestamp(start_ms/1000, tz=timezone.utc)
    return {"region":region,"window_start":start_ms,"date":dt.date().isoformat(),
            "hour":dt.hour,"avg_voltage":round(acc["sum_voltage"]/acc["count"],3),
            "avg_power":round(acc["sum_power"]/acc["count"],3),
            "max_power":round(acc["max_power"],3),
            "total_energy":round(acc["energy_kwh"],6),
            "total_readings":acc["count"]}


def event_datetime(reading):
    return datetime.fromtimestamp(int(reading["timestamp"])/1000,timezone.utc)
