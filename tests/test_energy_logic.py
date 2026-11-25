from datetime import datetime, timezone
from practical3_optimization.core import points
from practical3_optimization.core import percentile, summarize
from practical4_streaming.energy_logic import (
    detect_anomaly,fold_reading,new_accumulator,format_aggregate,
    format_hourly,merge_accumulators,event_datetime
)
from practical4_streaming.meter_logic import generate_reading,make_meters
import random


def sample(meter="SM-0001",timestamp=1760000000000,voltage=220,current=25,frequency=50,power=5225):
    return {"meter_id":meter,"timestamp":timestamp,"region":"Kyiv",
            "voltage":voltage,"current":current,"frequency":frequency,
            "power":power,"sample_interval_sec":1.0}


def test_normal_no_anomaly():
    assert detect_anomaly(sample()) is None


def test_voltage_warning_critical_and_current():
    assert detect_anomaly(sample(voltage=245))["severity"] == "warning"
    assert detect_anomaly(sample(voltage=255))["severity"] == "critical"
    assert detect_anomaly(sample(current=75))["anomaly_type"] == "current_surge"
    assert detect_anomaly(sample(frequency=50.8))["anomaly_type"] == "frequency_anomaly"


def test_accumulator_count_min_max_unique_meters():
    acc=new_accumulator()
    fold_reading(acc,sample("SM-1",voltage=220,power=5000))
    fold_reading(acc,sample("SM-1",voltage=230,power=6000,timestamp=1760000000500))
    fold_reading(acc,sample("SM-2",voltage=210,power=7000,timestamp=1760000000900))
    out=format_aggregate(("Kyiv",(0,acc)))
    assert out["readings_count"] == 3
    assert out["active_meters"] == 2
    assert out["avg_voltage"] == 220
    assert out["min_voltage"] == 210 and out["max_voltage"] == 230
    assert out["total_power"] == 18000
    assert out["window_end"]-out["window_start"]==60000


def test_merge_accumulators_hourly_energy():
    a,b=new_accumulator(),new_accumulator()
    fold_reading(a,sample("M1",power=3600000))
    fold_reading(b,sample("M2",power=3600000))
    c=merge_accumulators(a,b)
    assert len(c["meters"])==2
    assert abs(format_hourly(("Kyiv",(0,c)))["total_energy"]-2)<1e-8


def test_utc_timestamps():
    assert event_datetime(sample()).tzinfo == timezone.utc


def test_generator_reproducible_and_bucketed():
    end=datetime(2026,11,22,12,tzinfo=timezone.utc)
    a=list(points(days=1,step_minutes=60,end=end))
    b=list(points(days=1,step_minutes=60,end=end))
    assert a==b and len(a)==24
    assert a[0][0].hour==12


def test_percentiles():
    a=summarize([1,2,3,4,5])
    assert a["p50"]==3 and a["p95"]>4
    assert percentile([1,2],0)==1


def test_smart_meter_generator_fields():
    rng=random.Random(12)
    meter=make_meters(1,rng)[0]
    reading=generate_reading(meter,1760000000000,rng,inject=True)
    assert set(["meter_id","region","timestamp","voltage","current","power"]).issubset(reading)
    assert reading["power"]>=0
