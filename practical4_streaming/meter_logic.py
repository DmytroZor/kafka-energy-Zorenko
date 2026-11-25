"""Standalone IoT reading generation logic; deterministic unit tests."""
import random

REGIONS = ("Kyiv", "Lviv", "Odesa", "Kharkiv", "Dnipro")


def make_meters(count, rng):
    return [{"meter_id":f"SM-{i:04d}", "region": REGIONS[i % len(REGIONS)],
             "base_voltage":rng.gauss(220,2), "base_current":max(8,rng.gauss(25,5))}
            for i in range(count)]


def generate_reading(meter, now_ms, rng, interval_sec=1.0, inject=False):
    voltage = meter["base_voltage"] + rng.gauss(0,3)
    current = max(0.0,meter["base_current"] + rng.gauss(0,2))
    frequency = 50 + rng.gauss(0,0.1)
    if inject:
        if rng.random() < 0.5:
            voltage += rng.choice((-45,45))
        else:
            current *= 3.5
    pf = 0.95
    return {"meter_id":meter["meter_id"],"timestamp":now_ms,
            "region":meter["region"],"voltage":round(voltage,2),
            "current":round(current,2),"power":round(voltage*current*pf,2),
            "power_factor":pf,"frequency":round(frequency,3),
            "temperature":round(rng.gauss(25,3),2),"sample_interval_sec":interval_sec}
