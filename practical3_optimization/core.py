"""No-external-dependency helpers for P3."""
import random
import statistics
from datetime import datetime, timedelta, timezone

def points(days=3, step_minutes=1, end=None, seed=2026):
    if end is None:
        end = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    start = end - timedelta(days=days)
    rng = random.Random(seed)
    steps = int(days * 24 * 60 / step_minutes)
    for i in range(steps):
        dt = start + timedelta(minutes=i * step_minutes)
        power = rng.uniform(1.0, 4.0) if 6 <= dt.hour <= 20 else rng.uniform(0, 0.2)
        voltage = 400.0 + rng.uniform(-5, 5)
        yield dt, round(power, 4), round(voltage, 4)


def percentile(xs, p):
    ys=sorted(xs)
    if not ys:
        raise ValueError("no samples")
    k=(len(ys)-1)*p
    lo=int(k)
    hi=min(lo+1,len(ys)-1)
    return ys[lo]+(ys[hi]-ys[lo])*(k-lo)


def summarize(samples):
    return dict(avg=statistics.mean(samples),p50=percentile(samples,.50),
                p95=percentile(samples,.95),p99=percentile(samples,.99),
                min=min(samples),max=max(samples))
