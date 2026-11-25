USE energy_monitoring4;
-- First verify each table has rows (small lab only; COUNT(*) is expensive at scale).
SELECT COUNT(*) FROM raw_readings;
SELECT COUNT(*) FROM regional_metrics;
SELECT COUNT(*) FROM anomalies;
SELECT COUNT(*) FROM anomalies_by_severity;
SELECT COUNT(*) FROM hourly_stats;
-- Works without known timestamps, shows sample records.
SELECT * FROM regional_metrics WHERE region='Kyiv' LIMIT 8;
SELECT * FROM anomalies LIMIT 8;
-- For current hour, replace example UTC bucket by an hour returned by raw_readings:
-- SELECT reading_time,voltage,current,power FROM raw_readings
-- WHERE meter_id='SM-0001' AND hour_bucket='2026-11-22 10:00:00+0000' LIMIT 10;
-- SELECT detected_at,meter_id,severity FROM anomalies
-- WHERE region='Kyiv' AND hour_bucket='2026-11-22 10:00:00+0000' LIMIT 10;
-- SELECT detected_at,region,meter_id FROM anomalies_by_severity
-- WHERE severity='critical' AND hour_bucket='2026-11-22 10:00:00+0000' LIMIT 10;
SELECT TTL(voltage) FROM raw_readings LIMIT 1;
SELECT TTL(avg_voltage) FROM regional_metrics LIMIT 1;
DESCRIBE TABLE raw_readings;
DESCRIBE TABLE anomalies_by_severity;
