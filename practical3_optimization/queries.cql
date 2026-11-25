USE demo_practice;
SELECT COUNT(*) FROM telemetry_bad WHERE device_id='DEMO_SOLAR_001';
-- Use a specific bucket hour generated on your own run in the next query:
-- SELECT * FROM telemetry_hourly WHERE device_id='DEMO_SOLAR_001' AND bucket_hour='2026-11-22 11:00:00+0000' LIMIT 60;
-- In interactive cqlsh, use TRACING ON; run both queries; then TRACING OFF.
DESCRIBE TABLE telemetry_bad;
DESCRIBE TABLE telemetry_hourly;
