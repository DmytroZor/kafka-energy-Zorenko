-- P3: deliberately bad and hourly-bucketed schemas for A/B comparison.
CREATE KEYSPACE IF NOT EXISTS demo_practice
WITH replication={'class':'SimpleStrategy','replication_factor':1};
USE demo_practice;
CREATE TABLE IF NOT EXISTS telemetry_bad (
  device_id TEXT,
  recorded_at TIMESTAMP,
  power_output DOUBLE,
  voltage DOUBLE,
  PRIMARY KEY (device_id, recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND comment = 'TEACHING ANTIPATTERN: unbounded partition';
CREATE TABLE IF NOT EXISTS telemetry_hourly (
  device_id TEXT,
  bucket_hour TIMESTAMP,
  recorded_at TIMESTAMP,
  power_output DOUBLE,
  voltage DOUBLE,
  PRIMARY KEY ((device_id, bucket_hour), recorded_at)
) WITH CLUSTERING ORDER BY (recorded_at DESC)
AND comment = 'GOOD: hourly bucket limits partition growth';
