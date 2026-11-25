-- P4. Local one-node cluster: RF=1. For genuine 3-node production, set datacenter1:3.
CREATE KEYSPACE IF NOT EXISTS energy_monitoring4
WITH replication = {'class':'NetworkTopologyStrategy','datacenter1':1}
AND durable_writes = true;
USE energy_monitoring4;

CREATE TABLE IF NOT EXISTS raw_readings (
  meter_id TEXT,
  hour_bucket TIMESTAMP,
  reading_time TIMESTAMP,
  region TEXT STATIC,
  voltage FLOAT,
  current FLOAT,
  power FLOAT,
  frequency FLOAT,
  temperature FLOAT,
  PRIMARY KEY ((meter_id,hour_bucket),reading_time)
) WITH CLUSTERING ORDER BY (reading_time DESC)
AND compaction={'class':'TimeWindowCompactionStrategy','compaction_window_size':'1','compaction_window_unit':'HOURS'}
AND default_time_to_live=604800
AND compression={'class':'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS regional_metrics (
  region TEXT,
  window_time TIMESTAMP,
  avg_voltage FLOAT,
  max_voltage FLOAT,
  min_voltage FLOAT,
  total_power DOUBLE,
  active_meters INT,
  readings_count INT,
  PRIMARY KEY (region,window_time)
) WITH CLUSTERING ORDER BY (window_time DESC)
AND default_time_to_live=2592000
AND compression={'class':'LZ4Compressor'};

CREATE TABLE IF NOT EXISTS anomalies (
  region TEXT,
  hour_bucket TIMESTAMP,
  detected_at TIMESTAMP,
  meter_id TEXT,
  anomaly_type TEXT,
  severity TEXT,
  voltage FLOAT,
  current FLOAT,
  frequency FLOAT,
  deviation_score FLOAT,
  PRIMARY KEY ((region,hour_bucket),detected_at,meter_id)
) WITH CLUSTERING ORDER BY (detected_at DESC,meter_id ASC)
AND default_time_to_live=7776000;

-- Reliable query-focused denormalization instead of experimental Cassandra MV.
CREATE TABLE IF NOT EXISTS anomalies_by_severity (
  severity TEXT,
  hour_bucket TIMESTAMP,
  detected_at TIMESTAMP,
  region TEXT,
  meter_id TEXT,
  anomaly_type TEXT,
  voltage FLOAT,
  current FLOAT,
  deviation_score FLOAT,
  PRIMARY KEY ((severity,hour_bucket),detected_at,region,meter_id)
) WITH CLUSTERING ORDER BY (detected_at DESC,region ASC,meter_id ASC)
AND default_time_to_live=7776000;

CREATE TABLE IF NOT EXISTS hourly_stats (
  region TEXT,
  date DATE,
  hour INT,
  avg_voltage FLOAT,
  avg_power DOUBLE,
  max_power DOUBLE,
  total_energy DOUBLE,           -- kWh, based on each reading's sampling interval
  total_readings INT,
  PRIMARY KEY ((region,date),hour)
) WITH CLUSTERING ORDER BY (hour ASC)
AND default_time_to_live=7776000;
