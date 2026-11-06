CREATE TABLE IF NOT EXISTS pipeline_runs (
 run_id VARCHAR PRIMARY KEY, started_at TIMESTAMPTZ, ended_at TIMESTAMPTZ,
 status VARCHAR, mode VARCHAR, months_json VARCHAR, error VARCHAR
);
CREATE TABLE IF NOT EXISTS task_events (
 run_id VARCHAR, task VARCHAR, partition_month VARCHAR, status VARCHAR,
 recorded_at TIMESTAMPTZ, detail_json VARCHAR
);
CREATE TABLE IF NOT EXISTS partition_versions (
 version_id VARCHAR PRIMARY KEY, partition_month VARCHAR, source_sha256 VARCHAR,
 reference_hash VARCHAR, transform_version VARCHAR, received_at TIMESTAMPTZ,
 loaded_at TIMESTAMPTZ, raw_relative_path VARCHAR, silver_relative_path VARCHAR,
 raw_rows BIGINT, accepted_rows BIGINT, rejected_rows BIGINT, duplicate_rows BIGINT,
 inserted_rows BIGINT, updated_rows BIGINT, withdrawn_rows BIGINT, manifest_json VARCHAR
);
CREATE TABLE IF NOT EXISTS checkpoints (
 partition_month VARCHAR PRIMARY KEY, version_id VARCHAR, committed_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS flight_changes (
 version_id VARCHAR, flight_id VARCHAR, change_type VARCHAR, old_hash VARCHAR,
 new_hash VARCHAR, old_values_json VARCHAR, new_values_json VARCHAR
);
ALTER TABLE flight_changes ADD COLUMN IF NOT EXISTS run_id VARCHAR;
CREATE TABLE IF NOT EXISTS partition_activations (
 run_id VARCHAR, partition_month VARCHAR, version_id VARCHAR, previous_version_id VARCHAR,
 committed_at TIMESTAMPTZ, inserted_rows BIGINT, updated_rows BIGINT, withdrawn_rows BIGINT
);
CREATE TABLE IF NOT EXISTS quality_checks (
 run_id VARCHAR, partition_month VARCHAR, check_name VARCHAR, severity VARCHAR,
 affected_rows BIGINT, detail VARCHAR, recorded_at TIMESTAMPTZ
);
CREATE TABLE IF NOT EXISTS publications (
 publication_id VARCHAR PRIMARY KEY, created_at TIMESTAMPTZ, config_json VARCHAR,
 metrics_json VARCHAR, gold_relative_path VARCHAR
);
