#!/usr/bin/env bash
set -euo pipefail
psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname postgres --set=warehouse_password="$WAREHOUSE_PASSWORD" --set=airflow_password="$AIRFLOW_DB_PASSWORD" <<'SQL'
CREATE ROLE aircraft LOGIN PASSWORD :'warehouse_password';
CREATE ROLE airflow LOGIN PASSWORD :'airflow_password';
CREATE DATABASE aircraft_demo OWNER aircraft;
CREATE DATABASE aircraft_live OWNER aircraft;
CREATE DATABASE aircraft_test OWNER aircraft;
CREATE DATABASE airflow OWNER airflow;
SQL
