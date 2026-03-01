#!/usr/bin/env sh
# =============================================================================
# 02_set_passwords.sh — Set service role passwords from environment variables
# =============================================================================
#
# EXECUTION ORDER
#   Runs as the second initdb script (after 01_schema.sql, before 03_procedures.sql).
#   Must run after 01_schema.sql because the roles (kafka_connect, dbt_runner,
#   etc.) are created there. You cannot ALTER a role that does not exist yet.
#
# WHY A SHELL SCRIPT INSTEAD OF SQL
#   psql (used to execute .sql initdb files) cannot expand shell environment
#   variables. Only a shell script can read $KAFKA_CONNECT_PASSWORD etc. and
#   inject the values into SQL. This script bridges that gap.
#
# REQUIRED ENVIRONMENT VARIABLES
#   Set in .env, loaded by docker-compose into the container environment:
#     POSTGRES_USER            — superuser (set by the postgres image)
#     POSTGRES_DB              — database name (set by the postgres image)
#     KAFKA_CONNECT_PASSWORD
#     DBT_RUNNER_PASSWORD
#     API_READER_PASSWORD
#     DEBEZIUM_PASSWORD
#     AIRFLOW_RUNNER_PASSWORD
# =============================================================================

set -eu
# -e : exit immediately if any command fails
# -u : treat unset variables as errors — catches missing env vars immediately

echo "[initdb] Verifying required password variables..."

echo "[initdb] Setting service role passwords..."

exec psql -v ON_ERROR_STOP=1 \
     --username "${POSTGRES_USER}" \
     --dbname   "${POSTGRES_DB}" \
     <<-EOSQL
    ALTER ROLE kafka_connect  PASSWORD '${KAFKA_CONNECT_PASSWORD}';
    ALTER ROLE dbt_runner     PASSWORD '${DBT_RUNNER_PASSWORD}';
    ALTER ROLE api_reader     PASSWORD '${API_READER_PASSWORD}';
    ALTER ROLE debezium       PASSWORD '${DEBEZIUM_PASSWORD}';
    ALTER ROLE airflow_runner PASSWORD '${AIRFLOW_RUNNER_PASSWORD}';
EOSQL

echo "[initdb] Service role passwords set."
