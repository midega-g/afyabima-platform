# AfyaBima — Database Layer

## What this folder is

`database_scripts/` is the single source of truth for everything the PostgreSQL database needs to exist, be populated, and stay correct. It contains the schema DDL, the password bootstrap script, all server-side procedures, the full quality test suite, and the two data ingestion paths. Nothing in this folder assumes a local PostgreSQL installation — every command runs through Docker.

Without this folder, there is no database, no tables, no roles, and no ingestion path. It is the foundation every other layer (dbt, Airflow, FastAPI) builds on.

---

## Where this fits in the platform

```ascii
data_generation/          ← produces the synthetic CSV dataset
        │
        ▼
database_scripts/         ← YOU ARE HERE
  ├── 01_schema.sql           creates roles, schemas, tables, indexes
  ├── 02_set_passwords.sh     sets role passwords from environment variables
  ├── 03_stored_procedures.sql  ops.* procedures for upserts, monitoring, automation
  ├── load_csv.sh             Scenario A: bulk-loads CSVs directly into PostgreSQL
  ├── stream_producer.py      Scenario B: replays CSVs through Kafka
  ├── db_tests.sql            145 data quality tests — run after either scenario
  └── README_db.md            this file
        │
        ▼
dbt / Airflow / FastAPI   ← reads from marts.* after data is loaded and tested
```

The three SQL files are applied automatically on first container start via the PostgreSQL `docker-entrypoint-initdb.d` mechanism. The shell and Python scripts are run manually (or by Airflow) after the container is up and data has been generated.

---

## File inventory

| File | Type | Purpose |
| --- | --- | --- |
| `01_schema.sql` | DDL | Roles, schemas, all tables, all indexes |
| `02_set_passwords.sh` | Shell | `ALTER ROLE ... PASSWORD` from env vars |
| `03_stored_procedures.sql` | SQL | `ops.*` — upserts, quality gate, monitoring, fraud automation |
| `load_csv.sh` | Shell | Scenario A — bulk COPY from CSVs into PostgreSQL |
| `stream_producer.py` | Python | Scenario B — replay CSVs as Kafka events |
| `db_tests.sql` | SQL | 145 data quality tests — run after either ingestion scenario |

---

## Architecture: Why two ingestion scenarios exist

The data generator produces a complete 24-month synthetic dataset as CSV files. It is fundamentally a batch tool because two of its fraud patterns — benefit-sharing clusters and duplicate claim pairs — require scanning the entire claim history before injection. This makes day-by-day streaming generation impossible.

The two scenarios differ only in how those CSVs reach the database:

```ascii
Scenario A — Bulk load (no Kafka required):
  generator → CSVs → load_csv.sh → PostgreSQL (COPY)

Scenario B — Simulated streaming (Kafka required):
  generator → CSVs → stream_producer.py → Kafka topics
                                               ↓
                                       Kafka Connect (JDBC sink)
                                               ↓
                                          PostgreSQL
```

The database ends up with identical rows either way. Scenario B validates the full Kafka → Kafka Connect → PostgreSQL pipeline path using the same data — the ingestion mechanism changes, the data does not.

---

## Services and containers

The active services in `docker-compose.yml` at the database layer stage are:

| Service | Image | Status | When to use |
| --- | --- | --- | --- |
| **postgres** | `postgres:16-alpine` | Active | Always |
| **generator** | `python:3.11-alpine` (multi-stage) | Active — runs once and exits | Always |
| zookeeper | `confluentinc/cp-zookeeper:7.6.0` | Commented | Enable for Scenario B |
| kafka | `confluentinc/cp-kafka:7.6.0` | Commented | Enable for Scenario B |
| schema-registry | `confluentinc/cp-schema-registry:7.6.0` | Commented | Enable for Scenario B |
| kafka-connect | `confluentinc/cp-kafka-connect:7.6.0` | Commented | Enable for Scenario B |
| debezium | `debezium/connect:2.6` | Commented | Enable for CDC on `fraud_investigation_outcomes` |
| dbt | `ghcr.io/dbt-labs/dbt-postgres:1.8.0` | Commented | After postgres has data |
| airflow | `apache/airflow:2.9.0-python3.11` | Commented | After dbt marts exist |
| fastapi | custom build | Commented | After airflow + marts |

To enable any commented service, uncomment its block in `docker-compose.yml` and run `docker compose up --build`.

---

## First-time setup

```bash
# 1. Create your secrets file and fill in all passwords
cp .env.example .env

# 2. Build images and start postgres + generator
docker compose up --build

# 3. Watch the generator run — it exits automatically when done
docker logs -f afyabima-generator

# 4. Once the generator has exited, load the CSVs into postgres (Scenario A)
docker exec afyabima-postgres bash /sql/load_csv.sh \
  --data-dir /tmp/afyabima_data \
  --user kafka_connect \
  --db afyabima
```

The three SQL files (`01_schema.sql`, `02_set_passwords.sh`, `03_stored_procedures.sql`) are applied automatically by the postgres container on first start via `docker-entrypoint-initdb.d`. They do not need to be run manually. This initdb run is skipped on every subsequent restart — it only fires when the `pgdata` volume is empty.

---

## Connecting with psql (no local PostgreSQL needed)

All psql access goes through the container. The four roles and when to use each:

```bash
# Admin — full access, use for setup and debugging
docker exec -it afyabima-postgres psql -U afyabima_admin -d afyabima

# dbt_runner — reads raw.*, writes staging/intermediate/marts
docker exec -it afyabima-postgres psql -U dbt_runner -d afyabima

# api_reader — reads marts.* only; use to verify least-privilege
docker exec -it afyabima-postgres psql -U api_reader -d afyabima

# kafka_connect — INSERT/UPDATE/DELETE on raw.*
docker exec -it afyabima-postgres psql -U kafka_connect -d afyabima
```

Running a SQL file from the host without copying it in:

```bash
docker exec -i afyabima-postgres \
  psql -U afyabima_admin -d afyabima < db_tests.sql
```

---

## Scenario A — Bulk CSV load

> *This step is optional after running the 4th command in **[First-time setup](## First-time setup)**.*

**When to use:** initial seeding, dev/test resets, or when Kafka infrastructure is not yet running.

```bash
# Basic load
chmod +x load_csv.sh
./database_scripts/load_csv.sh --data-dir ./afyabima_data

# With explicit connection details
PGPASSWORD=secret ./database_scripts/load_csv.sh \
    --data-dir ./afyabima_data \
    --host db.internal \
    --db afyabima \
    --user kafka_connect

# Full reset — truncates all tables before reloading
./database_scripts/load_csv.sh --data-dir ./afyabima_data --clean
```

Load order respects foreign key dependencies:

```txt
Tier 1 (no FK dependencies):  plans, employers, icd10_codes, drug_formulary, providers
Tier 2:                        members, stock_levels
Tier 3:                        claims
Tier 4:                        claim_events, vitals, prescriptions, payments, fraud_investigations
Tier 5:                        fraud_investigation_outcomes
```

After loading, validate:

```bash
# Quick structured pass/fail summary
docker exec afyabima-postgres \
  psql -U afyabima_admin -d afyabima -c "SELECT * FROM ops.post_load_summary();"

# Full 145-test quality suite
docker exec -it afyabima-postgres \
    bash -c "psql -U afyabima_admin -d afyabima < sql/db_tests.sql"
```

---

## Scenario B — Kafka streaming

**When to use:** when Kafka and Kafka Connect are running and you want to validate the full pipeline ingestion path.

### Step 1: Create Kafka topics

```bash
kafka-topics.sh --bootstrap-server localhost:9092 --create \
    --topic afyabima.raw.plans --partitions 1 --replication-factor 1

# Repeat for: employers, icd10_codes, drug_formulary, providers,
#             members, claims, claim_events, vitals, prescriptions,
#             payments, stock_levels, fraud_investigations,
#             fraud_investigation_outcomes
```

Deploy a Kafka Connect JDBC Sink connector for each topic with:

- `insert.mode = upsert`
- `pk.mode = record_key`
- `auto.create = false` (tables already exist from `01_schema.sql`)

### Step 2: Install the Kafka Python client

```bash
uv add kafka-python
```

### Step 3: Run the streaming producer

```bash
# 1 simulated day per real second
python stream_producer.py \
    --data-dir ./afyabima_data \
    --bootstrap-servers localhost:9092 \
    --speed 1.0

# As fast as possible — for load testing
python stream_producer.py \
    --data-dir ./afyabima_data \
    --bootstrap-servers localhost:9092 \
    --speed 0

# Resume from a specific date after a crash
python stream_producer.py \
    --data-dir ./afyabima_data \
    --bootstrap-servers localhost:9092 \
    --speed 1.0 \
    --from-date 2024-06-01
```

What happens internally: reference tables (plans, employers, members) are published to their topics immediately at startup. Transactional records are bucketed by date and emitted one day at a time. Kafka Connect sinks each topic into the corresponding `raw.*` table. The `_loaded_at` column is filled by `DEFAULT now()` on insert — it reflects actual arrival time, not simulated time.

### Step 4: Validate after streaming

```bash
docker exec afyabima-postgres \
  psql -U afyabima_admin -d afyabima -c "SELECT * FROM ops.pipeline_health();"

docker exec -it afyabima-postgres \
    bash -c "psql -U afyabima_admin -d afyabima < sql/db_tests.sql"
```

---

## Stored procedures reference

All procedures live in the `ops` schema. Any role with `EXECUTE` on `ops.*` can call them.

### Quality and validation

```sql
-- Structured pass/fail report — use immediately after a Scenario A load
SELECT * FROM ops.post_load_summary();

-- Full quality gate — returns one row per violation; 0 rows = clean
SELECT * FROM ops.run_quality_gate();

-- Same gate but raises EXCEPTION on any critical violation — use in CI
SELECT ops.assert_quality_gate();
```

### Operational monitoring

```sql
-- Table freshness and row counts for all raw.* tables
SELECT * FROM ops.pipeline_health();

-- Dead letter queue depth by topic; breach_sla = events unresolved for > 7 days
SELECT * FROM ops.dlq_summary();

-- Debezium WAL replication lag
SELECT * FROM ops.replication_lag();
```

### Data operations

```sql
-- Auto-open investigations for claims with fraud_probability > 0.80
SELECT ops.auto_open_investigations('airflow_system');

-- Auto-close investigations open for > 30 days
SELECT ops.close_stale_investigations();

-- Refresh drug formulary from JSON (called by Airflow)
CALL ops.refresh_drug_formulary('[
    {"drug_code": "SHA-001", "drug_name": "Amoxicillin 500mg", ...}
]'::JSONB);
```

### Upsert procedures (called by Kafka Connect and Airflow)

```sql
CALL ops.upsert_claim(
    'CLM-001', 'MBR-001', 'PRV-001',
    '2024-01-15', '2024-01-16', 'CONS-001', 'A09',
    1, 3500.00, 3500.00, 'paid', 'portal',
    FALSE, NULL,
    '2024-01-16T09:00:00Z', '2024-01-20T14:00:00Z'
);

CALL ops.append_claim_event(
    'EVT-001', 'CLM-001', 'paid',
    'approved', 'paid',
    '2024-01-20T14:00:00Z', 'system', NULL
);
```

---

## Automation with Airflow

The `ops` schema is designed to be called from Airflow DAGs. Suggested structure once Airflow is enabled:

```txt
daily_data_quality_dag
├── task: run_quality_gate()     → fail DAG if critical violations found
├── task: pipeline_health()      → alert if any source is STALE
├── task: dlq_summary()          → alert if breach_sla = true
└── task: trigger dbt run        → only if quality gate passed

every_6h_fraud_dag
├── task: auto_open_investigations()
└── task: close_stale_investigations()

daily_reference_refresh_dag
├── task: load plans CSV → upsert_plan()
├── task: load members CSV → upsert_member()
└── task: refresh_drug_formulary()
```

---

## Schema inspection (inside psql)

```sql
-- List all tables in the raw schema
\dt raw.*

-- Describe a table's columns and constraints
\d raw.claims
\d raw.members

-- List all procedures in the ops schema
\df ops.*

-- List all schemas with owners
\dn+

-- List all roles and their attributes
\du

-- Show table-level privileges
\dp raw.*

-- Check exactly what kafka_connect can access
SELECT table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'kafka_connect'
ORDER BY table_schema, table_name;

-- Confirm WAL level (required for Debezium)
SHOW wal_level;

-- Confirm the Debezium publication exists
SELECT * FROM pg_publication;
SELECT * FROM pg_publication_tables;
```

---

## Health and monitoring (inside psql)

```sql
-- Table freshness and row counts
SELECT * FROM ops.pipeline_health();

-- Run all 145 quality tests
SELECT * FROM ops.run_quality_gate();

-- Post-load validation summary
SELECT * FROM ops.post_load_summary();

-- DLQ depth by topic
SELECT * FROM ops.dlq_summary();

-- Debezium WAL lag
SELECT * FROM ops.replication_lag();

-- Active connections by role
SELECT usename, count(*) FROM pg_stat_activity GROUP BY usename ORDER BY count DESC;

-- Slowest queries (requires pg_stat_statements)
SELECT query, calls, total_exec_time, mean_exec_time
FROM pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;
```

---

## Backup and restore

```bash
# Dump in custom format — smaller, supports parallel restore
docker exec afyabima-postgres \
  pg_dump -U afyabima_admin -Fc afyabima \
  > backup_$(date +%Y%m%d_%H%M%S).dump

# Dump a single table
docker exec afyabima-postgres \
  pg_dump -U afyabima_admin -t raw.claims afyabima \
  > claims_$(date +%Y%m%d).sql

# Restore from custom format
docker exec -i afyabima-postgres \
  pg_restore -U afyabima_admin -d afyabima --clean \
  < backup.dump
```

---

## Reset without destroying the database

```bash
docker exec -it afyabima-postgres psql -U afyabima_admin -d afyabima
```

```sql
-- Clear transactional data only; leave reference tables intact
TRUNCATE
    raw.claims,
    raw.vitals,
    raw.prescriptions,
    raw.claim_events,
    raw.payments,
    raw.fraud_predictions,
    raw.fraud_investigation_outcomes,
    raw.dlq_events
CASCADE;

-- Clear everything including reference tables (full re-seed)
TRUNCATE raw.members, raw.employers, raw.providers, raw.plans CASCADE;
```

Full environment reset (all data gone):

```bash
docker compose down -v && docker compose up --build
```

---

## Maintenance

```sql
-- Reclaim space and update planner statistics
VACUUM ANALYZE raw.claims;

-- Database size
SELECT pg_size_pretty(pg_database_size('afyabima'));

-- Table sizes in the raw schema, largest first
SELECT
    tablename,
    pg_size_pretty(pg_total_relation_size('raw.' || tablename)) AS total_size,
    pg_size_pretty(pg_relation_size('raw.' || tablename))       AS table_size,
    pg_size_pretty(pg_indexes_size('raw.' || tablename))        AS index_size
FROM pg_tables
WHERE schemaname = 'raw'
ORDER BY pg_total_relation_size('raw.' || tablename) DESC;
```

---

## Scanning for security vulnerabilities

`docker scan` was deprecated in January 2024. The two current options are:

### docker scout (built into Docker Desktop 4.17+)

```bash
# Requires a free Docker Hub account login
docker scout cves --only-severity critical,high afyabima-postgres:latest
docker scout recommendations afyabima-postgres:latest
```

### trivy (preferred for CI — no login required)

```bash
# Install: brew install aquasecurity/trivy/trivy (macOS)

# Full scan
trivy image --severity CRITICAL,HIGH afyabima-postgres:latest

# Exit code 1 on any CRITICAL CVE — use in CI to fail the pipeline
trivy image --exit-code 1 --severity CRITICAL afyabima-postgres:latest

# SARIF output for GitHub Security tab
trivy image --format sarif -o trivy-results.sarif afyabima-postgres:latest
```

---

## On stored procedures vs plain SQL files

Use stored procedures for logic that runs repeatedly on a schedule, operations that must be atomic (upserts with side effects), and business rules shared across multiple callers (Airflow, FastAPI, dbt). Keep things as plain SQL files for one-off operations like schema creation and initial loads, dbt models (dbt owns its own materialisation), and the test suite (plain SQL keeps it runnable from any client without needing `ops` schema access).

The boundary rule: if a piece of SQL runs on a schedule or is called by more than one system, it belongs in `ops`. If it runs once or is owned by a single tool, keep it as a file.

---

## Logs and debugging

```bash
# Follow postgres logs live
docker logs -f afyabima-postgres

# Generator output
docker logs afyabima-generator

# Live resource usage across all containers
docker stats

# Open a shell in the postgres container (Alpine — use sh not bash)
docker exec -it afyabima-postgres sh
```

---

## Quick reference

| Task | Command |
| --- | --- |
| Start everything | `docker compose up -d` |
| Full reset | `docker compose down -v && docker compose up --build` |
| Open psql (admin) | `docker exec -it afyabima-postgres psql -U afyabima_admin -d afyabima` |
| Open psql (api\_reader) | `docker exec -it afyabima-postgres psql -U api_reader -d afyabima` |
| Run SQL file from host | `docker exec -i afyabima-postgres psql -U afyabima_admin -d afyabima < database_scripts/file.sql` |
| Load CSVs (Scenario A) | `docker exec afyabima-postgres bash /sql/load_csv.sh --data-dir /tmp/afyabima_data` |
| Run quality tests | `docker exec -i afyabima-postgres psql -U afyabima_admin -d afyabima < database_scripts/db_tests.sql` |
| Pipeline health | `docker exec afyabima-postgres psql -U afyabima_admin -d afyabima -c "SELECT * FROM ops.pipeline_health();"` |
| Backup database | `docker exec afyabima-postgres pg_dump -U afyabima_admin -Fc afyabima > backup.dump` |
| Scan for CVEs | `trivy image --severity CRITICAL,HIGH afyabima-postgres:latest` |
| Container logs | `docker logs -f afyabima-postgres` |
| Container stats | `docker stats` |
