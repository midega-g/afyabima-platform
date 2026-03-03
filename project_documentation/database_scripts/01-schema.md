# `01_schema.sql` — PostgreSQL Schema Definition

## 1. What this file is

`01_schema.sql` is the structural foundation of the entire AfyaBima platform. It defines everything the database needs to exist as a purposeful system: the roles that control who can access what, the four schemas that partition data by processing stage, all 17 raw tables that receive data from Kafka and the generator, the empty stubs for the dbt-managed mart tables, and the Debezium publication that powers the fraud feedback loop.

Without this file, there is no database structure at all. Every other component in the stack — Kafka Connect, dbt, Airflow, FastAPI, Debezium, the stored procedures — assumes this file has already run successfully. It is the single document that a new engineer should read first to understand how the platform is organised, who can do what, and why the data is shaped the way it is.

---

## 2. Where it fits

```ascii
docker-entrypoint-initdb.d/
    ├── 01_schema.sql             ← YOU ARE HERE
    │     creates: roles, schemas, all raw.* tables, marts stubs,
    │              indexes, constraints, comments, Debezium publication
    ├── 02_set_passwords.sh       ← sets passwords on the roles created here
    └── 03_stored_procedures.sql  ← creates ops.* procedures; grants to roles created here
          │
          ▼
    Kafka Connect → writes to raw.*
    dbt           → reads raw.*, writes staging / intermediate / marts
    FastAPI       → reads marts.*
    Debezium      → reads raw WAL via logical replication slot
    Airflow       → calls ops.* procedures, reads raw.* and marts.*
```

This file runs first, once, automatically. Everything it creates is permanent for the life of the `pgdata` volume.

---

## 3. Prerequisites

- The `pgdata` Docker volume must be empty — this file only runs during initdb
- No prior PostgreSQL roles named `kafka_connect`, `dbt_runner`, `api_reader`, `debezium`, or `airflow_runner` may exist (the `IF NOT EXISTS` guards handle this safely if they do)
- `wal_level=logical` must be set at the server level before the publication in section 8 can be created — this is handled in `docker-compose.yml` via `command: ["postgres", "-c", "wal_level=logical"]`

---

## 4. Core concepts

**Four-schema architecture** separates data by processing stage, not by subject area.

- `raw` receives data exactly as it arrives.
- `staging` cleans and standardises it (one view per raw table, no joins).
- `intermediate` applies business logic and joins.
- `marts` holds the wide, denormalised tables consumed by applications and dashboards.

Each layer is owned by a different role and can only be written by the tool responsible for it.

**Role-based access control at the schema level** means privileges are granted once on the schema and inherited by every object created inside it. No per-table grants are needed for new tables (except for tables that exist at schema creation time, which require an explicit grant at the end of this file). The boundary rule is simple: each service role can only see the schemas its job requires.

**Natural primary keys** are used throughout `raw.*` — `claim_id`, `member_id`, `provider_id` etc. are the same identifiers that appear in Kafka message keys. This is deliberate: surrogate keys would require a lookup join on every upsert from Kafka Connect. Natural keys make upserts direct and make the raw layer a faithful mirror of the event bus.

**`_loaded_at` on every raw table** is the timestamp at which Kafka Connect (or `load_csv.sh`) wrote the row, not the time the underlying event occurred. It is used by dbt source freshness checks to detect stale ingestion. `submitted_at`, `event_at`, `paid_at` etc. are the business timestamps of the events themselves.

**`IF NOT EXISTS` on every `CREATE`** makes the entire file idempotent. Running it twice on the same database is safe — nothing is dropped or recreated. This matters because the file is also used in ad-hoc recovery scenarios where a developer may be uncertain whether a partial run succeeded.

**`TIMESTAMPTZ` for all timestamps** stores values as UTC and converts at query time based on the session timezone. Kafka events arrive as UTC strings; storing them as `TIMESTAMPTZ` means no data is lost and no manual timezone handling is needed in dbt or the API.

**`NUMERIC(15,2)` for all KES monetary columns** prevents floating-point rounding errors. `FLOAT` or `DOUBLE PRECISION` cannot represent most decimal values exactly, which produces cents-level discrepancies in financial aggregations. `NUMERIC` is exact.

---

## 5. Execution order within the file

The file is structured so each section depends only on what has already been created above it:

| Section | Name/Type | Description / Components |
| :--------: | :---------- | :------------------------- |
| **Section 0** | Extensions | `pgcrypto`, `pg_stat_statements` |
| **Section 1** | Roles | `kafka_connect`, `dbt_runner`, `api_reader`, `debezium`, `airflow_runner` |
| **Section 2** | Schemas + grants | `raw`, `staging`, `intermediate`, `marts` + schema-level `GRANT` / `ALTER DEFAULT PRIVILEGES` |
| **Section 3** | raw reference | `plans` → `employers` → `members` → `providers` → `drug_formulary` → `icd10_codes` |
| **Section 4** | raw transactional | `claims` → `claim_events` → `vitals` → `prescriptions` → `payments` → `stock_levels` |
| **Section 5** | raw fraud pipeline | `fraud_predictions` → `fraud_investigations` → `fraud_investigation_outcomes` → `model_performance_log` → `dlq_events` |
| **Section 6** | staging stubs | Comments only; dbt creates the actual views |
| **Section 7** | marts stubs | `fct_claims`, `fct_fraud_scoring`, `fct_payments`, `dim_members`, `dim_providers`, `mart_claims_performance`, `mart_provider_analytics`, `mart_member_utilisation`, `mart_supply_chain_summary` |
| **Section 8** | Debezium pub | `afyabima_debezium` publication on `raw.fraud_investigation_outcomes` |
| **Section 9** | Explicit grants | Table-level grants for tables that existed before `ALTER DEFAULT PRIVILEGES` |

Reference tables must be created before transactional tables because transactional tables hold foreign keys to them. Fraud pipeline tables come last within `raw` because they reference `raw.claims`.

---

## 6. Component-by-component

### Section 0 — Extensions

```sql
CREATE EXTENSION IF NOT EXISTS "pgcrypto";
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";
```

`pgcrypto` provides `gen_random_uuid()`, used as the default primary key for tables that have no natural business identifier (`stock_levels`, `fraud_predictions`, `fraud_investigations`, `fraud_investigation_outcomes`, `model_performance_log`, `dlq_events`). `pg_stat_statements` records execution statistics for every query and is the data source for the slowest-query monitoring query in the operational runbook.

### Section 1 — Roles

Five service roles are created inside a `DO $$` block so each `CREATE ROLE` is guarded by an `IF NOT EXISTS` check. A plain `CREATE ROLE` without the guard would fail if the role already existed from a previous partial run. No passwords are set here — that responsibility belongs to `02_set_passwords.sh`.

| Role | LOGIN | REPLICATION | Purpose |
| --- | --- | --- | --- |
| `kafka_connect` | yes | no | Writes raw events from Kafka topics |
| `dbt_runner` | yes | no | Reads raw, writes staging/intermediate/marts |
| `api_reader` | yes | no | Reads marts only (FastAPI) |
| `debezium` | yes | yes | Logical replication for CDC |
| `airflow_runner` | yes | no | Calls ops.* procedures, reads quality data |

`debezium` is the only role that requires `REPLICATION`. This privilege allows it to create and consume a logical replication slot, which is what Debezium uses to tail the WAL. Without it, Debezium cannot connect to the replication stream regardless of other grants.

### Section 2 — Schemas and grants

Four schemas are created and immediately granted to the appropriate roles.

**Schema-level `GRANT USAGE`** allows a role to see that a schema exists and address objects inside it. It does not grant access to the objects themselves — that requires a separate `GRANT SELECT` or `GRANT INSERT` on the tables. The two-level grant system (schema USAGE + table privilege) means accidentally granting the wrong table privilege does not also expose the schema structure to unintended roles.

**`ALTER DEFAULT PRIVILEGES`** is the mechanism that makes future tables in a schema automatically inherit the right grants without requiring manual `GRANT` statements after each `CREATE TABLE`. It instructs PostgreSQL that any table created in `raw` by the current user should automatically receive `SELECT` for `dbt_runner` and `airflow_runner`, and `INSERT/UPDATE/DELETE` for `kafka_connect`. Tables that already existed at the time `ALTER DEFAULT PRIVILEGES` was issued are not affected — this is why section 9 contains explicit grants.

The full access matrix:

| Role | `raw` | `staging` | `intermediate` | `marts` |
| --- | --- | --- | --- | --- |
| `kafka_connect` | `INSERT`, `UPDATE`, `DELETE` | — | — | — |
| `dbt_runner` | `SELECT` | ALL | ALL | ALL |
| `api_reader` | — | — | — | `SELECT` |
| `debezium` | `SELECT` (via replication slot) | — | — | — |
| `airflow_runner` | `SELECT` | — | — | `SELECT` |

### Section 3 — raw reference tables

Six tables that change infrequently and are refreshed in full by Airflow daily. They are loaded before transactional tables in every ingestion scenario because transactional tables hold foreign keys to them.

**`raw.plans`** — insurance product definitions. `plan_tier` is constrained to five values. All three limit columns have `CHECK (> 0)` constraints enforcing that a plan with a zero limit cannot be inserted.

**`raw.employers`** — corporate clients. `contract_end` is nullable (open-ended contracts) but must be after `contract_start` when set. `member_count` is informational and can lag behind the actual member table.

**`raw.members`** — insured beneficiaries. Four indexes serve specific query patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_members_plan_code` | Used when dbt joins claims → members to enrich with plan details (60% of fact queries) |
| `idx_members_employer_id` | Supports mart aggregations grouping by employer; prevents full table scans |
| `idx_members_county` | Enables regional rollups in `mart_member_utilisation` without scanning all members |
| `idx_members_is_active` | dbt snapshots filter on `is_active = TRUE` to capture only current members |

`employer_id` is nullable for individually enrolled members. `term_date` is NULL while a member is active and set only when coverage lapses.

**`raw.providers`** — contracted healthcare facilities. `accreditation_status` constrains to four lifecycle values. `risk_score` is a `NUMERIC(4,3)` value between 0 and 1 recomputed weekly by Airflow from claims history — it starts NULL on insert and is updated as history accumulates.

Three indexes target the most frequent access patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_providers_county` | Used by mart aggregations and regional fraud analysis |
| `idx_providers_accreditation_status` | Airflow quality checks filter on non-active statuses |
| `idx_providers_facility_type` | Supports provider network analytics and gap analysis |

**`raw.drug_formulary`** — SHA-approved drug schedule. `is_essential_medicine` and `is_controlled_substance` are boolean flags used by prescription validation rules in dbt and the fraud pipeline. The index `idx_drug_formulary_drug_class` supports therapeutic class analysis in supply chain marts.

**`raw.icd10_codes`** — WHO ICD-10 diagnosis code reference. Code, description, and clinical category. Seeded once from CSV and updated only when WHO releases a new version. The index `idx_icd10_codes_clinical_category` enables category-based aggregations (e.g., "all infectious disease claims").

### Section 4 — raw transactional tables

Six tables that grow continuously as claims move through the system. These are the primary targets for Kafka Connect writes.

**`raw.claims`** — the central fact of the system. Every other transactional table references it. The claim `status` column is constrained to nine lifecycle values. The two most important constraints: `claims_service_before_submit_chk` (service date cannot be after submission date — a common fraud signal when violated) and `claims_approved_lte_claimed_chk` (approved amount cannot exceed claimed amount).

Seven indexes support the access patterns required by dbt, fraud detection, and API queries:

| Index | Purpose |
| ------- | --------- |
| `idx_claims_member_id`, `provider_id`, `diagnosis_code` | Standard FK lookups — every mart join uses at least one of these |
| `idx_claims_date_of_service` | Range scans for time-windowed fraud features (e.g., "claims in last 7 days"). B-tree chosen because `date_of_service` can be backdated (not append-only) |
| `idx_claims_status` | Airflow quality checks and dashboards filtering on `status = 'flagged_fraud'` |
| `idx_claims_submitted_at` | dbt source freshness monitoring — identifies ingestion delays |
| `idx_claims_dup_detection` | **Composite index** specifically for duplicate claim detection. Covering all four columns in one index lets the query below run in milliseconds rather than scanning the entire table. |

  ```sql
  SELECT claim_id FROM raw.claims 
  WHERE member_id = :x AND provider_id = :y 
    AND diagnosis_code = :z 
    AND date_of_service BETWEEN now() - interval '48 hours' AND now()
  ```

**`raw.claim_events`** — an immutable append-only log of every state transition a claim passes through. It is never updated after insert. `triggered_by` constrains who or what can generate an event to five known actors. Two indexes serve the required query patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_claim_events_claim_id` | FK lookup — every investigation that needs claim history uses this |
| `idx_claim_events_event_at` | **BRIN index** — events arrive in chronological order. The BRIN index is 1/10th the size of a B-tree and serves all time-range queries (e.g., "events in last hour") with equivalent performance |

**`raw.vitals`** — clinical measurements at point of service. All numeric columns have physiologically bounded `CHECK` constraints: blood pressure, heart rate, temperature, weight, height, BMI, and SpO2 are all guarded against impossible values. The absence of a vitals row for a consultation claim is itself a fraud signal — the phantom-billing pattern. Two indexes cover the essential access paths:

| Index | Purpose |
| ------- | --------- |
| `idx_vitals_claim_id` | Primary lookup path — joining vitals back to claims for fraud detection |
| `idx_vitals_member_id` | Used when building member clinical history for risk adjustment models |

**`raw.prescriptions`** — one row per drug prescribed per claim. References `raw.drug_formulary` directly, meaning prescriptions for drugs not in the formulary are rejected at the database level, not by application code. `dispensed_on_site` distinguishes whether the pharmacy dispensed at the facility (TRUE) or the member collected externally. Indexes support the three main access patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_prescriptions_claim_id` | Joining prescriptions back to claims for completeness checks |
| `idx_prescriptions_member_id` | Member medication history queries |
| `idx_prescriptions_drug_code` | Drug utilization analysis and formulary compliance monitoring |

**`raw.payments`** — one row per payment disbursement. A single claim can have multiple payment rows (partial payments or phased disbursements). `payment_method` constrains to four known channels. Two indexes serve the required queries:

| Index | Purpose |
| ------- | --------- |
| `idx_payments_claim_id` | FK lookup — all payment inquiries start from a claim |
| `idx_payments_paid_at` | **BRIN index** — payments accumulate chronologically. Used by financial reporting and reconciliation queries |

**`raw.stock_levels`** — daily pharmacy stock snapshots. The natural key would be `(facility_id, drug_code, reported_date)` but a surrogate UUID is used as the primary key because the combination would make the primary key large and expensive to reference. The natural key is enforced separately as a `UNIQUE` constraint. Three indexes support supply chain analytics:

| Index | Purpose |
| ------- | --------- |
| `idx_stock_levels_facility_id` | All queries for a specific facility's inventory use this |
| `idx_stock_levels_drug_code` | Drug-level stockout analysis across facilities |
| `idx_stock_levels_reported_date` | **BRIN index** — chronological snapshots. Enables time-series analysis without scanning full table |
| `stock_levels_facility_drug_date_uq` | **Unique constraint** — enforces business rule: one snapshot per facility/drug/date |

### Section 5 — raw fraud pipeline tables

Five tables that form the fraud detection and feedback loop. These are not written by Kafka Connect directly — they are written by Flink, FastAPI, the PySpark retraining job, and Airflow.

**`raw.fraud_predictions`** — one row per claim per model scoring run. `feature_snapshot` is a `JSONB` column that records the exact feature values used at scoring time. This is not for querying convenience — it is for regulatory explainability. If a claim is disputed, the insurer must be able to show what the model saw. Five indexes support the required access patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_fraud_predictions_claim_id` | FK lookup — used when investigations need to see the prediction that triggered them |
| `idx_fraud_predictions_model_version` | Airflow model comparison queries (e.g., "how did v2.3 perform vs v2.2?"). Without this, evaluation would scan all predictions |
| `idx_fraud_predictions_risk_tier` | API and dashboard filters for risk-based triage ("show all critical claims in last 24h") |
| `idx_fraud_predictions_predicted_at` | **BRIN index** — predictions are generated in real-time, monotonically increasing. Used by dbt to incrementally process new predictions since last run |
| `idx_fraud_predictions_features_gin` | **GIN index** on `feature_snapshot` JSONB. Regulatory audit queries filter on specific feature values (e.g., "claims where feature_snapshot $\rightarrow$'provider_risk_score' > 0.8"). Without GIN, these would require full-table scans |

The GIN index is the largest in this table (~20% of table size) — this is an accepted trade-off for auditability.

**`raw.fraud_investigations`** — the investigation lifecycle. Updated by FastAPI as investigators work through cases. References both `raw.claims` and optionally `raw.fraud_predictions` (the specific prediction that triggered the investigation). An investigation can exist without a corresponding prediction if it was opened manually. Three indexes cover the essential query patterns:

| Index | Purpose |
| ------- | --------- |
| `idx_fraud_investigations_claim_id` | FK lookup — find all investigations for a given claim |
| `idx_fraud_investigations_assigned_to` | Investigator workload queries ("show all cases assigned to user X") |
| `idx_fraud_investigations_status` | Operational dashboards filtering by `status IN ('open', 'in_progress')` |

**`raw.fraud_investigation_outcomes`** — the final investigator verdict. INSERT-only — once an outcome is recorded it is never modified. The constraint `outcomes_one_per_investigation_uq` enforces exactly one outcome per investigation. `outcomes_loss_requires_fraud_chk` enforces that `estimated_loss_kes` can only be set when `final_label = 'confirmed_fraud'` — a legitimate claim has no loss to record. This is the most important table in the fraud feedback loop: Debezium watches it and publishes every new row to the `investigations.closed` Kafka topic, which feeds the ML retraining pipeline.

Four indexes support the required access patterns, with careful consideration of which columns are updated vs. insert-only:

| Index | Purpose |
| ------- | --------- |
| `idx_outcomes_claim_id` | FK lookup — joining outcomes back to claims for mart enrichment |
| `idx_outcomes_investigation_id` | FK lookup — linking to investigations table (unique constraint also serves this) |
| `idx_outcomes_final_label` | Used by retraining pipeline to filter only confirmed fraud cases. High selectivity (~3% of rows are fraud) |
| `idx_outcomes_confirmed_at` | **BRIN index** — outcomes are written once, in chronological order. Used by dbt to incrementally process new labels for retraining |

Note: No index on `fraud_type` — cardinality is low (5 values) and it's rarely used as a standalone filter without `final_label`.

**`raw.model_performance_log`** — metrics written by the PySpark retraining job after each training run. `auc_roc`, `precision_at_10pct`, `recall`, and `f1_score` are all constrained to `[0, 1]`. The `promoted` column is set by Airflow after it reads this table and decides whether the new model version outperforms the current champion. Two indexes support the promotion decision workflow:

| Index | Purpose |
| ------- | --------- |
| `idx_model_perf_evaluation_date` | Time-ordered queries for trend analysis ("how is model performance trending?") |
| `idx_model_perf_promoted` | Quick lookup of current champion model(s). Partial index consideration: `WHERE promoted = TRUE` would be more efficient if this query becomes frequent |

**`raw.dlq_events`** — the dead-letter queue sink. When Kafka Connect cannot write a message to its target table (schema mismatch, constraint violation, etc.), it routes the failed message here instead. `raw_payload` stores the original Kafka message as JSONB. The partial index `WHERE resolved = FALSE` on the `resolved` column means the index only covers unresolved records — as records are resolved, they drop out of the index automatically, keeping it small. Airflow monitors this table and alerts when unresolved depth exceeds 10.

Four indexes serve the DLQ monitoring and resolution workflow:

| Index | Purpose |
| ------- | --------- |
| `idx_dlq_source_topic` | Identifying which topics are generating the most failures |
| `idx_dlq_resolved` | **Partial index** `WHERE resolved = FALSE` — keeps the active monitoring index tiny; resolved records are excluded automatically |
| `idx_dlq_failed_at` | **BRIN index** — chronological failure patterns. Used to detect if failures spike at certain times |
| `idx_dlq_error_code` | (Not currently indexed) — if error-code analysis becomes frequent, add B-tree index |

The DLQ design prioritises operational monitoring over analytical queries — hence the partial index on unresolved records and BRIN for time-based analysis.

### Section 6 — staging and intermediate stubs

No tables are created in these schemas. The section consists of schema-level comments only. **dbt** creates the actual views when it runs.

**Why stubs are needed:** PostgreSQL requires a schema to exist before any objects can be created in it. More importantly, the `GRANT USAGE` statements in Section 2 would fail if the schemas didn't exist at grant time. By creating the schemas here (even empty), we enable:

- Role permissions to be set before dbt runs
- Connection tests from Airflow and FastAPI to validate authentication
- `ALTER DEFAULT PRIVILEGES` to take effect for future objects dbt creates

**What dbt actually creates:**

- `staging` — one view per raw table. Responsible for renaming, casting, and basic deduplication. No joins.
- `intermediate` — business logic and joins. Never accessed directly by applications.

Both are **views**, not tables — they add no storage cost and always reflect the latest raw data. dbt recreates them on every run.

**Schema-level comments** (already present) explain this to anyone who connects via psql:

```sql
COMMENT ON SCHEMA staging IS
    'One view per raw table. Rename, cast, deduplicate. No joins. Created by dbt.';
```

### Section 7 — marts stubs

Nine empty tables created so that grants and API connection tests can be validated before dbt populates them. **dbt** runs `INSERT` or `DELETE + INSERT` on these on every pipeline run. The stubs have the same column definitions that dbt will write — any mismatch would be caught as a dbt error on first run.

**Why stubs instead of letting dbt create them?** Three reasons:

1. **Grants** — `api_reader` and `airflow_runner` need `SELECT` permission on marts tables. Grants can only be applied to existing tables.
2. **Connection testing** — FastAPI health checks can verify it can query the tables before data exists.
3. **Schema validation** — Any mismatch between stub definitions and dbt models is caught immediately on first run.

**Important notes about each mart table:**

| Table | Key Points |
| ------- | --------- |
| **`marts.fct_claims`** | Fact table with fraud context. `claim_id` is the natural key — matches raw.claims one-to-one. No SCD. |
| **`marts.fct_fraud_scoring`** | Links predictions to final labels. `was_correct` is populated after outcomes are known. Enables model performance monitoring. |
| **`marts.fct_payments`** | One row per payment. A claim may have multiple rows. Includes `employer_id` for employer-level financial reporting. |
| **`marts.dim_members`** | **SCD Type 2** — the only slowly-changing dimension. `member_sk` is a surrogate key; `dbt_valid_from`/`_to` define the time window; `dbt_is_current` flags the active row. History is preserved when attributes change. |
| **`marts.dim_providers`** | Type 1 dimension (overwrite). Risk metrics (`total_claims_30d`, `fraud_rate_90d`) are recomputed nightly. |
| **`marts.mart_claims_performance`** | Monthly aggregate. `employer_id` and `county` can be NULL. Uses a `UNIQUE` index with `COALESCE` (see detailed explanation below). |
| **`marts.mart_provider_analytics`** | Daily provider metrics. `analytics_date` is the date of aggregation, not service date. Primary key is `(provider_id, analytics_date)`. |
| **`marts.mart_member_utilisation`** | Monthly member-level utilisation against plan limits. `ytd_utilisation_pct` is year-to-date percentage of annual limit consumed. |
| **`marts.mart_supply_chain_summary`** | Daily stock snapshot with derived fields. `is_below_reorder` is calculated; `stockout_days_30d` is a rolling window metric. |

**Special design notes:**

**`marts.dim_members` surrogate key** — The only mart table with a synthetic primary key. This is required for SCD Type 2 because a single `member_id` can appear in multiple rows (different time periods). `member_sk` is typically `member_id || '_' || dbt_scd_id` in practice.

**`marts.mart_claims_performance` unique constraint** — The `UNIQUE` index using `COALESCE` solves a PostgreSQL limitation: standard unique constraints treat multiple NULLs as distinct. Without this, you could have two rows with the same `(summary_month, plan_code)` where one has NULL employer and the other also has NULL employer — both would be allowed, violating business rules. `COALESCE(employer_id, '')` converts NULL to an empty string, making them comparable and enforcing true uniqueness.

**`marts.fct_fraud_scoring.was_correct`** — This column is NULL until an investigation outcome is recorded. It is updated when `fraud_investigation_outcomes` is written, not at prediction time. This enables retrospective accuracy analysis without reprocessing.

**No column comments on marts tables** — Column-level comments are intentionally omitted here because:

- Mart schemas are fully managed by dbt, which can (and should) document them in `schema.yml` files
- The stubs exist only for grants and testing; actual usage reads from dbt documentation
- Duplicating comments would create a maintenance burden (they'd need to stay in sync with dbt)

**What happens on first dbt run:** dbt will:

1. Validate that the stub schemas match its expectations
2. Truncate and reload or incrementally update each table
3. Apply its own documentation and testing framework

The stubs are scaffolding — they enable the platform to start cleanly, but dbt takes over ownership from the first pipeline run onward.

### Section 8 — Debezium logical replication publication

```sql
CREATE PUBLICATION afyabima_debezium
    FOR TABLE raw.fraud_investigation_outcomes;
```

**What WAL (Write-Ahead Log) is:** PostgreSQL records every change to the database in a sequential log called the WAL before it writes to the actual data files. This ensures durability — if the server crashes, it can replay the WAL to recover. **Logical replication** reads the WAL and decodes changes into a format (row inserts/updates/deletes) that can be sent to external systems.

**Why Debezium is the right tool:** Debezium is a CDC (Change Data Capture) platform that connects to PostgreSQL's logical replication stream and publishes each change as an event to Kafka. Compared to alternatives:

- **Polling queries** (`SELECT * FROM table WHERE updated_at > ?`) would miss deletes and add load
- **Triggers** would require writing to a separate table and add per-transaction overhead
- **LISTEN/NOTIFY** doesn't capture the changed data itself, only notifies that something happened

Debezium provides exactly-once semantics, schema evolution handling, and integrates natively with Kafka — the core event bus of the platform.

**Why only this table:** The publication declares which tables Debezium monitors. Only `raw.fraud_investigation_outcomes` is included — not the entire `raw` schema — for three reasons:

1. **WAL volume** — Every INSERT/UPDATE/DELETE in `raw` generates WAL traffic. Monitoring all tables would flood the replication slot with irrelevant events (thousands of claim inserts, vital sign updates, etc.) for every one outcome row we actually need.
2. **Focus** — The fraud feedback loop is the only pipeline that requires real-time CDC. Other tables are consumed by Kafka Connect via JDBC sinks on a schedule.
3. **Slot management** — Each replication slot reserves WAL segments until they're consumed. A slot monitoring the entire `raw` schema would force PostgreSQL to retain WAL much longer, increasing disk usage.

**What happens when an outcome is inserted:**

```txt
1. INSERT into raw.fraud_investigation_outcomes
2. PostgreSQL writes the change to WAL
3. Debezium (connected as 'debezium' role) reads the logical decoding stream
4. Debezium transforms the row into a Kafka message (Avro, with schema)
5. Message published to 'investigations.closed' Kafka topic
6. ML retraining pipeline (consuming that topic) adds the labelled outcome to training dataset
```

**Critical prerequisites:**

```sql
-- 1. WAL level must be logical (set at server start, not in SQL)
SHOW wal_level;  -- Must return 'logical'

-- 2. debezium role must have REPLICATION privilege (already granted in Section 1)
SELECT rolname, rolreplication FROM pg_roles WHERE rolname = 'debezium';

-- 3. The publication must exist before Debezium connects
SELECT * FROM pg_publication WHERE pubname = 'afyabima_debezium';

-- 4. Replication slot must be created by Debezium (automatic on first connection)
-- View active slots:
SELECT slot_name, slot_type, active FROM pg_replication_slots;
```

**`wal_level=logical`** is set in `docker-compose.yml` because it requires a server restart — it cannot be changed dynamically:

```yaml
# docker-compose.yml
command: postgres -c wal_level=logical -c max_replication_slots=5 -c max_wal_senders=5
```

**Replication slot management:** Each Debezium connector creates a replication slot. Slots must be monitored — if a slot becomes inactive (connector down), WAL segments accumulate and can fill disk. The operations runbook includes:

```sql
-- Monitor slot lag (bytes not yet consumed)
SELECT slot_name, database, active, pg_wal_lsn_diff(
    pg_current_wal_lsn(), 
    restart_lsn
) as lag_bytes
FROM pg_replication_slots;
```

**If Debezium is down:** WAL continues to accumulate until the slot is resumed or dropped. After 24 hours, this can reach gigabytes. Recovery procedure:

1. Fix the connector configuration
2. Restart Debezium connector — it resumes from where it left off
3. If slot is corrupted, delete and recreate (requires re-snapshot)

**Security considerations:** The replication stream contains all data from the monitored table — including sensitive investigation notes. The `debezium` role has `SELECT` on the table and `REPLICATION` privilege, but the connection must be over SSL in production to prevent wire-level eavesdropping.

**Why not use a trigger instead?** A trigger could write to an `outgoing_events` table, and Kafka Connect could poll that table. This would work but adds:

- Per-transaction overhead (every INSERT would fire the trigger)
- Need to track what's been sent (offset management)
- Risk of duplicate or missed events on failure

Debezium's WAL-based approach has no impact on transaction performance and provides exactly-once semantics with offset tracking in Kafka.

Here is the rewritten section, incorporating your existing text and the suggested enhancements into a single, comprehensive document.

### Section 9 — Explicit table-level grants

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO kafka_connect;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_runner, airflow_runner, debezium;
...
```

#### Why explicit grants are needed

`ALTER DEFAULT PRIVILEGES` (configured in [Section 2](#section-2--schemas-and-grants)) only applies to tables created **after** the default is set. The tables in `raw`, `staging`, `intermediate`, and `marts` were all created in Sections 3–7 *before* the `ALTER DEFAULT PRIVILEGES` statements in Section 2 took effect. This section retroactively applies the correct permissions to every existing table using `GRANT ... ON ALL TABLES IN SCHEMA`.

Without this section, all the tables would exist but none of the service roles would be able to read or write them.

#### Complete privilege matrix

The following table summarises the effective permissions after both default privileges and this section are applied.

| Role | `raw` (all tables) | `raw` (specific) | `staging` | `intermediate` | `marts` |
| --- | :--- | --- | :---: | :--- | :--- |
| **`kafka_connect`** | `SELECT`, `INSERT`, `UPDATE`, `DELETE` | `INSERT` on `dlq_events` | — | — | — |
| **`dbt_runner`** | `SELECT` | — | `ALL` | `ALL` | `ALL` |
| **`api_reader`** | — | — | — | — | `SELECT` |
| **`debezium`** | `SELECT` | — | — | — | — |
| **`airflow_runner`** | `SELECT` | `INSERT` on `model_performance_log` | — | — | `SELECT` |

*Note: `ALL` privilege includes `SELECT`, `INSERT`, `UPDATE`, `DELETE`, `TRUNCATE`, `REFERENCES`, and `TRIGGER`.*

#### Rationale for key grants

| Grant | Why It Exists |
| :--- | :--- |
| **`kafka_connect`** with `INSERT`, `UPDATE`, `DELETE` on `raw` | Kafka Connect performs upserts on `raw.claims` (requiring `UPDATE`) and writes new records (`INSERT`). `DELETE` is rarely used but necessary for potential compaction or cleanup operations. |
| **`dbt_runner`** with only `SELECT` on `raw` | dbt reads raw data to build staging, intermediate, and marts models, but it **never writes directly to `raw`**. All writes to `raw` come exclusively from Kafka Connect. This strict separation prevents accidental dbt modifications to source data. |
| **`airflow_runner`** with `INSERT` on `raw.model_performance_log` | The PySpark retraining job runs as an Airflow task and writes model evaluation metrics directly to this table. This is the **only** write access any role has to the `raw` schema besides `kafka_connect`. It is intentionally narrow. |
| **`debezium`** with only `SELECT` on `raw` | Logical replication reads the WAL, not the tables directly, to capture changes. However, the `debezium` role still needs `SELECT` permission to perform the initial snapshot of the table. The required `REPLICATION` privilege was granted at role creation time in Section 1. |
| **`airflow_runner`** with `SELECT` on `marts` | Airflow runs data quality checks against the final marts tables to ensure they meet freshness and accuracy SLAs. |
| **`kafka_connect`** with `INSERT` on `raw.dlq_events` | This is a separate grant from the bulk `raw` grant because it serves a different purpose. If Kafka Connect fails to write to a target table, it writes the failed message to this dead-letter queue. This permission ensures that even when the primary sink fails, the error can still be recorded. |

#### Notable omissions (intentional)

Every grant is chosen to give each role exactly the permissions it needs—**nothing more**. This "principle of least privilege" limits the potential impact if any credential is ever compromised.

| Missing Grant | Why It's Deliberate |
| :--- | :--- |
| **`api_reader`** on `raw` | The API should never read raw, unaggregated data. It only serves data from the `marts` schema. |
| **`airflow_runner`** on `staging` / `intermediate` | Airflow monitors data quality by comparing `raw` and `marts` tables. It has no need to read the intermediate layers. |
| **`debezium`** on any table other than `raw.fraud_investigation_outcomes` | Change Data Capture (CDC) is scoped to exactly one table to minimise WAL volume and replication slot overhead (see Section 8). |
| **`dbt_runner`** on `raw.dlq_events` | The dead-letter queue is for operational monitoring and manual replay; dbt transformations should never read from it. |

#### Principle of least privilege

The permission model is designed to contain security breaches. If a credential is compromised, the damage is limited to the bare minimum required for that service to function:

- **Compromised `api_reader`** → Attacker can only see aggregated `marts` data, not raw member details or claims.
- **Compromised `airflow_runner`** → Attacker can read `raw` and `marts`, and can write to `model_performance_log`, but cannot modify any other `raw` data or access `staging`/`intermediate`.
- **Compromised `debezium`** → Attacker can `SELECT` from `raw.fraud_investigation_outcomes` but cannot write to any table.

---

## 7. Index strategy

The index choices follow three rules applied consistently across all tables.

**1. Every foreign key column gets a B-tree index.** PostgreSQL does not create indexes on foreign keys automatically, but FK lookups are extremely common — every join between claims and members, for example, traverses the `member_id` FK. Without indexes on FK columns, these joins degrade to sequential scans.

**2. Append-only tables with monotonically increasing timestamps use `BRIN` indexes** rather than B-tree. BRIN (Block Range Index) stores the min/max value per range of disk blocks. It is tiny compared to B-tree and extremely fast for range scans on columns that are physically ordered by insertion time — which is exactly the case for `event_at`, `paid_at`, `predicted_at`, `confirmed_at`, `failed_at`, and `reported_date`. A B-tree on these columns would be much larger and provide no meaningful benefit over BRIN for range queries.

**3. `JSONB` columns get `GIN` indexes only where queries actually filter on their contents.** `feature_snapshot` in `raw.fraud_predictions` gets a GIN index because dbt audit queries filter on specific feature values. `metadata` in `raw.claim_events` does not — it is queried by retrieving the whole row, not by filtering on its contents.

> **Detailed index explanations** for each table (including the specific queries each index serves) are documented in the table's own section:
>
> - Reference tables: [Section 3](#section-3--raw-reference-tables)
> - Transactional tables: [Section 4](#section-4--raw-transactional-tables)
> - Fraud pipeline tables: [Section 5](#section-5--raw-fraud-pipeline-tables)

### 7.1 Index Trade-offs

Every index improves read performance at the cost of write performance and disk space. The current index set balances these factors for the expected workload:

| Trade-off | Impact | Acceptance Rationale |
| ----------- | -------- | ---------------------- |
| **Write slowdown** | Each additional index adds ~10-20% overhead to `INSERT`/`UPDATE` | Kafka Connect batch writes (~5000 rows/batch) absorb this overhead; indexes are essential for query performance |
| **Disk space** | Indexes typically consume 20-40% of table size | Storage is cheap; query performance is critical |
| **BRIN vs. B-tree** | BRIN indexes are ~10x smaller but slightly slower for very narrow time ranges | The space savings outweigh the minimal performance difference for our time-series queries |

**Not every column that *could* be indexed *is* indexed.** The following columns were deliberately left unindexed:

| Table | Column | Why No Index? |
| ------- | -------- | --------------- |
| `raw.claims` | `amount_claimed` | Numeric range scans are rare; filtering by amount happens in dbt after joins, not directly on raw table |
| `raw.prescriptions` | `dispensed_on_site` | Low cardinality (boolean) and not used as a standalone filter |
| `raw.stock_levels` | `quantity_on_hand` | Always queried with facility + drug filters first; the existing composite index covers it |

### 7.2 Future Index Candidates

These indexes are not currently present but may be needed as query patterns evolve. Monitor the queries running in production and add them if needed:

| Table | Candidate Index | Trigger to Add |
| ------- | ----------------- | ---------------- |
| `raw.claims` | `(date_of_service, status)` | If fraud team frequently queries "pending_review claims from last 7 days" |
| `raw.members` | `(plan_code, is_active)` | If plan-level active member counts become a frequent dashboard query |
| `raw.fraud_predictions` | `(model_version, predicted_at)` | If model comparison queries need time windows per version |
| `raw.dlq_events` | `(error_code)` | If error-code analysis becomes a common debugging step |

**How to decide:** Use `pg_stat_user_indexes` and `pg_stat_user_tables` to identify sequential scans that could benefit from new indexes:

```sql
SELECT schemaname, tablename, seq_scan, seq_tup_read, idx_scan
FROM pg_stat_user_tables
WHERE seq_scan > 1000 AND seq_tup_read > 1000000
ORDER BY seq_scan DESC;
```

### 7.3 What to Avoid

Over-indexing is a common mistake. This schema intentionally avoids:

- **Indexes on low-cardinality columns** (e.g., `gender`, `is_emergency`) — they're rarely selective enough to help
- **Composite indexes that duplicate other indexes** — e.g., `(member_id, date_of_service)` would duplicate `idx_claims_member_id` for most queries
- **Indexes on columns never used in `WHERE`, `JOIN`, or `ORDER BY`** — they'd only slow down writes

**Example of an index NOT created:**

```sql
-- NOT CREATED: Gender is only used in aggregations, never as a filter
CREATE INDEX idx_members_gender ON raw.members(gender);  -- Don't do this
```

### 7.4 Index Maintenance

PostgreSQL automatically maintains indexes during `INSERT`, `UPDATE`, and `DELETE`. However, over time, indexes can become bloated. Monitor index bloat with:

```sql
-- Check index bloat (run monthly)
SELECT schemaname, tablename, indexname, 
       pg_size_pretty(pg_relation_size(indexrelid)) as index_size
FROM pg_stat_user_indexes
ORDER BY pg_relation_size(indexrelid) DESC;
```

If any index grows unexpectedly large, consider `REINDEX` during a maintenance window:

```sql
REINDEX INDEX CONCURRENTLY idx_claims_dup_detection;
```

---

## 8. Key design decisions

**Why natural primary keys in `raw.*` instead of surrogates?** Kafka message keys are the natural identifiers. Using surrogate keys in `raw` would require a lookup on every Kafka Connect upsert to find the internal ID, adding a round-trip per message. Natural keys make upserts direct: `ON CONFLICT (claim_id) DO UPDATE` operates on the key that already exists in the message. Surrogate keys are appropriate in `marts` (where SCD2 requires them) but not in `raw`.

**Why are `staging` and `intermediate` views rather than tables?** dbt materialises them. Defining them as tables here would create two sources of truth — the DDL stub and the dbt model definition. Views are the right materialisation for these layers because they add no storage cost, always reflect the latest raw data, and are recreated on every dbt run without needing `DROP + CREATE` on tables with dependent objects.

**Why does `raw.fraud_investigation_outcomes` have `outcomes_loss_requires_fraud_chk`?** A constraint that cross-references two columns (`estimated_loss_kes` and `final_label`) enforces a business rule at the database level rather than in application code. Application code can be bypassed — a direct psql insert, an Airflow bug, a FastAPI edge case. A `CHECK` constraint cannot. This particular rule prevents the common data quality issue of recording a financial loss against an investigation that concluded as legitimate.

**Why is Debezium limited to one table rather than the whole `raw` schema?** WAL volume. Every row change in the `raw` schema generates a WAL record. If Debezium watched all of `raw`, it would process tens of thousands of irrelevant records (claim inserts, vital sign updates, payment rows) for every one `fraud_investigation_outcomes` row it actually needs. Scoping the publication to exactly one table keeps the replication slot small and the CDC pipeline focused.

**Why use `COALESCE` in the unique index on `marts.mart_claims_performance` instead of a standard constraint?** PostgreSQL's `UNIQUE` constraint uses `IS DISTINCT FROM` semantics for NULLs — two NULL values are considered distinct, so a standard `UNIQUE (summary_month, plan_code, employer_id, county)` would allow multiple rows with the same month/plan but both having NULL employer. `COALESCE` in the index converts NULLs to a non-null sentinel value, restoring the intended semantics: one row per (month, plan, employer, county) combination, treating NULL employer as a distinct grouping.

---

## 9. How to run

This file runs automatically via `docker-entrypoint-initdb.d` on first container start. It does not need to be run manually in normal operation.

To verify it ran correctly:

```bash
# Check logs for errors during initdb
docker logs afyabima-postgres | grep -E "ERROR|FATAL|initdb"

# Inspect the resulting schema
docker exec -it afyabima-postgres psql -U afyabima_admin -d afyabima
```

```sql
-- Confirm all four schemas exist
\dn+

-- Confirm all five roles exist with login capability
SELECT rolname, rolcanlogin, rolreplication
FROM pg_roles
WHERE rolname IN ('kafka_connect','dbt_runner','api_reader','debezium','airflow_runner');

-- Confirm raw tables were created
\dt raw.*

-- Confirm marts stubs were created
\dt marts.*

-- Confirm the Debezium publication exists
SELECT * FROM pg_publication;

-- Confirm role privileges on raw schema
SELECT table_name, privilege_type, grantee
FROM information_schema.role_table_grants
WHERE table_schema = 'raw'
ORDER BY table_name, grantee;

-- Confirm WAL level (required for Debezium)
SHOW wal_level;
```

To force a re-run (e.g. after a schema change):

```bash
# Full reset — destroys all data
docker compose down -v && docker compose up --build
```

---

## 10. What this connects to

`02_set_passwords.sh` runs immediately after this file and sets passwords on the five roles created in section 1. Without that script, the roles have no credentials and no service can authenticate.

`03_stored_procedures.sql` runs after that and creates the `ops` schema, with procedures that operate on the tables defined here. It also issues `GRANT EXECUTE` statements referencing `airflow_runner` and `dbt_runner` — both of which must exist before those grants can be issued.

Once all three initdb files have run, the database is ready for data: `load_csv.sh` (Scenario A) or `stream_producer.py` + Kafka Connect (Scenario B) populate the `raw.*` tables, dbt then reads them to build `staging`, `intermediate`, and `marts`, and FastAPI reads from `marts.*` using the `api_reader` role.

---

## 11. Things worth carrying forward

The four-schema layering pattern — raw / staging / intermediate / marts — is not specific to AfyaBima. It is the standard dbt project structure, and understanding why each layer exists (raw = faithful copy, staging = clean copy, intermediate = joined copy, marts = wide copy for consumption) makes the entire transformation pipeline easier to reason about.

The distinction between `ALTER DEFAULT PRIVILEGES` (for future tables) and `GRANT ... ON ALL TABLES` (for existing tables) is a frequent source of confusion in PostgreSQL. This file uses both deliberately — understanding why section 9 exists even though section 2 already has grants is a good test of whether the privilege system is properly understood.

BRIN vs B-tree is a decision worth internalising. The rule of thumb: if the physical order of rows on disk is correlated with the column you are indexing (timestamps on append-only tables are the canonical example), BRIN gives you 99% of the query benefit at 1% of the index size. If the data is randomly distributed relative to the index column (most lookup columns), B-tree is the right choice.

The feedback loop through `raw.fraud_investigation_outcomes` → Debezium → Kafka → ML retraining is the architectural feature that makes AfyaBima a self-improving system rather than a static model. The fact that it is initiated by a database publication rather than application code means investigator verdicts are captured reliably regardless of which application surface (FastAPI, a future mobile app, a direct psql update by an admin) writes the outcome row.
