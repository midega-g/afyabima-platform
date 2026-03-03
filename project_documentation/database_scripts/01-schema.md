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

**`raw.claims`** — the central fact of the system. Every other transactional table references it. The claim `status` column is constrained to nine lifecycle values. The two most important constraints: `claims_service_before_submit_chk` (service date cannot be after submission date — a common fraud signal when violated) and `claims_approved_lte_claimed_chk` (approved amount cannot exceed claimed amount). The composite index `idx_claims_dup_detection` on `(member_id, provider_id, diagnosis_code, date_of_service)` is built specifically for the duplicate claim fraud detection query.

**`raw.claim_events`** — an immutable append-only log of every state transition a claim passes through. It is never updated after insert. `triggered_by` constrains who or what can generate an event to five known actors. The `BRIN` index on `event_at` is efficient here because new events always have timestamps later than existing ones — BRIN is designed for exactly this monotonically increasing pattern.

**`raw.vitals`** — clinical measurements at point of service. All numeric columns have physiologically bounded `CHECK` constraints: blood pressure, heart rate, temperature, weight, height, BMI, and SpO2 are all guarded against impossible values. The absence of a vitals row for a consultation claim is itself a fraud signal — the phantom-billing pattern.

**`raw.prescriptions`** — one row per drug prescribed per claim. References `raw.drug_formulary` directly, meaning prescriptions for drugs not in the formulary are rejected at the database level, not by application code. `dispensed_on_site` distinguishes whether the pharmacy dispensed at the facility (TRUE) or the member collected externally.

**`raw.payments`** — one row per payment disbursement. A single claim can have multiple payment rows (partial payments or phased disbursements). `payment_method` constrains to four known channels. The `BRIN` index on `paid_at` follows the same reasoning as `claim_events` — payments accumulate chronologically.

**`raw.stock_levels`** — daily pharmacy stock snapshots. The natural key would be `(facility_id, drug_code, reported_date)` but a surrogate UUID is used as the primary key because the combination would make the primary key large and expensive to reference. The natural key is enforced separately as a `UNIQUE` constraint. The `BRIN` index on `reported_date` serves the supply chain mart aggregations.

### Section 5 — raw fraud pipeline tables

Five tables that form the fraud detection and feedback loop. These are not written by Kafka Connect directly — they are written by Flink, FastAPI, the PySpark retraining job, and Airflow.

**`raw.fraud_predictions`** — one row per claim per model scoring run. `feature_snapshot` is a `JSONB` column that records the exact feature values used at scoring time. This is not for querying convenience — it is for regulatory explainability. If a claim is disputed, the insurer must be able to show what the model saw. The `GIN` index on `feature_snapshot` supports dbt audit queries that filter on specific feature values.

**`raw.fraud_investigations`** — the investigation lifecycle. Updated by FastAPI as investigators work through cases. References both `raw.claims` and optionally `raw.fraud_predictions` (the specific prediction that triggered the investigation). An investigation can exist without a corresponding prediction if it was opened manually.

**`raw.fraud_investigation_outcomes`** — the final investigator verdict. INSERT-only — once an outcome is recorded it is never modified. The constraint `outcomes_one_per_investigation_uq` enforces exactly one outcome per investigation. `outcomes_loss_requires_fraud_chk` enforces that `estimated_loss_kes` can only be set when `final_label = 'confirmed_fraud'` — a legitimate claim has no loss to record. This is the most important table in the fraud feedback loop: Debezium watches it and publishes every new row to the `investigations.closed` Kafka topic, which feeds the ML retraining pipeline.

**`raw.model_performance_log`** — metrics written by the PySpark retraining job after each training run. `auc_roc`, `precision_at_10pct`, `recall`, and `f1_score` are all constrained to `[0, 1]`. The `promoted` column is set by Airflow after it reads this table and decides whether the new model version outperforms the current champion.

**`raw.dlq_events`** — the dead-letter queue sink. When Kafka Connect cannot write a message to its target table (schema mismatch, constraint violation, etc.), it routes the failed message here instead. `raw_payload` stores the original Kafka message as JSONB. The partial index `WHERE resolved = FALSE` on the `resolved` column means the index only covers unresolved records — as records are resolved, they drop out of the index automatically, keeping it small. Airflow monitors this table and alerts when unresolved depth exceeds 10.

### Section 6 — staging and intermediate stubs

No tables are created here. The section consists of schema-level comments only. `dbt` creates the actual views when it runs. The schemas exist so that grants and connection tests can be performed before dbt has run for the first time.

### Section 7 — marts stubs

Nine empty tables created so that grants and API connection tests can be validated before dbt populates them. `dbt` runs `INSERT` or `DELETE + INSERT` on these on every pipeline run. The stubs have the same column definitions that dbt will write — any mismatch would be caught as a dbt error on first run.

`marts.dim_members` is the only mart table with a surrogate primary key (`member_sk`). It implements SCD Type 2: each row represents a member's attributes for a specific time window, managed by dbt snapshots. The `dbt_valid_from`, `dbt_valid_to`, and `dbt_is_current` columns are the SCD2 bookkeeping fields.

`marts.mart_claims_performance` has a composite primary key on `(summary_month, plan_code)` but also a `UNIQUE` index using `COALESCE` on nullable columns. This is because `employer_id` and `county` can be NULL (some claims have no employer, some summaries are not county-scoped), and a standard `UNIQUE` constraint treats two NULLs as non-equal — meaning two rows with the same month and plan but both having NULL employer would not be caught. `COALESCE(employer_id, '')` converts NULL to an empty string, making NULLs comparable for uniqueness purposes.

### Section 8 — Debezium logical replication publication

```sql
CREATE PUBLICATION afyabima_debezium
    FOR TABLE raw.fraud_investigation_outcomes;
```

A PostgreSQL publication declares which tables Debezium monitors for CDC (change data capture). Only `raw.fraud_investigation_outcomes` is included — not the entire `raw` schema — to minimise WAL volume. Every INSERT into this table triggers a WAL record that Debezium reads and publishes to the `investigations.closed` Kafka topic. The ML retraining pipeline consumes that topic to update the training dataset with labelled outcomes.

`wal_level=logical` must be active for this publication to work. It is set in `docker-compose.yml` at server start, not in this file — PostgreSQL does not allow `wal_level` to be changed via SQL; it requires a server restart with the parameter in `postgresql.conf` or the command line.

### Section 9 — Explicit table-level grants

```sql
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO kafka_connect;
GRANT SELECT ON ALL TABLES IN SCHEMA raw TO dbt_runner, airflow_runner, debezium;
...
```

`ALTER DEFAULT PRIVILEGES` only applies to tables created after it is issued. The tables created in sections 3–5 of this file already existed by the time the grants in section 2 were set. Section 9 retroactively applies the correct grants to all of them using `GRANT ... ON ALL TABLES IN SCHEMA`. Without this section, all the tables would exist but none of the service roles would be able to read or write them.

Two specific grants deserve attention: `GRANT INSERT ON raw.model_performance_log TO airflow_runner` gives Airflow write access to exactly one table in `raw` (the PySpark job submits via Airflow). `GRANT INSERT ON raw.dlq_events TO kafka_connect` is separate from the bulk grant because DLQ writes need to be explicit — Kafka Connect should be able to write dead-letter records even if its sink target table is the one it cannot write to.

---

## 7. Index strategy

The index choices follow three rules applied consistently across all tables.

Every foreign key column gets a B-tree index. PostgreSQL does not create indexes on foreign keys automatically, but FK lookups are extremely common — every join between claims and members, for example, traverses the `member_id` FK. Without indexes on FK columns, these joins degrade to sequential scans.

Append-only tables with monotonically increasing timestamps use `BRIN` indexes rather than B-tree. BRIN (Block Range Index) stores the min/max value per range of disk blocks. It is tiny compared to B-tree and extremely fast for range scans on columns that are physically ordered by insertion time — which is exactly the case for `event_at`, `paid_at`, `predicted_at`, `confirmed_at`, `failed_at`, and `reported_date`. A B-tree on these columns would be much larger and provide no meaningful benefit over BRIN for range queries.

`JSONB` columns get `GIN` indexes only where queries actually filter on their contents. `feature_snapshot` in `raw.fraud_predictions` gets a GIN index because dbt audit queries filter on specific feature values. `metadata` in `raw.claim_events` does not — it is queried by retrieving the whole row, not by filtering on its contents.

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
