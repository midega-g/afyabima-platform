# `03_stored_procedures.sql` — Stored Procedures & Automation Layer

## 1. What this file is

`03_stored_procedures.sql` is the operational brain of the AfyaBima database layer. Where `01_schema.sql` defines structure and `02_set_passwords.sh` enables authentication, this file defines behaviour — what the database can do on behalf of the services that connect to it.

Without this file, every service that needs to write data, check quality, or monitor the pipeline would have to embed that logic in application code. Kafka Connect would need custom SQL in its connector config. Airflow would need inline SQL in its DAGs. FastAPI would need health-check queries in its Python. Instead, all of that logic lives here in a single, versioned, tested place, accessible to any caller with `EXECUTE` privilege — regardless of the language, framework, or tool doing the calling.

The file creates one schema (`ops`) and seventeen routines across six functional groups. It is the last of the three initdb files to run.

---

## 2. Where it fits

```
docker-entrypoint-initdb.d/
    ├── 01_schema.sql            ← raw.* tables, roles, grants
    ├── 02_set_passwords.sh      ← credentials on those roles
    └── 03_stored_procedures.sql ← YOU ARE HERE
          │  creates ops.* schema and all routines
          │
          ▼
    Kafka Connect  → CALL ops.upsert_claim(), ops.append_claim_event()
    Airflow DAGs   → SELECT ops.run_quality_gate(), ops.auto_open_investigations()
    FastAPI        → SELECT ops.pipeline_health(), ops.dlq_summary()
    dbt            → SELECT ops.run_quality_gate(), ops.assert_quality_gate()
    load_csv.sh    → SELECT ops.post_load_summary()  (after bulk load)
```

The `ops` schema is a clean namespace boundary. Nothing in `raw`, `staging`, `intermediate`, or `marts` is touched by this file structurally — it only adds callable routines that operate on those schemas.

---

## 3. Prerequisites

- `01_schema.sql` must have run successfully — all `raw.*` tables and all five service roles must exist before this file runs
- `02_set_passwords.sh` must have run — the roles need passwords to authenticate before they can be granted `EXECUTE`
- The `pgdata` volume need not be empty — unlike the other two initdb files, this file uses `CREATE OR REPLACE` throughout, which means it is safe to re-run against a live database after procedure changes without a full volume reset

---

## 4. Core concepts

**`PROCEDURE` vs `FUNCTION` in PostgreSQL** — this file uses both deliberately. `PROCEDURE` (called with `CALL`) can manage its own transactions — it can `COMMIT` or `ROLLBACK` mid-body. `FUNCTION` (called with `SELECT`) runs inside the caller's transaction and cannot commit independently. All upserts are `PROCEDURE` because they modify data and need transactional control. All monitoring and quality checks are `FUNCTION` because they are read-only and return result sets that `SELECT` can consume directly, including inside Airflow operators and dbt macros.

**`CREATE OR REPLACE`** makes every routine in this file idempotent. Unlike `01_schema.sql` which uses `IF NOT EXISTS`, `CREATE OR REPLACE` actually replaces the existing routine body on re-run. This is the right behaviour for procedures: schema objects like tables are dangerous to drop and recreate (data loss), but procedure bodies have no state and are safe to overwrite. This means procedure changes can be deployed by re-running only this file — no volume reset required.

**`RETURN QUERY` inside a `FUNCTION`** is the PostgreSQL pattern for streaming rows out of a function one query at a time. Each `RETURN QUERY` statement appends its result set to the function's output. The function does not exit — it continues executing subsequent `RETURN QUERY` statements. This is how `ops.run_quality_gate()` runs a dozen independent quality checks and returns all violations in a single result set.

**`ON CONFLICT ... DO UPDATE` vs `DO NOTHING`** — the upsert procedures use different conflict strategies for different tables. `DO UPDATE` is used for tables with mutable state (claims advance through statuses; member addresses change; plan limits are revised). `DO NOTHING` is used for `raw.claim_events` because that table is deliberately immutable — a replayed Kafka event should be silently ignored, not used to overwrite the original record.

**`HAVING COUNT(*) > 0`** in the quality gate — every test in `ops.run_quality_gate()` uses this pattern. A `SELECT ... HAVING COUNT(*) > 0` returns zero rows when there are no violations and one row when there are. This is the cleanest way to make the function return nothing on pass and a row on fail, which is exactly what Airflow needs to evaluate — zero rows from the whole function means all tests passed.

**`GET DIAGNOSTICS v_count = ROW_COUNT`** — this is the PostgreSQL way to capture how many rows were affected by the most recent DML statement inside a PL/pgSQL block. Used in `ops.close_stale_investigations()` after the `UPDATE` to know how many investigations were auto-closed without running a separate `SELECT COUNT(*)`.

---

## 5. Execution order within the file

| Section | Routines | Called by |
| --- | --- | --- |
| **0** — ops schema | (schema creation only) | — |
| **1** — Upsert procedures | `upsert_plan`, `upsert_member`, `upsert_claim`, `append_claim_event` | Kafka Connect, Airflow |
| **2** — Reference data refresh | `refresh_drug_formulary` | Airflow |
| **3** — Quality gate | `run_quality_gate`, `assert_quality_gate` | Airflow, dbt |
| **4** — Operational monitoring | `pipeline_health`, `dlq_summary`, `replication_lag` | Airflow, FastAPI, Grafana |
| **5** — Fraud automation | `auto_open_investigations`, `close_stale_investigations` | Airflow |
| **6** — Post-load validation | `post_load_summary` | Manual / load_csv.sh |
| **7** — Grants | (privilege assignments only) | — |

The ordering within the file matters only for the grants section, which must come last because it references routines that must already exist.

---

## 6. Component-by-component

### Section 0 — `ops` schema

```sql
CREATE SCHEMA IF NOT EXISTS ops;
```

The `ops` schema is a clean namespace that keeps operational routines visually and structurally separated from application data schemas. A developer running `\df raw.*` or `\df marts.*` sees only application-facing objects. `\df ops.*` is the unambiguous entry point for anything that operates on the system. `IF NOT EXISTS` makes this safe to re-run.

---

### Section 1 — Upsert procedures

Four procedures that provide safe, idempotent writes for the two most frequently updated entities (`claims` and `claim_events`) and the two most frequently refreshed reference tables (`plans` and `members`).

#### `ops.upsert_plan`

Inserts a new plan or updates all mutable fields on conflict. Every column is mutable — plan names, tiers, and financial limits can all change when the insurer revises its product catalogue. `_loaded_at` is refreshed to `now()` on every update, not preserved from the original insert, because an updated plan should appear fresh to dbt source freshness checks.

```sql
CALL ops.upsert_plan('NAT-001', 'National Basic', 'basic', 500000, 300000, 200000, 2500, FALSE);
```

#### `ops.upsert_member`

Inserts a new member or updates mutable fields on conflict. The key design decision is in the `DO UPDATE` block — `member_id`, `date_of_birth`, `id_number`, and `enrol_date` are deliberately excluded from the update list. These are immutable identity fields: a member's date of birth does not change, and their ID number and enrolment date are audit-critical. Only contact details (`phone_number`, `county`, `sub_county`), coverage details (`plan_code`, `employer_id`, `term_date`), and status (`is_active`) are allowed to advance.

```sql
CALL ops.upsert_member('MBR-001', 'Jane', 'Doe', '1985-04-12', 'F',
    'National ID', '12345678', '0712345678', 'Nairobi', 'Westlands',
    '2024-01-01', NULL, 'NAT-001', 'EMP-001', TRUE);
```

#### `ops.upsert_claim`

The most carefully designed upsert in the file. Claims are the most frequently updated entity — a single claim passes through up to nine status values over its lifecycle. Three protections guard against data corruption:

**Immutable field protection** — `member_id`, `provider_id`, `date_of_service`, `procedure_code`, `diagnosis_code`, `amount_claimed`, and `submitted_at` are excluded from the `DO UPDATE` set. These define what the claim *is*. Only fields that advance with the claim lifecycle (`status`, `amount_approved`, `prior_auth_number`, `updated_at`) are mutable.

**`COALESCE` on nullable mutable fields** — `amount_approved` and `prior_auth_number` use `COALESCE(EXCLUDED.value, raw.claims.value)`. This means an incoming Kafka message with a NULL `amount_approved` (e.g. a status update before adjudication) does not overwrite a previously recorded non-NULL approved amount. The existing value is preserved.

**Out-of-order delivery guard** — the `WHERE raw.claims.updated_at <= EXCLUDED.updated_at` clause at the end of the conflict block means the update only proceeds if the incoming record is at least as recent as the stored record. Kafka does not guarantee strict ordering across partitions. Without this guard, a delayed message carrying `status = 'submitted'` could arrive after a message carrying `status = 'paid'` and roll the claim back to an earlier state.

```sql
CALL ops.upsert_claim('CLM-001', 'MBR-001', 'PRV-001',
    '2024-01-15', '2024-01-16', 'CONS-001', 'A09',
    1, 3500.00, 3500.00, 'paid', 'portal',
    FALSE, NULL, '2024-01-16T09:00:00Z', '2024-01-20T14:00:00Z');
```

#### `ops.append_claim_event`

The simplest procedure in the file, and deliberately so. `claim_events` is an immutable append-only log — once an event is written it must never change. The entire conflict strategy is `ON CONFLICT (event_id) DO NOTHING`. If the same event is replayed from a Kafka topic (e.g. during a consumer restart), the duplicate insert is silently discarded. No update, no error, no noise in the logs.

| Procedure | Conflict strategy | Why |
| --- | --- | --- |
| `upsert_plan` | `DO UPDATE` all fields | Plans can be revised |
| `upsert_member` | `DO UPDATE` mutable fields only | Identity fields are immutable |
| `upsert_claim` | `DO UPDATE` mutable fields + recency guard | Status must only advance |
| `append_claim_event` | `DO NOTHING` | Event log must never change |

---

### Section 2 — Reference data refresh

#### `ops.refresh_drug_formulary`

Accepts a JSONB array of drug objects and upserts each one into `raw.drug_formulary`. The procedure loops over `jsonb_array_elements(p_rows)`, extracting each field with the `->>'key'` operator and casting to the appropriate type. The counter `v_count` accumulates the number of rows processed and is emitted as a `RAISE NOTICE` at the end — this surfaces in Airflow task logs without requiring a separate `SELECT`.

Unlike `upsert_plan` and `upsert_member`, this procedure accepts a full payload rather than individual columns, because the formulary update arrives as a single JSON document from the SHA API rather than as individual Kafka messages. The JSONB input format also means the same procedure can be called from Python (Airflow), SQL (psql), or any HTTP client that can POST JSON — the interface is format-agnostic.

```sql
CALL ops.refresh_drug_formulary('[
    {"drug_code": "SHA-001", "drug_name": "Amoxicillin 500mg",
     "drug_class": "Antibiotics", "is_essential_medicine": true,
     "is_controlled_substance": false, "unit_cost_kes": 45.00}
]'::JSONB);
```

**What this does not do:** it does not deactivate drugs that are absent from the incoming payload. If a drug is removed from the formulary, it remains in `raw.drug_formulary` with its existing data until explicitly deleted. This is intentional — active prescriptions reference existing drug codes, and deleting a drug code would violate the foreign key constraint from `raw.prescriptions`. Deactivation logic (e.g. adding an `is_active` flag to `drug_formulary`) is a future enhancement.

---

### Section 3 — Quality gate

Two functions that expose the platform's core data quality logic as callable database objects.

#### `ops.run_quality_gate()`

Returns one row per violation across fifteen quality tests, grouped into five categories. Zero rows means all tests pass.

| Test ID | Severity | Category | What it checks |
| --- | --- | --- | --- |
| `FK-01` | critical | FK integrity | `claims.member_id` resolves to a member |
| `FK-02` | critical | FK integrity | `claims.provider_id` resolves to a provider |
| `FK-03` | critical | FK integrity | `prescriptions.drug_code` resolves to formulary |
| `FK-04` | critical | FK integrity | `outcome.claim_id` matches its parent investigation's `claim_id` |
| `FIN-01` | critical | Financial | `amount_approved` does not exceed `amount_claimed` |
| `FIN-02` | critical | Financial | Total payments per claim do not exceed `amount_approved` |
| `FIN-03` | critical | Financial | No payments exist against rejected or fraud-flagged claims |
| `EVT-01` | critical | Event log | Every claim has at least one event in `claim_events` |
| `EVT-02` | critical | Event log | Every `paid` claim has a `paid` event |
| `FRD-01` | critical | Fraud pipeline | `confirmed_fraud` outcomes have a `fraud_type` |
| `FRD-02` | critical | Fraud pipeline | Closed investigations have an outcome row |
| `TMP-01` | warning | Temporal | No claims with `date_of_service` in the future |
| `TMP-02` | warning | Temporal | No submission lag greater than 90 days |
| `TMP-03` | warning | Temporal | No payments made before the claim was submitted |
| `FRS-01` | warning | Freshness | `raw.claims` updated within the last hour |
| `FRS-02` | warning | Freshness | `raw.members` refreshed today |
| `DLQ-01` | warning | DLQ backlog | No unresolved DLQ events older than 7 days |

The `critical` vs `warning` distinction maps directly to Airflow behaviour: a critical violation causes the quality gate DAG task to fail, which blocks the downstream dbt run. A warning sends an alert but allows dbt to proceed. This is the separation between "the data is structurally broken and dbt will produce wrong results" versus "something looks odd but the pipeline can continue."

One test (`FIN-02`) deserves special attention — it appears twice in the file. The first attempt uses a `HAVING` clause that cannot operate on a subquery aggregate in this context and is correctly noted with a comment. The corrected form wraps the per-claim aggregation in a subquery and counts violations in the outer query. This is worth noting for any future contributor: the first block with `LIMIT 0` is a deliberate placeholder acknowledging the pattern limitation, not dead code.

**Airflow usage:**
```sql
-- In a PythonOperator or SQLCheckOperator:
SELECT COUNT(*) FROM ops.run_quality_gate() WHERE severity = 'critical';
-- Returns 0 → pass (proceed to dbt)
-- Returns > 0 → fail (block dbt, alert on-call)
```

#### `ops.assert_quality_gate()`

A thin wrapper around `run_quality_gate()` for use in contexts that need a go/no-go signal rather than a structured result set. It calls `run_quality_gate()` internally, counts critical and warning rows using the `FILTER` aggregate clause, and raises a PostgreSQL `EXCEPTION` if any critical violations exist or a `WARNING` if only warnings are found.

The `EXCEPTION` will cause any calling transaction to be rolled back, making this safe to include in migration scripts where you want to abort the entire migration if data quality is compromised. The `RAISE NOTICE` on a clean pass gives a confirmation line in the logs.

```sql
-- In a migration script — aborts if any critical violations exist:
SELECT ops.assert_quality_gate();

-- In Airflow — same effect via a SQLSensor:
SELECT ops.assert_quality_gate();
```

---

### Section 4 — Operational monitoring

Three read-only functions that expose live pipeline metrics to Airflow, FastAPI, and Grafana without requiring direct table access.

#### `ops.pipeline_health()`

Returns one row per monitored source table with four fields: the table name, the timestamp of the most recent `_loaded_at`, the lag in minutes since that load, and a computed `status` of `OK`, `STALE`, or `NEVER LOADED`.

The staleness thresholds are intentionally different per source:

| Source | Stale threshold | Rationale |
| --- | --- | --- |
| `raw.claims` | > 1 hour | Claims stream continuously; an hour without updates signals a Kafka Connect outage |
| `raw.members`, `raw.providers`, `raw.plans` | Not updated today | Reference data refreshes daily via Airflow; missing a day is the alert boundary |
| `raw.fraud_predictions` | > 2 hours | Flink scores within 500ms of ingestion; 2-hour lag signals a Flink job failure |

The function uses a `VALUES` clause to build the source list inline, avoiding a separate metadata table. Each `VALUES` row contains a subquery for `MAX(_loaded_at)` and `COUNT(*)`. This is less elegant than a loop but executes as a single query plan and is easy to extend by adding a new row to the `VALUES` list.

```sql
SELECT * FROM ops.pipeline_health();
-- source               | last_loaded          | lag_minutes | row_count | status
-- raw.claims           | 2026-03-01 14:32:00  | 2.3         | 45821     | OK
-- raw.fraud_predictions| 2026-03-01 12:00:00  | 152.4       | 44103     | STALE
```

#### `ops.dlq_summary()`

Aggregates `raw.dlq_events` by `source_topic` for unresolved records and returns the count, the oldest failure timestamp, the maximum retry count, and a boolean `breach_sla` flag that is `TRUE` when the oldest unresolved failure is more than 7 days old. Results are ordered by `unresolved DESC` so the most problematic topic appears first.

`breach_sla` is computed as `MIN(failed_at) < NOW() - INTERVAL '7 days'` — if even the oldest failure in a topic is within the SLA window, the flag is FALSE. Airflow's DLQ monitoring task alerts if any row has `breach_sla = TRUE`.

```sql
SELECT * FROM ops.dlq_summary();
-- source_topic           | unresolved | oldest_failure       | max_retries | breach_sla
-- afyabima.raw.claims    | 3          | 2026-02-20 08:00:00  | 5           | true
```

#### `ops.replication_lag()`

Queries `pg_replication_slots` for any slot whose name starts with `debezium` and returns the lag in bytes and megabytes between the current WAL position and the slot's last confirmed flush position. A slot that is inactive gets `INACTIVE` status regardless of lag. A lag over 100 MB gets `HIGH LAG` status. Otherwise `OK`.

`pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)` is the standard PostgreSQL expression for replication lag. `confirmed_flush_lsn` is the position up to which the slot consumer (Debezium) has confirmed it has processed and flushed — WAL below this point can be recycled. The difference between current WAL and this position is the backlog.

```sql
SELECT * FROM ops.replication_lag();
-- slot_name         | lag_bytes | lag_mb | status
-- debezium_afyabima | 1048576   | 1.00   | OK
```

---

### Section 5 — Fraud pipeline automation

Two functions that implement the investigation lifecycle rules as scheduled database operations. In a full streaming deployment, `auto_open_investigations` is mirrored by equivalent logic in the Flink job. These procedures exist so the same behaviour is available in Scenario A (no Kafka/Flink) and can be called by Airflow directly.

#### `ops.auto_open_investigations(p_assigned_to TEXT DEFAULT 'system')`

Finds all claims that have a `fraud_probability > 0.80` prediction and no active investigation, then opens one for each. The function performs two writes per qualifying claim inside its loop:

First, it inserts a new row into `raw.fraud_investigations`. The `investigation_id` is generated as `'INV-' || encode(gen_random_bytes(8), 'hex')` — a human-readable prefix with a random 16-character hex suffix. Priority is mapped from the fraud probability: `>= 0.95` → `critical`, `>= 0.90` → `high`, everything above `0.80` → `medium`.

Second, it updates `raw.claims` to set `status = 'under_investigation'` — but only if the claim is not already in that status or a terminal state (`closed`, `paid`). This prevents the function from overwriting a claim that has already been paid or closed by a concurrent process.

Three filters govern which claims qualify: `fraud_probability > 0.80` (the risk threshold), no existing non-closed investigation (the deduplication guard), and `predicted_at >= NOW() - INTERVAL '30 days'` (the recency window, preventing the function from acting on months-old predictions during catch-up runs).

```sql
-- Called by Airflow every 6 hours:
SELECT ops.auto_open_investigations('airflow_system');
-- Returns: number of new investigations opened
```

#### `ops.close_stale_investigations()`

Finds all investigations in `open` or `in_progress` status that were opened more than 30 days ago and sets them to `closed`. It appends a note to the `notes` field using `COALESCE(notes, '') || ' [Auto-closed...]'` — `COALESCE` prevents `NULL || text` from producing NULL, which would silently erase any existing notes.

`GET DIAGNOSTICS v_count = ROW_COUNT` captures the number of rows the preceding `UPDATE` affected without a separate count query. The count is returned as the function's return value and also emitted as a `RAISE NOTICE` that appears in Airflow task logs.

The purpose of this function extends beyond hygiene. `raw.fraud_investigation_outcomes` is the table Debezium monitors for the ML retraining feedback loop. If investigations are never closed, outcomes are never written, and the retraining pipeline receives no new labelled data. Auto-closing stale investigations after 30 days ensures the pipeline continues to receive a minimum flow of labelled outcomes even when the fraud team has a backlog.

```sql
-- Called by Airflow every 6 hours, after auto_open_investigations:
SELECT ops.close_stale_investigations();
-- Returns: number of investigations auto-closed
```

---

### Section 6 — Post-load validation summary

#### `ops.post_load_summary()`

A convenience function intended to be called immediately after `load_csv.sh` completes a bulk data load. It returns a structured result table with one row per check, where each row has a `check_name`, a `result` (`OK`, `PASS`, `FAIL`, or `WARN`), a `value` showing the actual count or finding, and a `notes` field explaining what was checked.

It performs four categories of checks: row counts for all eleven tables (always `OK` — these are informational), FK orphan checks for the two most critical relationships (`claims → members`, `prescriptions → drug_formulary`), a duplicate PK check on `raw.claims`, a business rule check (`paid` claims must have payment records), and finally calls `ops.run_quality_gate()` internally to surface any critical violations as a single summary row.

The function is designed to give a first-time loader immediate confidence that the data arrived correctly, with enough detail to identify the problem if something is wrong, and a pointer to `run_quality_gate()` for the full diagnostic.

```sql
-- Immediately after load_csv.sh:
SELECT * FROM ops.post_load_summary();

-- check_name                      | result | value              | notes
-- row_count.plans                 | OK     | 5                  | insurance products
-- row_count.claims                | OK     | 1460               | claims submitted
-- fk.claims_members               | PASS   | 0 orphans          | claims.member_id → members
-- fk.prescriptions_formulary      | PASS   | 0 orphans          | prescriptions.drug_code → drug_formulary
-- pk.claims_unique                | PASS   | 0 duplicates       | raw.claims PK uniqueness
-- business.paid_claims_have_payments | PASS | 0 paid claims missing payment | ...
-- quality_gate.critical           | PASS   | 0 critical violations | Run: SELECT * FROM ops.run_quality_gate()...
```

---

### Section 7 — Grants

All routines in `ops` are private by default — `GRANT USAGE ON SCHEMA ops` plus specific `GRANT EXECUTE` on individual routines controls exactly which role can call what.

| Role | What it can call | Why |
| --- | --- | --- |
| `airflow_runner` | All routines in `ops` | Airflow runs quality gates, monitoring, reference refresh, and fraud automation |
| `dbt_runner` | `run_quality_gate`, `assert_quality_gate`, `post_load_summary` | dbt checks quality before running models; nothing else |
| `api_reader` | `pipeline_health`, `dlq_summary`, `replication_lag` | FastAPI `/health` endpoint surfaces these to ops dashboards |
| `kafka_connect` | `upsert_plan`, `upsert_member`, `upsert_claim`, `append_claim_event` | JDBC sink connector calls these instead of raw `INSERT` |

`airflow_runner` receives `GRANT EXECUTE ON ALL ROUTINES IN SCHEMA ops` — a blanket grant rather than per-routine grants. This is intentional: Airflow orchestrates the entire platform and should have access to every operational lever. The other roles receive only what they need.

`kafka_connect` receives `GRANT EXECUTE ON PROCEDURE` (not `FUNCTION`) for its four upserts, which reflects the PostgreSQL distinction between procedure and function. A JDBC sink connector calls procedures via `CALL` and functions via `SELECT` — granting the wrong type would cause a runtime authentication error even if the logic is otherwise correct.

---

## 7. Key design decisions

**Why stored procedures rather than application-side SQL?** The boundary rule used throughout this project is: if a piece of SQL is called by more than one system or runs on a schedule, it belongs in `ops`. `upsert_claim` is called by both Kafka Connect and `load_csv.sh`. `run_quality_gate` is called by Airflow, dbt, and ad-hoc psql sessions. Keeping this logic in the database means a single change to the procedure is immediately available to all callers — there is no need to deploy new application code, update connector configs, or modify DAG files.

**Why is `upsert_claim` a procedure with a `WHERE` recency guard?** Kafka guarantees at-least-once delivery and does not guarantee ordering across partitions. A claim that moves from `submitted` to `approved` to `paid` may generate three Kafka messages. If the `approved` message is delayed, it could arrive after `paid` and roll back a completed claim. The `WHERE raw.claims.updated_at <= EXCLUDED.updated_at` guard makes the upsert conditional — the update only proceeds if the incoming record is at least as recent as the stored one. This is the database-level equivalent of a compare-and-swap operation.

**Why does `auto_open_investigations` also update `raw.claims`?** Two tables need to reflect the same state transition: a new investigation row in `raw.fraud_investigations` and an updated `status` in `raw.claims`. Doing both in a single procedure call ensures they stay in sync atomically. If the update to `raw.claims` fails (e.g. the claim is already closed), the investigation insert still proceeds — the `status NOT IN (...)` guard on the `UPDATE` is deliberately permissive, not a rollback condition.

**Why does `refresh_drug_formulary` accept JSONB rather than individual columns?** The SHA formulary arrives as a bulk JSON document, not as a stream of individual messages. A JSONB interface matches the upstream data format directly and makes the Airflow task simple: read the file, pass it as a parameter, done. It also means the same procedure can be called from Python with a JSON-serialised list or from psql with a `::JSONB` cast, without any glue code to flatten the structure.

**Why are freshness thresholds hardcoded in `pipeline_health()` rather than parameterised?** The thresholds (`1 hour` for claims, `today` for reference tables, `2 hours` for fraud predictions) reflect the SLAs of the upstream systems — Kafka Connect running every few minutes, Airflow running daily, Flink running sub-second. Parameterising them would add flexibility but would also mean the SLAs could drift from the documented values without any code change. Hardcoding them makes the SLA explicit in the code and detectable in code review.

**Why `DO NOTHING` and not `DO UPDATE` for `append_claim_event`?** The event log's value comes from its immutability. An investigator reviewing a claim's history must be able to trust that the sequence of events is exactly what happened, not a best-effort approximation that could have been partially overwritten by replayed messages. `DO NOTHING` on conflict is a hard guarantee that once an event is written, it cannot be changed by any future call to this procedure.

---

## 8. How to run

This file runs automatically via `docker-entrypoint-initdb.d` on first container start. For procedure updates after the initial setup, it can be re-run directly without a volume reset:

```bash
# Re-run after changing a procedure (no volume reset needed):
docker exec -i afyabima-postgres \
  psql -U afyabima_admin -d afyabima < 03_stored_procedures.sql

# Confirm all routines were created:
docker exec afyabima-postgres \
  psql -U afyabima_admin -d afyabima -c "\df ops.*"
```

Verifying the grants are correct:

```sql
-- Check what kafka_connect can call:
SELECT routine_name, privilege_type
FROM information_schema.role_routine_grants
WHERE grantee = 'kafka_connect'
  AND routine_schema = 'ops';

-- Quick smoke tests for each section:
SELECT * FROM ops.pipeline_health();
SELECT * FROM ops.run_quality_gate();          -- 0 rows if data is clean
SELECT * FROM ops.post_load_summary();         -- run after load_csv.sh
SELECT * FROM ops.dlq_summary();               -- 0 rows if DLQ is empty
SELECT ops.auto_open_investigations('test');   -- returns count of new investigations
SELECT ops.close_stale_investigations();       -- returns count of closed investigations
```

---

## 9. What this connects to

Once this file runs, the full operational surface of the database is available. The downstream connections are:

`load_csv.sh` calls `ops.post_load_summary()` as its final validation step after loading CSVs. This is the first production use of these procedures after a fresh setup.

Airflow DAGs call `ops.run_quality_gate()` as the gate before triggering dbt, `ops.pipeline_health()` and `ops.dlq_summary()` as monitoring tasks, and `ops.auto_open_investigations()` + `ops.close_stale_investigations()` on the 6-hour fraud schedule.

FastAPI exposes `ops.pipeline_health()`, `ops.dlq_summary()`, and `ops.replication_lag()` through its `/health` endpoint, surfacing them to Grafana dashboards without requiring the monitoring tool to authenticate directly to the database.

Kafka Connect calls `ops.upsert_claim()` and `ops.append_claim_event()` on every message delivered from the claims topics, replacing the raw `INSERT` statements that a default JDBC sink would use.

---

## 10. Things worth carrying forward

The `PROCEDURE` vs `FUNCTION` distinction in PostgreSQL is not stylistic — it determines transaction semantics. Procedures can commit; functions cannot. Getting this wrong means a routine that looks correct in isolation silently fails to commit its writes when called from within a larger transaction. Every write in this file is a `PROCEDURE`; every read is a `FUNCTION`.

The `WHERE raw.claims.updated_at <= EXCLUDED.updated_at` pattern in `upsert_claim` is the correct way to handle out-of-order event delivery at the database level. It is worth understanding deeply because the same problem — out-of-order messages producing state rollback — appears in any system that consumes an event stream and materialises current state. The solution is always some form of recency guard, and the database upsert layer is the right place to enforce it.

The `RETURN QUERY ... HAVING COUNT(*) > 0` pattern in `run_quality_gate()` is a clean, composable way to build a multi-test quality function. Each test is independent, testable in isolation by running it directly, and contributes zero rows on pass rather than a `passed: true` row that the caller must filter. The convention of returning only violations, never confirmations, is what makes zero rows the definitive pass signal.

`GET DIAGNOSTICS v_count = ROW_COUNT` is the idiomatic way in PL/pgSQL to capture DML row counts without a second round-trip. The alternative — running a `SELECT COUNT(*)` after the `UPDATE` — would add latency and could return a different count if concurrent writes occur between the two statements.
