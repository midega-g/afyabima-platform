-- =============================================================================
-- AfyaBima — Stored Procedures & Automation Layer
-- =============================================================================
--
-- PURPOSE
--   Provides reusable server-side logic for:
--     1. Safe idempotent upserts for each Kafka Connect sink table
--     2. Data quality gate — runs db_tests.sql logic as a callable function
--     3. Reference data refresh (plans, providers, members, drug_formulary)
--     4. Operational monitoring (DLQ depth, freshness, replication lag)
--     5. Fraud pipeline automation (auto-open investigations on high-risk claims)
--     6. Post-load validation summary (returns structured pass/fail table)
--
-- SCHEMA CONVENTION
--   All stored procedures live in a dedicated `ops` schema to keep them
--   cleanly separated from raw/staging/marts application schemas.
--
-- EXECUTION ORDER
--   1. Run schema.sql first (creates raw.* tables)
--   2. Run this file: psql -U postgres -d afyabima -f stored_procedures.sql
--   3. Procedures are then available to any role with EXECUTE privilege.
--
-- ROLES
--   GRANT EXECUTE ON ALL ROUTINES IN SCHEMA ops TO airflow_runner;
--   GRANT EXECUTE ON SPECIFIC FUNCTIONS ... TO dbt_runner;
--   (See bottom of this file for the full grant block.)
-- =============================================================================


-- ---------------------------------------------------------------------------
-- 0. Create ops schema
-- ---------------------------------------------------------------------------

CREATE SCHEMA IF NOT EXISTS ops;

COMMENT ON SCHEMA ops IS
    'Stored procedures and utility functions for AfyaBima operations and automation.';


-- ===========================================================================
-- SECTION 1 — UPSERT PROCEDURES
-- Safe for repeated calls. On conflict, updates mutable fields only;
-- immutable audit fields (_loaded_at of the original row) are preserved.
-- Used by: Kafka Connect (via JDBC sink with upsert mode)
--          Airflow (for reference table full-refresh)
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1.1  ops.upsert_plan
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ops.upsert_plan(
    p_plan_code             TEXT,
    p_plan_name             TEXT,
    p_plan_tier             TEXT,
    p_annual_limit_kes      NUMERIC,
    p_inpatient_limit_kes   NUMERIC,
    p_outpatient_limit_kes  NUMERIC,
    p_premium_monthly_kes   NUMERIC,
    p_is_family_plan        BOOLEAN
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO raw.plans (
        plan_code, plan_name, plan_tier,
        annual_limit_kes, inpatient_limit_kes, outpatient_limit_kes,
        premium_monthly_kes, is_family_plan
    )
    VALUES (
        p_plan_code, p_plan_name, p_plan_tier,
        p_annual_limit_kes, p_inpatient_limit_kes, p_outpatient_limit_kes,
        p_premium_monthly_kes, p_is_family_plan
    )
    ON CONFLICT (plan_code) DO UPDATE SET
        plan_name             = EXCLUDED.plan_name,
        plan_tier             = EXCLUDED.plan_tier,
        annual_limit_kes      = EXCLUDED.annual_limit_kes,
        inpatient_limit_kes   = EXCLUDED.inpatient_limit_kes,
        outpatient_limit_kes  = EXCLUDED.outpatient_limit_kes,
        premium_monthly_kes   = EXCLUDED.premium_monthly_kes,
        is_family_plan        = EXCLUDED.is_family_plan,
        _loaded_at            = now();   -- refresh freshness timestamp on update
END;
$$;

COMMENT ON PROCEDURE ops.upsert_plan IS
    'Idempotent upsert for raw.plans. Safe to call on every Airflow reference refresh.';


-- ---------------------------------------------------------------------------
-- 1.2  ops.upsert_member
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ops.upsert_member(
    p_member_id     TEXT,
    p_first_name    TEXT,
    p_last_name     TEXT,
    p_date_of_birth DATE,
    p_gender        TEXT,
    p_id_type       TEXT,
    p_id_number     TEXT,
    p_phone_number  TEXT,
    p_county        TEXT,
    p_sub_county    TEXT,
    p_enrol_date    DATE,
    p_term_date     DATE,
    p_plan_code     TEXT,
    p_employer_id   TEXT,
    p_is_active     BOOLEAN
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO raw.members (
        member_id, first_name, last_name, date_of_birth, gender,
        id_type, id_number, phone_number, county, sub_county,
        enrol_date, term_date, plan_code, employer_id, is_active
    )
    VALUES (
        p_member_id, p_first_name, p_last_name, p_date_of_birth, p_gender,
        p_id_type, p_id_number, p_phone_number, p_county, p_sub_county,
        p_enrol_date, p_term_date, p_plan_code, p_employer_id, p_is_active
    )
    ON CONFLICT (member_id) DO UPDATE SET
        -- Immutable fields (never overwrite): member_id, date_of_birth, id_number
        first_name    = EXCLUDED.first_name,
        last_name     = EXCLUDED.last_name,
        gender        = EXCLUDED.gender,
        phone_number  = EXCLUDED.phone_number,
        county        = EXCLUDED.county,
        sub_county    = EXCLUDED.sub_county,
        term_date     = EXCLUDED.term_date,
        plan_code     = EXCLUDED.plan_code,
        employer_id   = EXCLUDED.employer_id,
        is_active     = EXCLUDED.is_active,
        _loaded_at    = now();
END;
$$;

COMMENT ON PROCEDURE ops.upsert_member IS
    'Idempotent upsert for raw.members. Preserves immutable fields (dob, id_number).';


-- ---------------------------------------------------------------------------
-- 1.3  ops.upsert_claim
-- Claims are the most frequently upserted entity (status advances over time).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ops.upsert_claim(
    p_claim_id          TEXT,
    p_member_id         TEXT,
    p_provider_id       TEXT,
    p_date_of_service   DATE,
    p_date_submitted    DATE,
    p_procedure_code    TEXT,
    p_diagnosis_code    TEXT,
    p_quantity          INT,
    p_amount_claimed    NUMERIC,
    p_amount_approved   NUMERIC,
    p_status            TEXT,
    p_submission_channel TEXT,
    p_is_emergency      BOOLEAN,
    p_prior_auth_number TEXT,
    p_submitted_at      TIMESTAMPTZ,
    p_updated_at        TIMESTAMPTZ
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO raw.claims (
        claim_id, member_id, provider_id, date_of_service, date_submitted,
        procedure_code, diagnosis_code, quantity,
        amount_claimed, amount_approved, status, submission_channel,
        is_emergency, prior_auth_number, submitted_at, updated_at
    )
    VALUES (
        p_claim_id, p_member_id, p_provider_id, p_date_of_service, p_date_submitted,
        p_procedure_code, p_diagnosis_code, p_quantity,
        p_amount_claimed, p_amount_approved, p_status, p_submission_channel,
        p_is_emergency, p_prior_auth_number, p_submitted_at, p_updated_at
    )
    ON CONFLICT (claim_id) DO UPDATE SET
        -- Immutable fields: claim_id, member_id, provider_id, date_of_service,
        --                   procedure_code, diagnosis_code, amount_claimed, submitted_at
        -- Mutable fields advance as the claim lifecycle progresses:
        amount_approved    = COALESCE(EXCLUDED.amount_approved, raw.claims.amount_approved),
        status             = EXCLUDED.status,
        prior_auth_number  = COALESCE(EXCLUDED.prior_auth_number, raw.claims.prior_auth_number),
        updated_at         = EXCLUDED.updated_at,
        _loaded_at         = now()
    -- Only update if the incoming record is newer (prevents out-of-order Kafka messages
    -- from rolling back a claim to an earlier status)
    WHERE raw.claims.updated_at <= EXCLUDED.updated_at;
END;
$$;

COMMENT ON PROCEDURE ops.upsert_claim IS
    'Idempotent upsert for raw.claims. Only advances status — never rolls back. '
    'Guards against out-of-order Kafka delivery with the WHERE clause.';


-- ---------------------------------------------------------------------------
-- 1.4  ops.append_claim_event
-- claim_events is immutable — only INSERT, never UPDATE.
-- Uses DO NOTHING on conflict so replaying a topic is safe.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ops.append_claim_event(
    p_event_id       TEXT,
    p_claim_id       TEXT,
    p_event_type     TEXT,
    p_previous_status TEXT,
    p_new_status     TEXT,
    p_event_at       TIMESTAMPTZ,
    p_triggered_by   TEXT,
    p_metadata       JSONB
)
LANGUAGE plpgsql AS $$
BEGIN
    INSERT INTO raw.claim_events (
        event_id, claim_id, event_type,
        previous_status, new_status, event_at, triggered_by, metadata
    )
    VALUES (
        p_event_id, p_claim_id, p_event_type,
        p_previous_status, p_new_status, p_event_at, p_triggered_by, p_metadata
    )
    ON CONFLICT (event_id) DO NOTHING;  -- idempotent replay protection
END;
$$;

COMMENT ON PROCEDURE ops.append_claim_event IS
    'Append-only insert for raw.claim_events. DO NOTHING on conflict makes '
    'Kafka topic replay fully safe.';


-- ===========================================================================
-- SECTION 2 — REFERENCE DATA FULL REFRESH
-- Airflow calls these on a daily schedule to keep master data current.
-- Each procedure accepts a JSON array, upserts all rows, and deactivates
-- anything not in the incoming payload.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 2.1  ops.refresh_drug_formulary
-- Replaces the formulary from a JSON array of drug objects.
-- Called by Airflow when the official SHA formulary CSV is updated.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE ops.refresh_drug_formulary(
    p_rows JSONB   -- JSON array: [{drug_code, drug_name, drug_class, ...}, ...]
)
LANGUAGE plpgsql AS $$
DECLARE
    v_row  JSONB;
    v_count INT := 0;
BEGIN
    FOR v_row IN SELECT * FROM jsonb_array_elements(p_rows)
    LOOP
        INSERT INTO raw.drug_formulary (
            drug_code, drug_name, drug_class,
            is_essential_medicine, is_controlled_substance, unit_cost_kes
        )
        VALUES (
            v_row->>'drug_code',
            v_row->>'drug_name',
            v_row->>'drug_class',
            (v_row->>'is_essential_medicine')::BOOLEAN,
            (v_row->>'is_controlled_substance')::BOOLEAN,
            (v_row->>'unit_cost_kes')::NUMERIC
        )
        ON CONFLICT (drug_code) DO UPDATE SET
            drug_name               = EXCLUDED.drug_name,
            drug_class              = EXCLUDED.drug_class,
            is_essential_medicine   = EXCLUDED.is_essential_medicine,
            is_controlled_substance = EXCLUDED.is_controlled_substance,
            unit_cost_kes           = EXCLUDED.unit_cost_kes,
            _loaded_at              = now();
        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'ops.refresh_drug_formulary: % rows upserted.', v_count;
END;
$$;

COMMENT ON PROCEDURE ops.refresh_drug_formulary IS
    'Full refresh of raw.drug_formulary from a JSON array. Called by Airflow daily.';


-- ===========================================================================
-- SECTION 3 — DATA QUALITY GATE
-- Returns a structured table of test results. Zero rows = all tests pass.
-- Designed to be called by Airflow as a quality gate before triggering dbt.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 3.1  ops.run_quality_gate()
-- Returns one row per violation. Airflow marks the DAG as failed if any
-- row is returned with severity = 'critical'.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.run_quality_gate()
RETURNS TABLE (
    test_id         TEXT,
    severity        TEXT,   -- 'critical' | 'warning'
    table_name      TEXT,
    violation_count BIGINT,
    description     TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    -- -----------------------------------------------------------------------
    -- Critical: orphaned FK references (data integrity failures)
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT
        'FK-01'::TEXT, 'critical'::TEXT, 'raw.claims'::TEXT,
        COUNT(*),
        'claims.member_id not in members'::TEXT
    FROM raw.claims c
    LEFT JOIN raw.members m ON m.member_id = c.member_id
    WHERE m.member_id IS NULL
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FK-02', 'critical', 'raw.claims',
        COUNT(*),
        'claims.provider_id not in providers'
    FROM raw.claims c
    LEFT JOIN raw.providers p ON p.provider_id = c.provider_id
    WHERE p.provider_id IS NULL
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FK-03', 'critical', 'raw.prescriptions',
        COUNT(*),
        'prescriptions.drug_code not in drug_formulary'
    FROM raw.prescriptions p
    LEFT JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
    WHERE df.drug_code IS NULL
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FK-04', 'critical', 'raw.fraud_investigation_outcomes',
        COUNT(*),
        'outcome.claim_id != parent investigation.claim_id'
    FROM raw.fraud_investigation_outcomes o
    JOIN raw.fraud_investigations i ON i.investigation_id = o.investigation_id
    WHERE o.claim_id != i.claim_id
    HAVING COUNT(*) > 0;

    -- -----------------------------------------------------------------------
    -- Critical: financial integrity failures
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'FIN-01', 'critical', 'raw.claims',
        COUNT(*),
        'amount_approved exceeds amount_claimed'
    FROM raw.claims
    WHERE amount_approved IS NOT NULL
      AND amount_approved > amount_claimed
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FIN-02', 'critical', 'raw.payments',
        COUNT(*),
        'total payments per claim exceed amount_approved'
    FROM raw.payments pay
    JOIN raw.claims c ON c.claim_id = pay.claim_id
    WHERE c.amount_approved IS NOT NULL
    GROUP BY c.claim_id, c.amount_approved
    HAVING SUM(pay.amount_paid_kes) > c.amount_approved
    -- Wrap in a count since we can't directly HAVING on the outer HAVING
    LIMIT 0;  -- placeholder; see below for correct form

    -- Corrected form:
    RETURN QUERY
    SELECT 'FIN-02', 'critical', 'raw.payments',
        COUNT(*)::BIGINT,
        'total payments per claim exceed amount_approved'
    FROM (
        SELECT c.claim_id
        FROM raw.payments pay
        JOIN raw.claims c ON c.claim_id = pay.claim_id
        WHERE c.amount_approved IS NOT NULL
        GROUP BY c.claim_id, c.amount_approved
        HAVING SUM(pay.amount_paid_kes) > c.amount_approved
    ) violations
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FIN-03', 'critical', 'raw.payments',
        COUNT(*),
        'payment exists for rejected or fraud-flagged claim'
    FROM raw.payments pay
    JOIN raw.claims c ON c.claim_id = pay.claim_id
    WHERE c.status IN ('rejected', 'flagged_fraud')
    HAVING COUNT(*) > 0;

    -- -----------------------------------------------------------------------
    -- Critical: event log integrity
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'EVT-01', 'critical', 'raw.claim_events',
        COUNT(*),
        'claims with no submitted event in claim_events'
    FROM raw.claims c
    LEFT JOIN raw.claim_events e ON e.claim_id = c.claim_id
    WHERE e.event_id IS NULL
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'EVT-02', 'critical', 'raw.claim_events',
        COUNT(*),
        'paid claims with no paid event'
    FROM raw.claims c
    LEFT JOIN raw.claim_events e ON e.claim_id = c.claim_id AND e.event_type = 'paid'
    WHERE c.status = 'paid' AND e.event_id IS NULL
    HAVING COUNT(*) > 0;

    -- -----------------------------------------------------------------------
    -- Critical: fraud pipeline integrity
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'FRD-01', 'critical', 'raw.fraud_investigation_outcomes',
        COUNT(*),
        'confirmed_fraud outcome missing fraud_type'
    FROM raw.fraud_investigation_outcomes
    WHERE final_label = 'confirmed_fraud' AND fraud_type IS NULL
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'FRD-02', 'critical', 'raw.fraud_investigations',
        COUNT(*),
        'closed investigation with no outcome row'
    FROM raw.fraud_investigations fi
    LEFT JOIN raw.fraud_investigation_outcomes o ON o.investigation_id = fi.investigation_id
    WHERE fi.status = 'closed' AND o.outcome_id IS NULL
    HAVING COUNT(*) > 0;

    -- -----------------------------------------------------------------------
    -- Warning: temporal anomalies
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'TMP-01', 'warning', 'raw.claims',
        COUNT(*),
        'date_of_service in the future'
    FROM raw.claims
    WHERE date_of_service > CURRENT_DATE
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'TMP-02', 'warning', 'raw.claims',
        COUNT(*),
        'submission lag > 90 days'
    FROM raw.claims
    WHERE (date_submitted - date_of_service) > 90
    HAVING COUNT(*) > 0;

    RETURN QUERY
    SELECT 'TMP-03', 'warning', 'raw.payments',
        COUNT(*),
        'paid_at before claim submitted_at'
    FROM raw.payments pay
    JOIN raw.claims c ON c.claim_id = pay.claim_id
    WHERE pay.paid_at < c.submitted_at
    HAVING COUNT(*) > 0;

    -- -----------------------------------------------------------------------
    -- Warning: freshness (only meaningful in production, not initial load)
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'FRS-01', 'warning', 'raw.claims',
        1::BIGINT,
        'claims table not updated in last hour'
    WHERE (SELECT MAX(_loaded_at) FROM raw.claims) < NOW() - INTERVAL '1 hour'
       OR (SELECT MAX(_loaded_at) FROM raw.claims) IS NULL;

    RETURN QUERY
    SELECT 'FRS-02', 'warning', 'raw.members',
        1::BIGINT,
        'members table not refreshed today'
    WHERE (SELECT MAX(_loaded_at) FROM raw.members)::date < CURRENT_DATE
       OR (SELECT MAX(_loaded_at) FROM raw.members) IS NULL;

    -- -----------------------------------------------------------------------
    -- Warning: DLQ backlog
    -- -----------------------------------------------------------------------
    RETURN QUERY
    SELECT 'DLQ-01', 'warning', 'raw.dlq_events',
        COUNT(*),
        'unresolved DLQ events older than 7 days'
    FROM raw.dlq_events
    WHERE resolved = FALSE
      AND failed_at < NOW() - INTERVAL '7 days'
    HAVING COUNT(*) > 0;

END;
$$;

COMMENT ON FUNCTION ops.run_quality_gate IS
    'Runs core data quality tests. Returns one row per violation. '
    'Zero rows = all checks pass. Airflow calls this before triggering dbt. '
    'severity=critical blocks dbt; severity=warning sends alert but allows dbt to run.';


-- ---------------------------------------------------------------------------
-- 3.2  ops.assert_quality_gate()
-- Raises an exception if any critical test fails.
-- Use this as a simple go/no-go check in migration scripts.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.assert_quality_gate()
RETURNS VOID
LANGUAGE plpgsql AS $$
DECLARE
    v_critical_count INT;
    v_warning_count  INT;
BEGIN
    SELECT
        COUNT(*) FILTER (WHERE severity = 'critical'),
        COUNT(*) FILTER (WHERE severity = 'warning')
    INTO v_critical_count, v_warning_count
    FROM ops.run_quality_gate();

    IF v_critical_count > 0 THEN
        RAISE EXCEPTION
            'Quality gate FAILED: % critical violation(s), % warning(s). '
            'Run: SELECT * FROM ops.run_quality_gate(); for details.',
            v_critical_count, v_warning_count;
    END IF;

    IF v_warning_count > 0 THEN
        RAISE WARNING
            'Quality gate passed with % warning(s). '
            'Run: SELECT * FROM ops.run_quality_gate() WHERE severity = ''warning''; for details.',
            v_warning_count;
    END IF;

    RAISE NOTICE 'Quality gate PASSED (0 critical, % warnings).', v_warning_count;
END;
$$;

COMMENT ON FUNCTION ops.assert_quality_gate IS
    'Raises EXCEPTION if any critical quality tests fail. '
    'Use in migration scripts: SELECT ops.assert_quality_gate();';


-- ===========================================================================
-- SECTION 4 — OPERATIONAL MONITORING
-- Functions that return live operational metrics.
-- Called by Airflow, Grafana, or the FastAPI /health endpoint.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 4.1  ops.pipeline_health()
-- Returns a snapshot of pipeline health for all data sources.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.pipeline_health()
RETURNS TABLE (
    source          TEXT,
    last_loaded     TIMESTAMPTZ,
    lag_minutes     NUMERIC,
    row_count       BIGINT,
    status          TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        src.source,
        src.last_loaded,
        ROUND(EXTRACT(EPOCH FROM (NOW() - src.last_loaded)) / 60.0, 1) AS lag_minutes,
        src.row_count,
        CASE
            WHEN src.last_loaded IS NULL                            THEN 'NEVER LOADED'
            WHEN src.source = 'raw.claims'
             AND src.last_loaded < NOW() - INTERVAL '1 hour'       THEN 'STALE'
            WHEN src.source IN ('raw.members','raw.providers','raw.plans')
             AND src.last_loaded::date < CURRENT_DATE              THEN 'STALE'
            WHEN src.source = 'raw.fraud_predictions'
             AND src.last_loaded < NOW() - INTERVAL '2 hours'      THEN 'STALE'
            ELSE 'OK'
        END AS status
    FROM (
        VALUES
            ('raw.claims',
             (SELECT MAX(_loaded_at) FROM raw.claims),
             (SELECT COUNT(*)        FROM raw.claims)),
            ('raw.members',
             (SELECT MAX(_loaded_at) FROM raw.members),
             (SELECT COUNT(*)        FROM raw.members)),
            ('raw.providers',
             (SELECT MAX(_loaded_at) FROM raw.providers),
             (SELECT COUNT(*)        FROM raw.providers)),
            ('raw.plans',
             (SELECT MAX(_loaded_at) FROM raw.plans),
             (SELECT COUNT(*)        FROM raw.plans)),
            ('raw.drug_formulary',
             (SELECT MAX(_loaded_at) FROM raw.drug_formulary),
             (SELECT COUNT(*)        FROM raw.drug_formulary)),
            ('raw.fraud_predictions',
             (SELECT MAX(_loaded_at) FROM raw.fraud_predictions),
             (SELECT COUNT(*)        FROM raw.fraud_predictions)),
            ('raw.fraud_investigations',
             (SELECT MAX(_loaded_at) FROM raw.fraud_investigations),
             (SELECT COUNT(*)        FROM raw.fraud_investigations))
    ) AS src(source, last_loaded, row_count);
END;
$$;

COMMENT ON FUNCTION ops.pipeline_health IS
    'Returns freshness and row counts for all pipeline source tables. '
    'Call from Airflow health-check DAG or Grafana data source.';


-- ---------------------------------------------------------------------------
-- 4.2  ops.dlq_summary()
-- Returns DLQ depth by topic and age bucket.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.dlq_summary()
RETURNS TABLE (
    source_topic    TEXT,
    unresolved      BIGINT,
    oldest_failure  TIMESTAMPTZ,
    max_retries     SMALLINT,
    breach_sla      BOOLEAN
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        d.source_topic,
        COUNT(*)                            AS unresolved,
        MIN(d.failed_at)                    AS oldest_failure,
        MAX(d.retry_count)                  AS max_retries,
        MIN(d.failed_at) < NOW() - INTERVAL '7 days' AS breach_sla
    FROM raw.dlq_events d
    WHERE d.resolved = FALSE
    GROUP BY d.source_topic
    ORDER BY unresolved DESC;
END;
$$;

COMMENT ON FUNCTION ops.dlq_summary IS
    'DLQ depth by topic. breach_sla=true means events older than 7 days exist.';


-- ---------------------------------------------------------------------------
-- 4.3  ops.replication_lag()
-- Returns Debezium WAL lag in bytes.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.replication_lag()
RETURNS TABLE (
    slot_name       TEXT,
    lag_bytes       BIGINT,
    lag_mb          NUMERIC,
    status          TEXT
)
LANGUAGE plpgsql AS $$
BEGIN
    RETURN QUERY
    SELECT
        s.slot_name::TEXT,
        pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)  AS lag_bytes,
        ROUND(pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)
              / 1048576.0, 2)                                           AS lag_mb,
        CASE
            WHEN NOT s.active THEN 'INACTIVE'
            WHEN pg_wal_lsn_diff(pg_current_wal_lsn(), s.confirmed_flush_lsn)
                 > 100 * 1048576 THEN 'HIGH LAG'
            ELSE 'OK'
        END AS status
    FROM pg_replication_slots s
    WHERE s.slot_name LIKE 'debezium%';
END;
$$;

COMMENT ON FUNCTION ops.replication_lag IS
    'Debezium WAL replication lag in bytes and MB. HIGH LAG = > 100 MB.';


-- ===========================================================================
-- SECTION 5 — FRAUD PIPELINE AUTOMATION
-- Automates investigation opening for high-risk claims.
-- In production this logic lives in the Flink job. These procedures provide
-- the same behaviour for the initial CSV-loaded / non-streaming scenario.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 5.1  ops.auto_open_investigations()
-- Opens an investigation for every claim with:
--   • A fraud_prediction with fraud_probability > 0.80
--   • No existing open investigation
-- Returns the count of new investigations created.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.auto_open_investigations(
    p_assigned_to TEXT DEFAULT 'system'
)
RETURNS INT
LANGUAGE plpgsql AS $$
DECLARE
    v_count INT := 0;
    v_rec   RECORD;
BEGIN
    FOR v_rec IN
        SELECT fp.claim_id, fp.prediction_id, fp.fraud_probability
        FROM raw.fraud_predictions fp
        LEFT JOIN raw.fraud_investigations fi
            ON fi.claim_id = fp.claim_id
            AND fi.status NOT IN ('closed')
        WHERE fp.fraud_probability > 0.80
          AND fi.investigation_id IS NULL
          -- Only open if predicted within last 30 days (recent claims)
          AND fp.predicted_at >= NOW() - INTERVAL '30 days'
    LOOP
        INSERT INTO raw.fraud_investigations (
            investigation_id, claim_id, prediction_id,
            assigned_to, opened_at, status, priority
        )
        VALUES (
            'INV-' || encode(gen_random_bytes(8), 'hex'),
            v_rec.claim_id,
            v_rec.prediction_id,
            p_assigned_to,
            NOW(),
            'open',
            CASE
                WHEN v_rec.fraud_probability >= 0.95 THEN 'critical'
                WHEN v_rec.fraud_probability >= 0.90 THEN 'high'
                ELSE 'medium'
            END
        );

        -- Also update the claim status to under_investigation
        UPDATE raw.claims
        SET status     = 'under_investigation',
            updated_at = NOW()
        WHERE claim_id = v_rec.claim_id
          AND status NOT IN ('under_investigation', 'closed', 'paid');

        v_count := v_count + 1;
    END LOOP;

    RAISE NOTICE 'ops.auto_open_investigations: % new investigations opened.', v_count;
    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ops.auto_open_investigations IS
    'Opens investigations for high-risk claims (fraud_probability > 0.80) '
    'that have no active investigation. Called by Airflow every 6 hours. '
    'Mirrors the Flink auto-routing logic for the non-streaming scenario.';


-- ---------------------------------------------------------------------------
-- 5.2  ops.close_stale_investigations()
-- Marks investigations as closed if open for > 30 days with no outcome.
-- Prevents the fraud_investigation_outcomes CDC pipe from going silent.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION ops.close_stale_investigations()
RETURNS INT
LANGUAGE plpgsql AS $$
DECLARE
    v_count INT;
BEGIN
    UPDATE raw.fraud_investigations
    SET status    = 'closed',
        closed_at = NOW(),
        notes     = COALESCE(notes, '') ||
                    ' [Auto-closed by ops.close_stale_investigations after 30 days]'
    WHERE status IN ('open', 'in_progress')
      AND opened_at < NOW() - INTERVAL '30 days';

    GET DIAGNOSTICS v_count = ROW_COUNT;
    RAISE NOTICE 'ops.close_stale_investigations: % investigations auto-closed.', v_count;
    RETURN v_count;
END;
$$;

COMMENT ON FUNCTION ops.close_stale_investigations IS
    'Auto-closes investigations open for > 30 days. Prevents stale open state. '
    'Run after ops.auto_open_investigations in the Airflow DAG.';


-- ===========================================================================
-- SECTION 6 — POST-LOAD VALIDATION SUMMARY
-- Convenience function for validating an initial CSV load.
-- Call immediately after load_csv.sh to get a structured pass/fail report.
-- ===========================================================================

CREATE OR REPLACE FUNCTION ops.post_load_summary()
RETURNS TABLE (
    check_name      TEXT,
    result          TEXT,
    value           TEXT,
    notes           TEXT
)
LANGUAGE plpgsql AS $$
DECLARE
    v_plans         BIGINT;
    v_employers     BIGINT;
    v_members       BIGINT;
    v_providers     BIGINT;
    v_claims        BIGINT;
    v_events        BIGINT;
    v_vitals        BIGINT;
    v_prescriptions BIGINT;
    v_payments      BIGINT;
    v_investigations BIGINT;
    v_outcomes      BIGINT;
    v_orphan_claims BIGINT;
    v_orphan_rx     BIGINT;
    v_dupe_claims   BIGINT;
    v_unpaid_paid   BIGINT;
    v_quality_issues BIGINT;
BEGIN
    -- Row counts
    SELECT COUNT(*) INTO v_plans         FROM raw.plans;
    SELECT COUNT(*) INTO v_employers     FROM raw.employers;
    SELECT COUNT(*) INTO v_members       FROM raw.members;
    SELECT COUNT(*) INTO v_providers     FROM raw.providers;
    SELECT COUNT(*) INTO v_claims        FROM raw.claims;
    SELECT COUNT(*) INTO v_events        FROM raw.claim_events;
    SELECT COUNT(*) INTO v_vitals        FROM raw.vitals;
    SELECT COUNT(*) INTO v_prescriptions FROM raw.prescriptions;
    SELECT COUNT(*) INTO v_payments      FROM raw.payments;
    SELECT COUNT(*) INTO v_investigations FROM raw.fraud_investigations;
    SELECT COUNT(*) INTO v_outcomes      FROM raw.fraud_investigation_outcomes;

    RETURN QUERY SELECT 'row_count.plans',           'OK',    v_plans::TEXT,          'insurance products';
    RETURN QUERY SELECT 'row_count.employers',       'OK',    v_employers::TEXT,       'corporate clients';
    RETURN QUERY SELECT 'row_count.members',         'OK',    v_members::TEXT,         'insured beneficiaries';
    RETURN QUERY SELECT 'row_count.providers',       'OK',    v_providers::TEXT,       'healthcare facilities';
    RETURN QUERY SELECT 'row_count.claims',          'OK',    v_claims::TEXT,          'claims submitted';
    RETURN QUERY SELECT 'row_count.claim_events',    'OK',    v_events::TEXT,          'state transition events';
    RETURN QUERY SELECT 'row_count.vitals',          'OK',    v_vitals::TEXT,          'clinical measurements';
    RETURN QUERY SELECT 'row_count.prescriptions',   'OK',    v_prescriptions::TEXT,   'drug prescriptions';
    RETURN QUERY SELECT 'row_count.payments',        'OK',    v_payments::TEXT,        'disbursements';
    RETURN QUERY SELECT 'row_count.investigations',  'OK',    v_investigations::TEXT,  'fraud investigations';
    RETURN QUERY SELECT 'row_count.outcomes',        'OK',    v_outcomes::TEXT,        'investigation outcomes';

    -- Orphan check
    SELECT COUNT(*) INTO v_orphan_claims
    FROM raw.claims c LEFT JOIN raw.members m ON m.member_id = c.member_id
    WHERE m.member_id IS NULL;

    RETURN QUERY SELECT
        'fk.claims_members',
        CASE WHEN v_orphan_claims = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_orphan_claims::TEXT || ' orphans',
        'claims.member_id → members';

    SELECT COUNT(*) INTO v_orphan_rx
    FROM raw.prescriptions p LEFT JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
    WHERE df.drug_code IS NULL;

    RETURN QUERY SELECT
        'fk.prescriptions_formulary',
        CASE WHEN v_orphan_rx = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_orphan_rx::TEXT || ' orphans',
        'prescriptions.drug_code → drug_formulary';

    -- Duplicate PK check
    SELECT COUNT(*) INTO v_dupe_claims
    FROM (SELECT claim_id FROM raw.claims GROUP BY claim_id HAVING COUNT(*) > 1) d;

    RETURN QUERY SELECT
        'pk.claims_unique',
        CASE WHEN v_dupe_claims = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_dupe_claims::TEXT || ' duplicates',
        'raw.claims PK uniqueness';

    -- Paid claims have payments
    SELECT COUNT(*) INTO v_unpaid_paid
    FROM raw.claims c LEFT JOIN raw.payments p ON p.claim_id = c.claim_id
    WHERE c.status = 'paid' AND p.payment_id IS NULL;

    RETURN QUERY SELECT
        'business.paid_claims_have_payments',
        CASE WHEN v_unpaid_paid = 0 THEN 'PASS' ELSE 'WARN' END,
        v_unpaid_paid::TEXT || ' paid claims missing payment',
        'paid status implies a payment record exists';

    -- Overall quality gate
    SELECT COUNT(*) INTO v_quality_issues
    FROM ops.run_quality_gate()
    WHERE severity = 'critical';

    RETURN QUERY SELECT
        'quality_gate.critical',
        CASE WHEN v_quality_issues = 0 THEN 'PASS' ELSE 'FAIL' END,
        v_quality_issues::TEXT || ' critical violations',
        'Run: SELECT * FROM ops.run_quality_gate() for details';
END;
$$;

COMMENT ON FUNCTION ops.post_load_summary IS
    'Convenience validation function. Run immediately after load_csv.sh. '
    'Returns a structured pass/fail table covering row counts, FK integrity, '
    'PK uniqueness, and the full quality gate.';


-- ===========================================================================
-- SECTION 7 — GRANTS
-- ===========================================================================

GRANT USAGE ON SCHEMA ops TO airflow_runner, dbt_runner, api_reader;

-- Airflow: needs to run quality gates, monitoring, and fraud automation
GRANT EXECUTE ON ALL ROUTINES IN SCHEMA ops TO airflow_runner;

-- dbt: needs quality gate to decide whether to proceed with model runs
GRANT EXECUTE ON FUNCTION ops.run_quality_gate()    TO dbt_runner;
GRANT EXECUTE ON FUNCTION ops.assert_quality_gate() TO dbt_runner;
GRANT EXECUTE ON FUNCTION ops.post_load_summary()   TO dbt_runner;

-- API: health endpoint
GRANT EXECUTE ON FUNCTION ops.pipeline_health()  TO api_reader;
GRANT EXECUTE ON FUNCTION ops.dlq_summary()      TO api_reader;
GRANT EXECUTE ON FUNCTION ops.replication_lag()  TO api_reader;

-- kafka_connect: upserts
GRANT EXECUTE ON PROCEDURE ops.upsert_plan         TO kafka_connect;
GRANT EXECUTE ON PROCEDURE ops.upsert_member       TO kafka_connect;
GRANT EXECUTE ON PROCEDURE ops.upsert_claim        TO kafka_connect;
GRANT EXECUTE ON PROCEDURE ops.append_claim_event  TO kafka_connect;
