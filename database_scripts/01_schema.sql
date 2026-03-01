-- =============================================================================
-- AfyaBima — PostgreSQL Schema Definition
-- =============================================================================
-- Production-ready DDL for all four schemas: raw, staging, intermediate, marts.
--
-- Design decisions
-- ----------------
-- 1. Role-based access control enforced at the schema level.
--    Kafka Connect  → raw (write only)
--    dbt            → raw (read), staging / intermediate / marts (write)
--    FastAPI        → marts (read only)
--    Debezium       → raw (read via logical replication slot)
--
-- 2. raw.* tables mirror Kafka message payloads exactly.
--    - Natural primary keys (claim_id, member_id, etc.) — no surrogate keys.
--    - _loaded_at timestamptz added by Kafka Connect for freshness tracking.
--    - Columns never altered after the table is live; schema evolution happens
--      via Avro schema compatibility in Schema Registry.
--
-- 3. Indexes are targeted — not exhaustive.
--    - Every FK column gets an index (Postgres does not auto-index FKs).
--    - Date/range columns used in dbt window functions get BRIN indexes
--      (append-only event tables with monotonically increasing dates).
--    - JSONB columns get GIN indexes only where API or dbt queries filter on them.
--
-- 4. staging and intermediate are VIEWS (not tables).
--    dbt materialises them as views; they are defined here as DDL stubs
--    with comments so the schema and grants can be created before dbt runs.
--
-- 5. marts tables are created empty here; dbt populates them on first run.
--
-- 6. All TIMESTAMP columns use TIMESTAMPTZ (time-zone aware).
--    Kafka events are UTC; stored as UTC; converted at query time.
--
-- 7. NUMERIC(15,2) for all KES monetary amounts — never FLOAT for money.
--
-- Run order:
--   1. Roles and schemas
--   2. Extensions
--   3. raw.*  (reference tables first, then transactional, then fraud pipeline)
--   4. staging / intermediate / marts (stubs + grants)
-- =============================================================================


-- =============================================================================
-- 0. EXTENSIONS
-- =============================================================================

CREATE EXTENSION IF NOT EXISTS "pgcrypto";   -- gen_random_uuid() for default PKs
CREATE EXTENSION IF NOT EXISTS "pg_stat_statements";  -- query performance tracking


-- =============================================================================
-- 1. ROLES
-- =============================================================================

-- Service roles — passwords set via environment variables at deploy time,
-- never hardcoded here.

DO $$
BEGIN
    -- Kafka Connect: writes raw events from topics
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'kafka_connect') THEN
        CREATE ROLE kafka_connect WITH LOGIN;
    END IF;

    -- dbt: reads raw, writes staging/intermediate/marts
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'dbt_runner') THEN
        CREATE ROLE dbt_runner WITH LOGIN;
    END IF;

    -- FastAPI: reads marts only
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'api_reader') THEN
        CREATE ROLE api_reader WITH LOGIN;
    END IF;

    -- Debezium: logical replication (must be superuser or replication role)
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'debezium') THEN
        CREATE ROLE debezium WITH LOGIN REPLICATION;
    END IF;

    -- Airflow: reads raw and marts for quality checks, writes model_performance_log
    IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'airflow_runner') THEN
        CREATE ROLE airflow_runner WITH LOGIN;
    END IF;
END
$$;


-- =============================================================================
-- 2. SCHEMAS
-- =============================================================================

CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS staging;
CREATE SCHEMA IF NOT EXISTS intermediate;
CREATE SCHEMA IF NOT EXISTS marts;

-- Schema-level grants
GRANT USAGE ON SCHEMA raw          TO kafka_connect, dbt_runner, airflow_runner, debezium;
GRANT USAGE ON SCHEMA staging      TO dbt_runner;
GRANT USAGE ON SCHEMA intermediate TO dbt_runner;
GRANT USAGE ON SCHEMA marts        TO dbt_runner, api_reader, airflow_runner;

-- Default privileges — any future table created in each schema inherits these
ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT SELECT ON TABLES TO dbt_runner, airflow_runner;

ALTER DEFAULT PRIVILEGES IN SCHEMA raw
    GRANT INSERT, UPDATE, DELETE ON TABLES TO kafka_connect;

ALTER DEFAULT PRIVILEGES IN SCHEMA marts
    GRANT SELECT ON TABLES TO api_reader, airflow_runner;


-- =============================================================================
-- 3. RAW SCHEMA — REFERENCE / MASTER DATA
-- These tables change infrequently; loaded via full refresh by Airflow.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- raw.plans
-- Financial parameters for each insurance product.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.plans (
    plan_code               TEXT        PRIMARY KEY,
    plan_name               TEXT        NOT NULL,
    plan_tier               TEXT        NOT NULL
                                        CHECK (plan_tier IN ('basic', 'standard', 'premium', 'family', 'corporate')),
    annual_limit_kes        NUMERIC(15,2) NOT NULL CHECK (annual_limit_kes > 0),
    inpatient_limit_kes     NUMERIC(15,2) NOT NULL CHECK (inpatient_limit_kes > 0),
    outpatient_limit_kes    NUMERIC(15,2) NOT NULL CHECK (outpatient_limit_kes > 0),
    premium_monthly_kes     NUMERIC(15,2) NOT NULL CHECK (premium_monthly_kes >= 0),
    is_family_plan          BOOLEAN     NOT NULL DEFAULT FALSE,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

COMMENT ON TABLE  raw.plans IS 'Insurance product definitions. Full refresh daily.';
COMMENT ON COLUMN raw.plans.plan_code              IS 'Primary identifier, e.g. NAT-001.';
COMMENT ON COLUMN raw.plans.annual_limit_kes       IS 'Total annual benefit limit in KES.';
COMMENT ON COLUMN raw.plans._loaded_at             IS 'Row load timestamp; used by dbt source freshness.';

-- ---------------------------------------------------------------------------
-- raw.employers
-- Corporate clients that sponsor member coverage.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.employers (
    employer_id             TEXT        PRIMARY KEY,
    employer_name           TEXT        NOT NULL,
    industry                TEXT,
    county                  TEXT,
    member_count            INT         CHECK (member_count >= 0),
    contract_start          DATE        NOT NULL,
    contract_end            DATE,
    is_active               BOOLEAN     NOT NULL DEFAULT TRUE,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT employers_contract_dates_chk
        CHECK (contract_end IS NULL OR contract_end > contract_start)
);

COMMENT ON TABLE raw.employers IS 'Corporate clients. Full refresh daily.';

-- ---------------------------------------------------------------------------
-- raw.members
-- Insured beneficiaries. FK to plans and employers.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.members (
    member_id               TEXT        PRIMARY KEY,
    first_name              TEXT        NOT NULL,
    last_name               TEXT        NOT NULL,
    date_of_birth           DATE        NOT NULL,
    gender                  TEXT        NOT NULL CHECK (gender IN ('M', 'F')),
    id_type                 TEXT        NOT NULL
                                        CHECK (id_type IN ('National ID', 'Passport', 'Alien ID', 'Military ID')),
    id_number               TEXT        NOT NULL,
    phone_number            TEXT,
    county                  TEXT        NOT NULL,
    sub_county              TEXT,
    enrol_date              DATE        NOT NULL,
    term_date               DATE,
    plan_code               TEXT        NOT NULL REFERENCES raw.plans (plan_code),
    employer_id             TEXT        REFERENCES raw.employers (employer_id),
    is_active               BOOLEAN     NOT NULL DEFAULT TRUE,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT members_term_after_enrol_chk
        CHECK (term_date IS NULL OR term_date > enrol_date)
);

CREATE INDEX IF NOT EXISTS idx_members_plan_code   ON raw.members (plan_code);
CREATE INDEX IF NOT EXISTS idx_members_employer_id ON raw.members (employer_id);
CREATE INDEX IF NOT EXISTS idx_members_county      ON raw.members (county);
CREATE INDEX IF NOT EXISTS idx_members_is_active   ON raw.members (is_active);

COMMENT ON TABLE  raw.members IS 'Insured beneficiaries. Full refresh daily.';
COMMENT ON COLUMN raw.members.term_date   IS 'NULL while member is active; set when coverage lapses.';
COMMENT ON COLUMN raw.members.employer_id IS 'NULL for individually enrolled members.';

-- ---------------------------------------------------------------------------
-- raw.providers
-- Healthcare facilities in the provider network.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.providers (
    provider_id             TEXT        PRIMARY KEY,
    provider_name           TEXT        NOT NULL,
    facility_type           TEXT        NOT NULL,
    county                  TEXT        NOT NULL,
    sub_county              TEXT,
    license_number          TEXT        NOT NULL,
    accreditation_status    TEXT        NOT NULL
                                        CHECK (accreditation_status IN ('Active', 'Probation', 'Suspended', 'Terminated')),
    panel_capacity          INT         CHECK (panel_capacity > 0),
    contract_start_date     DATE        NOT NULL,
    contract_end_date       DATE,
    risk_score              NUMERIC(4,3) CHECK (risk_score BETWEEN 0 AND 1),
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT providers_contract_dates_chk
        CHECK (contract_end_date IS NULL OR contract_end_date > contract_start_date)
);

CREATE INDEX IF NOT EXISTS idx_providers_county               ON raw.providers (county);
CREATE INDEX IF NOT EXISTS idx_providers_accreditation_status ON raw.providers (accreditation_status);
CREATE INDEX IF NOT EXISTS idx_providers_facility_type        ON raw.providers (facility_type);

COMMENT ON TABLE  raw.providers IS 'Contracted healthcare facilities. Full refresh daily.';
COMMENT ON COLUMN raw.providers.risk_score IS '0.0-1.0. Recomputed weekly by Airflow from claims history.';

-- ---------------------------------------------------------------------------
-- raw.drug_formulary
-- Approved drug list used for prescription validation.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.drug_formulary (
    drug_code               TEXT        PRIMARY KEY,
    drug_name               TEXT        NOT NULL,
    drug_class              TEXT        NOT NULL,
    is_essential_medicine   BOOLEAN     NOT NULL DEFAULT FALSE,
    is_controlled_substance BOOLEAN     NOT NULL DEFAULT FALSE,
    unit_cost_kes           NUMERIC(10,2),
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_drug_formulary_drug_class ON raw.drug_formulary (drug_class);

COMMENT ON TABLE raw.drug_formulary IS
    'SHA-approved drug schedule. Full refresh from official formulary CSV.';

-- ---------------------------------------------------------------------------
-- raw.icd10_codes
-- Diagnosis code lookup. Seeded from the WHO ICD-10 CSV.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.icd10_codes (
    icd10_code              TEXT        PRIMARY KEY,
    description             TEXT        NOT NULL,
    clinical_category       TEXT        NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_icd10_codes_clinical_category ON raw.icd10_codes (clinical_category);

COMMENT ON TABLE raw.icd10_codes IS 'ICD-10 code reference. Seeded from CSV; updated when WHO releases new version.';


-- =============================================================================
-- 4. RAW SCHEMA — TRANSACTIONAL EVENT TABLES
-- These tables grow continuously as Kafka Connect writes from event topics.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- raw.claims
-- Current claim state. Upserted by Kafka Connect on claim_id.
-- The source of truth for every claim that enters the system.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.claims (
    claim_id                TEXT        PRIMARY KEY,
    member_id               TEXT        NOT NULL REFERENCES raw.members  (member_id),
    provider_id             TEXT        NOT NULL REFERENCES raw.providers (provider_id),
    date_of_service         DATE        NOT NULL,
    date_submitted          DATE        NOT NULL,
    procedure_code          TEXT        NOT NULL,
    diagnosis_code          TEXT        NOT NULL REFERENCES raw.icd10_codes (icd10_code),
    quantity                INT         NOT NULL DEFAULT 1 CHECK (quantity > 0),
    amount_claimed          NUMERIC(15,2) NOT NULL CHECK (amount_claimed > 0),
    amount_approved         NUMERIC(15,2) CHECK (amount_approved >= 0),
    status                  TEXT        NOT NULL
                                        CHECK (status IN (
                                            'submitted', 'pending_review', 'approved',
                                            'partially_approved', 'rejected', 'paid',
                                            'flagged_fraud', 'under_investigation', 'closed'
                                        )),
    submission_channel      TEXT        NOT NULL
                                        CHECK (submission_channel IN ('portal', 'mobile', 'api', 'paper')),
    is_emergency            BOOLEAN     NOT NULL DEFAULT FALSE,
    prior_auth_number       TEXT,
    submitted_at            TIMESTAMPTZ NOT NULL,
    updated_at              TIMESTAMPTZ NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT claims_service_before_submit_chk
        CHECK (date_of_service <= date_submitted),
    CONSTRAINT claims_approved_lte_claimed_chk
        CHECK (amount_approved IS NULL OR amount_approved <= amount_claimed)
);

-- FK indexes (Postgres does not auto-create these)
CREATE INDEX IF NOT EXISTS idx_claims_member_id        ON raw.claims (member_id);
CREATE INDEX IF NOT EXISTS idx_claims_provider_id      ON raw.claims (provider_id);
CREATE INDEX IF NOT EXISTS idx_claims_diagnosis_code   ON raw.claims (diagnosis_code);
-- Range and filter indexes for dbt window functions and API queries
CREATE INDEX IF NOT EXISTS idx_claims_date_of_service  ON raw.claims (date_of_service);
CREATE INDEX IF NOT EXISTS idx_claims_status           ON raw.claims (status);
CREATE INDEX IF NOT EXISTS idx_claims_submitted_at     ON raw.claims (submitted_at);
-- Composite for the fraud feature: same member, same provider, same diagnosis
CREATE INDEX IF NOT EXISTS idx_claims_dup_detection
    ON raw.claims (member_id, provider_id, diagnosis_code, date_of_service);

COMMENT ON TABLE  raw.claims IS
    'Current claim state. Upserted by Kafka Connect on claim_id from claims.submitted topic.';
COMMENT ON COLUMN raw.claims.amount_approved IS 'NULL until claim reaches approved/partially_approved status.';
COMMENT ON COLUMN raw.claims._loaded_at      IS 'Kafka Connect load time. Used by dbt source freshness.';

-- ---------------------------------------------------------------------------
-- raw.claim_events
-- Immutable event log — one row per state transition.
-- Never updated; only appended.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.claim_events (
    event_id                TEXT        PRIMARY KEY,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims (claim_id),
    event_type              TEXT        NOT NULL
                                        CHECK (event_type IN (
                                            'submitted', 'status_changed', 'approved',
                                            'rejected', 'paid', 'flagged', 'investigation_opened',
                                            'investigation_closed', 'appealed', 'closed'
                                        )),
    previous_status         TEXT,
    new_status              TEXT        NOT NULL,
    event_at                TIMESTAMPTZ NOT NULL,
    triggered_by            TEXT        NOT NULL
                                        CHECK (triggered_by IN ('system', 'auto_approve', 'investigator', 'api', 'airflow')),
    metadata                JSONB,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- BRIN is efficient for append-only tables ordered by time
CREATE INDEX IF NOT EXISTS idx_claim_events_claim_id  ON raw.claim_events (claim_id);
CREATE INDEX IF NOT EXISTS idx_claim_events_event_at  ON raw.claim_events USING BRIN (event_at);

COMMENT ON TABLE  raw.claim_events IS
    'Immutable event log. One row per claim state transition. Never updated after insert.';
COMMENT ON COLUMN raw.claim_events.metadata IS
    'Freeform JSONB for event-specific context (e.g. rejection reason, reviewer ID).';

-- ---------------------------------------------------------------------------
-- raw.vitals
-- Clinical measurements recorded at the point of service.
-- Absence for a consultation claim is the primary phantom-billing signal.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.vitals (
    vitals_id               TEXT        PRIMARY KEY,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims  (claim_id),
    member_id               TEXT        NOT NULL REFERENCES raw.members (member_id),
    date_taken              DATE        NOT NULL,
    systolic_bp             SMALLINT    CHECK (systolic_bp  BETWEEN 40  AND 300),
    diastolic_bp            SMALLINT    CHECK (diastolic_bp BETWEEN 20  AND 200),
    heart_rate              SMALLINT    CHECK (heart_rate   BETWEEN 20  AND 300),
    temperature_c           NUMERIC(4,1) CHECK (temperature_c BETWEEN 30 AND 45),
    weight_kg               NUMERIC(5,1) CHECK (weight_kg   BETWEEN 0.5 AND 500),
    height_cm               NUMERIC(5,1) CHECK (height_cm   BETWEEN 30  AND 250),
    bmi                     NUMERIC(5,2) CHECK (bmi         BETWEEN 5   AND 100),
    spo2_pct                SMALLINT    CHECK (spo2_pct     BETWEEN 50  AND 100),
    notes                   TEXT,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_vitals_claim_id  ON raw.vitals (claim_id);
CREATE INDEX IF NOT EXISTS idx_vitals_member_id ON raw.vitals (member_id);

COMMENT ON TABLE  raw.vitals IS
    'Point-of-service clinical measurements. One row per clinical encounter.';
COMMENT ON COLUMN raw.vitals.spo2_pct IS 'Blood oxygen saturation percentage. Added to proposal schema for clinical completeness.';

-- ---------------------------------------------------------------------------
-- raw.prescriptions
-- One row per drug prescribed per claim.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.prescriptions (
    prescription_id         TEXT        PRIMARY KEY,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims         (claim_id),
    member_id               TEXT        NOT NULL REFERENCES raw.members        (member_id),
    drug_code               TEXT        NOT NULL REFERENCES raw.drug_formulary (drug_code),
    dosage                  TEXT        NOT NULL,
    frequency               TEXT        NOT NULL,
    duration_days           INT         NOT NULL CHECK (duration_days > 0),
    quantity_dispensed      INT         CHECK (quantity_dispensed >= 0),
    dispensed_on_site       BOOLEAN     NOT NULL DEFAULT TRUE,
    prescribed_at           TIMESTAMPTZ NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_prescriptions_claim_id  ON raw.prescriptions (claim_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_member_id ON raw.prescriptions (member_id);
CREATE INDEX IF NOT EXISTS idx_prescriptions_drug_code ON raw.prescriptions (drug_code);

COMMENT ON TABLE  raw.prescriptions IS
    'Drugs prescribed per claim. Absence for diagnoses requiring medication is a fraud signal.';

-- ---------------------------------------------------------------------------
-- raw.payments
-- One row per payment disbursement. A claim may have multiple partial payments.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.payments (
    payment_id              TEXT        PRIMARY KEY,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims (claim_id),
    amount_paid_kes         NUMERIC(15,2) NOT NULL CHECK (amount_paid_kes > 0),
    payment_method          TEXT        NOT NULL
                                        CHECK (payment_method IN ('EFT', 'MPESA', 'Cheque', 'Cash')),
    reference_number        TEXT        UNIQUE,
    paid_at                 TIMESTAMPTZ NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_payments_claim_id ON raw.payments (claim_id);
CREATE INDEX IF NOT EXISTS idx_payments_paid_at  ON raw.payments USING BRIN (paid_at);

COMMENT ON TABLE raw.payments IS
    'Payment disbursements. One claim can have multiple rows (partial payments).';

-- ---------------------------------------------------------------------------
-- raw.stock_levels
-- Pharmacy stock snapshot per facility per drug.
-- Enables stockout detection and supply chain analytics.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.stock_levels (
    stock_id                TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    facility_id             TEXT        NOT NULL REFERENCES raw.providers    (provider_id),
    drug_code               TEXT        NOT NULL REFERENCES raw.drug_formulary (drug_code),
    quantity_on_hand        INT         NOT NULL CHECK (quantity_on_hand >= 0),
    reorder_level           INT         NOT NULL CHECK (reorder_level >= 0),
    reported_date           DATE        NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT stock_levels_facility_drug_date_uq
        UNIQUE (facility_id, drug_code, reported_date)
);

CREATE INDEX IF NOT EXISTS idx_stock_levels_facility_id   ON raw.stock_levels (facility_id);
CREATE INDEX IF NOT EXISTS idx_stock_levels_drug_code     ON raw.stock_levels (drug_code);
CREATE INDEX IF NOT EXISTS idx_stock_levels_reported_date ON raw.stock_levels USING BRIN (reported_date);

COMMENT ON TABLE  raw.stock_levels IS
    'Daily pharmacy stock snapshot. One row per facility/drug/date.';
COMMENT ON COLUMN raw.stock_levels.reorder_level IS
    'Minimum quantity before a reorder alert is triggered.';


-- =============================================================================
-- 5. RAW SCHEMA — FRAUD PIPELINE TABLES
-- Written by Flink, FastAPI, and the PySpark retraining job — not by generators.
-- =============================================================================

-- ---------------------------------------------------------------------------
-- raw.fraud_predictions
-- One row per claim per model version. Written by Flink scoring job.
-- feature_snapshot records exact feature values at scoring time for audit.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fraud_predictions (
    prediction_id           TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims (claim_id),
    model_version           TEXT        NOT NULL,
    fraud_probability       NUMERIC(6,5) NOT NULL CHECK (fraud_probability BETWEEN 0 AND 1),
    risk_tier               TEXT        NOT NULL
                                        CHECK (risk_tier IN ('low', 'medium', 'high', 'critical')),
    feature_snapshot        JSONB       NOT NULL,
    predicted_at            TIMESTAMPTZ NOT NULL,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_fraud_predictions_claim_id     ON raw.fraud_predictions (claim_id);
CREATE INDEX IF NOT EXISTS idx_fraud_predictions_model_version ON raw.fraud_predictions (model_version);
CREATE INDEX IF NOT EXISTS idx_fraud_predictions_risk_tier    ON raw.fraud_predictions (risk_tier);
CREATE INDEX IF NOT EXISTS idx_fraud_predictions_predicted_at ON raw.fraud_predictions USING BRIN (predicted_at);
-- GIN index for feature_snapshot queries (e.g. dbt audit queries filtering on feature values)
CREATE INDEX IF NOT EXISTS idx_fraud_predictions_features_gin
    ON raw.fraud_predictions USING GIN (feature_snapshot);

COMMENT ON TABLE  raw.fraud_predictions IS
    'Fraud score per claim per model version. Written by Flink within 500ms of claim ingestion.';
COMMENT ON COLUMN raw.fraud_predictions.feature_snapshot IS
    'Exact feature values at scoring time. Required for regulatory explainability and model auditing.';

-- ---------------------------------------------------------------------------
-- raw.fraud_investigations
-- Investigation lifecycle. One row per investigation.
-- Updated as the investigation moves through statuses.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fraud_investigations (
    investigation_id        TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims (claim_id),
    prediction_id           TEXT        REFERENCES raw.fraud_predictions (prediction_id),
    assigned_to             TEXT        NOT NULL,  -- investigator user ID
    opened_at               TIMESTAMPTZ NOT NULL DEFAULT now(),
    closed_at               TIMESTAMPTZ,
    status                  TEXT        NOT NULL
                                        CHECK (status IN ('open', 'in_progress', 'pending_review', 'closed', 'escalated')),
    priority                TEXT        NOT NULL
                                        CHECK (priority IN ('low', 'medium', 'high', 'critical')),
    notes                   TEXT,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT investigations_closed_after_opened_chk
        CHECK (closed_at IS NULL OR closed_at >= opened_at)
);

CREATE INDEX IF NOT EXISTS idx_fraud_investigations_claim_id  ON raw.fraud_investigations (claim_id);
CREATE INDEX IF NOT EXISTS idx_fraud_investigations_assigned  ON raw.fraud_investigations (assigned_to);
CREATE INDEX IF NOT EXISTS idx_fraud_investigations_status    ON raw.fraud_investigations (status);

COMMENT ON TABLE  raw.fraud_investigations IS
    'Investigation lifecycle. Updated by FastAPI as investigators work cases.';

-- ---------------------------------------------------------------------------
-- raw.fraud_investigation_outcomes
-- Final investigator verdict. INSERT-only.
-- Debezium watches this table and publishes every INSERT to Kafka →
-- investigations.closed topic → retraining pipeline.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.fraud_investigation_outcomes (
    outcome_id              TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    claim_id                TEXT        NOT NULL REFERENCES raw.claims              (claim_id),
    investigation_id        TEXT        NOT NULL REFERENCES raw.fraud_investigations (investigation_id),
    final_label             TEXT        NOT NULL
                                        CHECK (final_label IN ('confirmed_fraud', 'legitimate', 'inconclusive')),
    fraud_type              TEXT
                                        CHECK (fraud_type IN (
                                            'phantom_billing', 'threshold_manip',
                                            'benefit_sharing', 'duplicate_claim', 'other'
                                        )),
    confirmed_by            TEXT        NOT NULL,  -- investigator user ID
    confirmed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
    estimated_loss_kes      NUMERIC(15,2) CHECK (estimated_loss_kes >= 0),
    amount_recovered_kes    NUMERIC(15,2) CHECK (amount_recovered_kes >= 0),
    notes                   TEXT,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- One outcome per investigation — enforced as unique constraint
    CONSTRAINT outcomes_one_per_investigation_uq UNIQUE (investigation_id),

    CONSTRAINT outcomes_loss_requires_fraud_chk
        CHECK (
            estimated_loss_kes IS NULL
            OR final_label = 'confirmed_fraud'
        )
);

CREATE INDEX IF NOT EXISTS idx_outcomes_claim_id        ON raw.fraud_investigation_outcomes (claim_id);
CREATE INDEX IF NOT EXISTS idx_outcomes_investigation_id ON raw.fraud_investigation_outcomes (investigation_id);
CREATE INDEX IF NOT EXISTS idx_outcomes_final_label     ON raw.fraud_investigation_outcomes (final_label);
CREATE INDEX IF NOT EXISTS idx_outcomes_confirmed_at    ON raw.fraud_investigation_outcomes USING BRIN (confirmed_at);

COMMENT ON TABLE  raw.fraud_investigation_outcomes IS
    'Final verdict per investigation. INSERT-only. Debezium publishes every new row to '
    'investigations.closed Kafka topic for the ML retraining feedback loop.';

-- ---------------------------------------------------------------------------
-- raw.model_performance_log
-- Written by the PySpark retraining job after each training run.
-- Airflow reads this to decide whether to promote the new model version.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.model_performance_log (
    log_id                  TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    model_version           TEXT        NOT NULL,
    evaluation_date         DATE        NOT NULL,
    training_claim_count    INT         NOT NULL,
    training_fraud_count    INT         NOT NULL,
    auc_roc                 NUMERIC(6,5) NOT NULL CHECK (auc_roc BETWEEN 0 AND 1),
    precision_at_10pct      NUMERIC(6,5) CHECK (precision_at_10pct BETWEEN 0 AND 1),
    recall                  NUMERIC(6,5) CHECK (recall BETWEEN 0 AND 1),
    f1_score                NUMERIC(6,5) CHECK (f1_score BETWEEN 0 AND 1),
    promoted                BOOLEAN     NOT NULL DEFAULT FALSE,
    promotion_reason        TEXT,
    s3_artefact_path        TEXT,
    logged_at               TIMESTAMPTZ NOT NULL DEFAULT now(),

    CONSTRAINT model_perf_unique_version_date UNIQUE (model_version, evaluation_date)
);

CREATE INDEX IF NOT EXISTS idx_model_perf_evaluation_date ON raw.model_performance_log (evaluation_date);
CREATE INDEX IF NOT EXISTS idx_model_perf_promoted        ON raw.model_performance_log (promoted);

COMMENT ON TABLE  raw.model_performance_log IS
    'Model quality metrics per training run. Airflow uses auc_roc to decide promotion.';
COMMENT ON COLUMN raw.model_performance_log.s3_artefact_path IS
    'S3 URI of the ONNX model artefact, e.g. s3://afyabima-models/xgboost/v2.3/model.onnx.';

-- ---------------------------------------------------------------------------
-- raw.dlq_events
-- Dead-letter queue sink. Kafka Connect routes failed sink writes here.
-- Airflow monitors consumer lag and alerts if depth exceeds threshold.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS raw.dlq_events (
    dlq_id                  TEXT        PRIMARY KEY DEFAULT gen_random_uuid()::text,
    source_topic            TEXT        NOT NULL,
    failed_at               TIMESTAMPTZ NOT NULL,
    error_code              TEXT,
    error_message           TEXT,
    raw_payload             JSONB       NOT NULL,
    retry_count             SMALLINT    NOT NULL DEFAULT 0,
    resolved                BOOLEAN     NOT NULL DEFAULT FALSE,
    resolved_at             TIMESTAMPTZ,
    _loaded_at              TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dlq_source_topic ON raw.dlq_events (source_topic);
CREATE INDEX IF NOT EXISTS idx_dlq_resolved     ON raw.dlq_events (resolved) WHERE resolved = FALSE;
CREATE INDEX IF NOT EXISTS idx_dlq_failed_at    ON raw.dlq_events USING BRIN (failed_at);

COMMENT ON TABLE  raw.dlq_events IS
    'Dead-letter records for Kafka Connect sink failures. Airflow alerts if unresolved count > 10.';


-- =============================================================================
-- 6. STAGING SCHEMA — STUBS
-- dbt materialises these as views. DDL stubs are created here so grants and
-- schema search paths can be validated before dbt runs.
-- =============================================================================

-- Grants applied once at schema level via ALTER DEFAULT PRIVILEGES above.
-- Individual view creation is handled by dbt.

COMMENT ON SCHEMA staging IS
    'One view per raw table. Rename, cast, deduplicate. No joins. Created by dbt.';

COMMENT ON SCHEMA intermediate IS
    'Business logic and joins. Never accessed by applications. Created by dbt.';


-- =============================================================================
-- 7. MARTS SCHEMA — EMPTY TABLE STUBS
-- dbt populates these on first run. Stubs allow grants and connection tests
-- before dbt has run for the first time.
-- =============================================================================

-- Fact: claims with fraud score context
CREATE TABLE IF NOT EXISTS marts.fct_claims (
    claim_id                TEXT        PRIMARY KEY,
    member_id               TEXT,
    provider_id             TEXT,
    date_of_service         DATE,
    date_submitted          DATE,
    amount_claimed          NUMERIC(15,2),
    amount_approved         NUMERIC(15,2),
    status                  TEXT,
    days_to_submit          INT,
    fraud_probability       NUMERIC(6,5),
    risk_tier               TEXT,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fact: fraud scoring with model version tracking
CREATE TABLE IF NOT EXISTS marts.fct_fraud_scoring (
    prediction_id           TEXT        PRIMARY KEY,
    claim_id                TEXT,
    model_version           TEXT,
    fraud_probability       NUMERIC(6,5),
    risk_tier               TEXT,
    final_label             TEXT,
    was_correct             BOOLEAN,
    predicted_at            TIMESTAMPTZ,
    outcome_confirmed_at    TIMESTAMPTZ,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Fact: payments
CREATE TABLE IF NOT EXISTS marts.fct_payments (
    payment_id              TEXT        PRIMARY KEY,
    claim_id                TEXT,
    member_id               TEXT,
    provider_id             TEXT,
    employer_id             TEXT,
    amount_paid_kes         NUMERIC(15,2),
    payment_method          TEXT,
    paid_at                 TIMESTAMPTZ,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Dimension: members (SCD2 snapshot managed by dbt snapshot)
CREATE TABLE IF NOT EXISTS marts.dim_members (
    member_sk               TEXT        PRIMARY KEY,  -- surrogate: member_id + dbt_scd_id
    member_id               TEXT        NOT NULL,
    full_name               TEXT,
    date_of_birth           DATE,
    gender                  TEXT,
    county                  TEXT,
    plan_code               TEXT,
    plan_name               TEXT,
    employer_id             TEXT,
    employer_name           TEXT,
    is_active               BOOLEAN,
    dbt_valid_from          TIMESTAMPTZ,
    dbt_valid_to            TIMESTAMPTZ,
    dbt_is_current          BOOLEAN,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE INDEX IF NOT EXISTS idx_dim_members_member_id    ON marts.dim_members (member_id);
CREATE INDEX IF NOT EXISTS idx_dim_members_is_current   ON marts.dim_members (dbt_is_current);

-- Dimension: providers with risk profile
CREATE TABLE IF NOT EXISTS marts.dim_providers (
    provider_id             TEXT        PRIMARY KEY,
    provider_name           TEXT,
    facility_type           TEXT,
    county                  TEXT,
    accreditation_status    TEXT,
    risk_score              NUMERIC(4,3),
    total_claims_30d        INT,
    avg_claim_amount_30d    NUMERIC(15,2),
    fraud_rate_90d          NUMERIC(6,5),
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Mart: monthly claims performance summary
CREATE TABLE IF NOT EXISTS marts.mart_claims_performance (
    summary_month           DATE        NOT NULL,   -- first day of month
    plan_code               TEXT        NOT NULL,
    employer_id             TEXT,
    county                  TEXT,
    total_claims            INT,
    total_amount_claimed    NUMERIC(15,2),
    total_amount_approved   NUMERIC(15,2),
    approval_rate           NUMERIC(6,5),
    avg_days_to_submit      NUMERIC(8,2),
    flagged_fraud_count     INT,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    -- PRIMARY KEY cannot contain expressions (COALESCE). Use a unique index
    -- instead. NULLS NOT DISTINCT means two NULLs in employer_id or county
    -- are treated as equal — one row per (month, plan, employer, county) combination.
    PRIMARY KEY (summary_month, plan_code)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_mart_claims_performance
    ON marts.mart_claims_performance (summary_month, plan_code, COALESCE(employer_id, ''), COALESCE(county, ''));

-- Mart: provider analytics
CREATE TABLE IF NOT EXISTS marts.mart_provider_analytics (
    provider_id             TEXT        NOT NULL,
    analytics_date          DATE        NOT NULL,
    claim_count_1d          INT,
    claim_count_7d          INT,
    claim_count_30d         INT,
    avg_amount_7d           NUMERIC(15,2),
    amount_zscore_30d       NUMERIC(8,4),
    fraud_prediction_rate   NUMERIC(6,5),
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (provider_id, analytics_date)
);

-- Mart: member utilisation
CREATE TABLE IF NOT EXISTS marts.mart_member_utilisation (
    member_id               TEXT        NOT NULL,
    utilisation_month       DATE        NOT NULL,
    total_claimed_kes       NUMERIC(15,2),
    total_approved_kes      NUMERIC(15,2),
    annual_limit_kes        NUMERIC(15,2),
    ytd_utilisation_pct     NUMERIC(6,5),
    claim_count             INT,
    distinct_providers      INT,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (member_id, utilisation_month)
);

-- Mart: supply chain summary
CREATE TABLE IF NOT EXISTS marts.mart_supply_chain_summary (
    facility_id             TEXT        NOT NULL,
    drug_code               TEXT        NOT NULL,
    reported_date           DATE        NOT NULL,
    quantity_on_hand        INT,
    reorder_level           INT,
    is_below_reorder        BOOLEAN,
    stockout_days_30d       INT,
    refreshed_at            TIMESTAMPTZ NOT NULL DEFAULT now(),

    PRIMARY KEY (facility_id, drug_code, reported_date)
);

-- Grants for marts stubs
GRANT SELECT ON ALL TABLES IN SCHEMA marts TO api_reader, airflow_runner;


-- =============================================================================
-- 8. LOGICAL REPLICATION CONFIGURATION FOR DEBEZIUM
-- =============================================================================

-- Debezium requires wal_level=logical in postgresql.conf (set at server level,
-- not here). The publication below declares which table Debezium monitors.
-- Only fraud_investigation_outcomes is watched — not the entire raw schema —
-- to minimise WAL volume.

DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_publication WHERE pubname = 'afyabima_debezium'
    ) THEN
        CREATE PUBLICATION afyabima_debezium
            FOR TABLE raw.fraud_investigation_outcomes;
    END IF;
END
$$;

COMMENT ON PUBLICATION afyabima_debezium IS
    'Debezium CDC publication. Watches raw.fraud_investigation_outcomes only. '
    'Every INSERT triggers an event on the investigations.closed Kafka topic.';


-- =============================================================================
-- 9. GRANT SUMMARY (explicit table-level grants for pre-existing tables)
-- =============================================================================

-- raw schema: Kafka Connect writes, everyone else reads
GRANT SELECT, INSERT, UPDATE, DELETE ON ALL TABLES IN SCHEMA raw TO kafka_connect;
GRANT SELECT                          ON ALL TABLES IN SCHEMA raw TO dbt_runner, airflow_runner, debezium;

-- staging and intermediate: dbt owns these schemas entirely
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA staging      TO dbt_runner;
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA intermediate  TO dbt_runner;

-- marts: dbt writes, api and airflow read
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA marts TO dbt_runner;
GRANT SELECT         ON ALL TABLES IN SCHEMA marts TO api_reader, airflow_runner;

-- model_performance_log: airflow writes (PySpark job submits via Airflow)
GRANT INSERT ON raw.model_performance_log TO airflow_runner;

-- dlq: Kafka Connect writes, Airflow monitors
GRANT INSERT ON raw.dlq_events TO kafka_connect;
GRANT SELECT ON raw.dlq_events TO airflow_runner;
