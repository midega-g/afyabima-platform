-- =============================================================================
-- AfyaBima — Production Database Quality Test Suite
-- =============================================================================
--
-- PURPOSE
--   Comprehensive, self-contained SQL test suite that validates every table in
--   the AfyaBima raw schema from 18 distinct angles. Complements dbt tests
--   (which run on transformed marts) by testing the raw source layer directly.
--
-- DESIGN
--   Every test follows the same contract:
--     • Returns 0 rows  → PASS  (no violations found)
--     • Returns >0 rows → FAIL  (each row is a violating record)
--
--   Tests are grouped into numbered sections. Run the full file or cherry-pick
--   individual sections. Each test block is preceded by a comment explaining
--   WHY the rule matters, not just what it checks.
--
-- EXECUTION
--   psql -U <superuser> -d afyabima -f db_tests.sql
--   Or wrap in a monitoring script that alerts when any query returns rows.
--
-- SECTIONS
--   01  Schema Integrity          — tables, columns, indexes exist
--   02  Primary Key Integrity     — uniqueness and non-null PKs
--   03  Not-Null Completeness     — every mandatory column is populated
--   04  Foreign Key Validity      — all FK values resolve to parent rows
--   05  Enum / Check Constraints  — domain values match schema enums
--   06  Cross-Column Row Rules    — multi-column logic within a single row
--   07  Cross-Table Business Rules— relationships spanning multiple tables
--   08  Temporal Consistency      — date/timestamp ordering and plausibility
--   09  Clinical Plausibility     — vitals, BMI derivation, physiological ranges
--   10  Fraud Pipeline Logic      — risk tiers, outcome completeness, ML audit
--   11  Financial Integrity       — payments, approvals, limits
--   12  Event Log Integrity       — claim_events sequence and completeness
--   13  Data Freshness            — _loaded_at staleness alerts
--   14  Referential Coverage      — orphan detection beyond FK constraints
--   15  Statistical Anomalies     — volume outliers, rate spikes
--   16  Security and Governance   — role privileges, sensitive field exposure
--   17  Index Effectiveness       — critical indexes are present
--   18  CDC / Debezium Integrity  — replication publication health
--
-- =============================================================================


-- =============================================================================
-- HELPER: test result wrapper
-- Wrap any test in DO $$ to raise a NOTICE on pass/fail, or query directly.
-- The pattern below keeps tests runnable as plain SELECT statements so results
-- can be piped to any monitoring tool.
-- =============================================================================


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 01 — SCHEMA INTEGRITY
-- Verifies that every expected table, column, and index exists with the correct
-- data type. Protects against accidental migrations, typos, or schema drift
-- caused by Kafka Connect, dbt, or manual ALTER TABLE statements.
-- ─────────────────────────────────────────────────────────────────────────────

-- 01-T01: All expected raw schema tables must exist
-- Returns missing table names. Any row = a table was dropped or never created.
SELECT
    expected.table_name,
    'MISSING TABLE' AS failure_reason
FROM (
    VALUES
        ('plans'), ('employers'), ('members'), ('providers'),
        ('drug_formulary'), ('icd10_codes'), ('claims'), ('claim_events'),
        ('vitals'), ('prescriptions'), ('payments'), ('stock_levels'),
        ('fraud_predictions'), ('fraud_investigations'),
        ('fraud_investigation_outcomes'), ('model_performance_log'),
        ('dlq_events')
) AS expected(table_name)
LEFT JOIN information_schema.tables t
    ON  t.table_schema = 'raw'
    AND t.table_name   = expected.table_name
    AND t.table_type   = 'BASE TABLE'
WHERE t.table_name IS NULL;


-- 01-T02: Critical columns must exist with the correct data type
-- Schema drift (e.g. TEXT column silently changed to VARCHAR) breaks dbt casts.
SELECT
    expected.table_name,
    expected.column_name,
    expected.expected_type,
    c.data_type         AS actual_type,
    'COLUMN TYPE MISMATCH OR MISSING' AS failure_reason
FROM (
    VALUES
        -- raw.claims — money columns must be numeric, never float
        ('claims', 'amount_claimed',  'numeric'),
        ('claims', 'amount_approved', 'numeric'),
        ('claims', 'submitted_at',    'timestamp with time zone'),
        ('claims', 'updated_at',      'timestamp with time zone'),
        ('claims', 'date_of_service', 'date'),
        -- raw.plans — financial limits must be numeric
        ('plans',  'annual_limit_kes',    'numeric'),
        ('plans',  'inpatient_limit_kes', 'numeric'),
        ('plans',  'premium_monthly_kes', 'numeric'),
        -- raw.vitals — clinical measurements
        ('vitals', 'systolic_bp',    'smallint'),
        ('vitals', 'temperature_c',  'numeric'),
        ('vitals', 'bmi',            'numeric'),
        -- raw.fraud_predictions — probability must be numeric
        ('fraud_predictions', 'fraud_probability', 'numeric'),
        ('fraud_predictions', 'feature_snapshot',  'jsonb'),
        -- raw.payments — money column
        ('payments', 'amount_paid_kes', 'numeric'),
        -- raw.model_performance_log — ML metrics
        ('model_performance_log', 'auc_roc',  'numeric'),
        ('model_performance_log', 'f1_score', 'numeric')
) AS expected(table_name, column_name, expected_type)
LEFT JOIN information_schema.columns c
    ON  c.table_schema = 'raw'
    AND c.table_name   = expected.table_name
    AND c.column_name  = expected.column_name
WHERE c.data_type IS NULL
   OR c.data_type != expected.expected_type;


-- 01-T03: Critical indexes must exist
-- Missing indexes on FK columns cause sequential scans that degrade at scale.
SELECT
    expected.index_name,
    'MISSING INDEX' AS failure_reason
FROM (
    VALUES
        ('idx_claims_member_id'),
        ('idx_claims_provider_id'),
        ('idx_claims_diagnosis_code'),
        ('idx_claims_date_of_service'),
        ('idx_claims_status'),
        ('idx_claims_submitted_at'),
        ('idx_claims_dup_detection'),
        ('idx_members_plan_code'),
        ('idx_members_employer_id'),
        ('idx_members_is_active'),
        ('idx_vitals_claim_id'),
        ('idx_vitals_member_id'),
        ('idx_prescriptions_claim_id'),
        ('idx_prescriptions_drug_code'),
        ('idx_payments_claim_id'),
        ('idx_fraud_predictions_claim_id'),
        ('idx_fraud_investigations_claim_id'),
        ('idx_outcomes_claim_id'),
        ('idx_outcomes_investigation_id')
) AS expected(index_name)
LEFT JOIN pg_indexes idx
    ON idx.schemaname = 'raw'
    AND idx.indexname = expected.index_name
WHERE idx.indexname IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 02 — PRIMARY KEY INTEGRITY
-- Every PK must be unique and non-null. The UNIQUE constraint on text PKs
-- also guards against generator bugs that could produce colliding UUIDs.
-- ─────────────────────────────────────────────────────────────────────────────

-- 02-T01: Duplicate primary keys — raw.claims
SELECT claim_id, COUNT(*) AS duplicate_count, 'DUPLICATE PK: raw.claims' AS failure_reason
FROM raw.claims
GROUP BY claim_id
HAVING COUNT(*) > 1;

-- 02-T02: Duplicate primary keys — raw.members
SELECT member_id, COUNT(*) AS duplicate_count, 'DUPLICATE PK: raw.members' AS failure_reason
FROM raw.members
GROUP BY member_id
HAVING COUNT(*) > 1;

-- 02-T03: Duplicate primary keys — raw.providers
SELECT provider_id, COUNT(*) AS duplicate_count, 'DUPLICATE PK: raw.providers' AS failure_reason
FROM raw.providers
GROUP BY provider_id
HAVING COUNT(*) > 1;

-- 02-T04: Duplicate primary keys — raw.fraud_investigation_outcomes
-- Extra critical: schema has UNIQUE (investigation_id) constraint.
-- A duplicate here means the Debezium CDC loop fired twice.
SELECT investigation_id, COUNT(*) AS duplicate_count,
       'DUPLICATE INVESTIGATION_ID: raw.fraud_investigation_outcomes' AS failure_reason
FROM raw.fraud_investigation_outcomes
GROUP BY investigation_id
HAVING COUNT(*) > 1;

-- 02-T05: Duplicate primary keys — raw.payments.reference_number
-- reference_number has a UNIQUE constraint; duplicates indicate payment processing bugs.
SELECT reference_number, COUNT(*) AS duplicate_count,
       'DUPLICATE REFERENCE_NUMBER: raw.payments' AS failure_reason
FROM raw.payments
WHERE reference_number IS NOT NULL
GROUP BY reference_number
HAVING COUNT(*) > 1;

-- 02-T06: Duplicate composite key — raw.stock_levels (facility_id, drug_code, reported_date)
SELECT facility_id, drug_code, reported_date, COUNT(*) AS duplicate_count,
       'DUPLICATE COMPOSITE KEY: raw.stock_levels' AS failure_reason
FROM raw.stock_levels
GROUP BY facility_id, drug_code, reported_date
HAVING COUNT(*) > 1;

-- 02-T07: Duplicate composite key — raw.model_performance_log (model_version, evaluation_date)
SELECT model_version, evaluation_date, COUNT(*) AS duplicate_count,
       'DUPLICATE COMPOSITE KEY: raw.model_performance_log' AS failure_reason
FROM raw.model_performance_log
GROUP BY model_version, evaluation_date
HAVING COUNT(*) > 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 03 — NOT-NULL COMPLETENESS
-- Nullable in Postgres does not mean "should be null". These tests enforce
-- the business not-null requirements that go beyond what the DDL enforces,
-- and also re-verify DDL not-null constraints are holding after bulk loads.
-- ─────────────────────────────────────────────────────────────────────────────

-- 03-T01: Core identity fields must never be null
SELECT 'raw.members'   AS table_name, 'first_name NULL'   AS failure_reason, member_id   AS row_id FROM raw.members   WHERE first_name IS NULL
UNION ALL
SELECT 'raw.members',                 'last_name NULL',                        member_id            FROM raw.members   WHERE last_name  IS NULL
UNION ALL
SELECT 'raw.members',                 'date_of_birth NULL',                    member_id            FROM raw.members   WHERE date_of_birth IS NULL
UNION ALL
SELECT 'raw.members',                 'county NULL',                           member_id            FROM raw.members   WHERE county IS NULL
UNION ALL
SELECT 'raw.providers',               'provider_name NULL',                    provider_id          FROM raw.providers WHERE provider_name IS NULL
UNION ALL
SELECT 'raw.providers',               'license_number NULL',                   provider_id          FROM raw.providers WHERE license_number IS NULL
UNION ALL
SELECT 'raw.providers',               'facility_type NULL',                    provider_id          FROM raw.providers WHERE facility_type IS NULL;

-- 03-T02: Claim mandatory fields must never be null
SELECT claim_id, 'amount_claimed NULL' AS failure_reason FROM raw.claims WHERE amount_claimed IS NULL
UNION ALL
SELECT claim_id, 'status NULL'         FROM raw.claims WHERE status IS NULL
UNION ALL
SELECT claim_id, 'submitted_at NULL'   FROM raw.claims WHERE submitted_at IS NULL
UNION ALL
SELECT claim_id, 'updated_at NULL'     FROM raw.claims WHERE updated_at IS NULL
UNION ALL
SELECT claim_id, 'procedure_code NULL' FROM raw.claims WHERE procedure_code IS NULL;

-- 03-T03: Fraud pipeline mandatory fields must never be null
SELECT prediction_id AS row_id, 'fraud_probability NULL' AS failure_reason
FROM raw.fraud_predictions WHERE fraud_probability IS NULL
UNION ALL
SELECT prediction_id, 'feature_snapshot NULL'
FROM raw.fraud_predictions WHERE feature_snapshot IS NULL
UNION ALL
SELECT investigation_id, 'assigned_to NULL'
FROM raw.fraud_investigations WHERE assigned_to IS NULL
UNION ALL
SELECT outcome_id, 'final_label NULL'
FROM raw.fraud_investigation_outcomes WHERE final_label IS NULL
UNION ALL
SELECT outcome_id, 'confirmed_by NULL'
FROM raw.fraud_investigation_outcomes WHERE confirmed_by IS NULL;

-- 03-T04: Model performance log mandatory fields
SELECT log_id, 'auc_roc NULL' AS failure_reason
FROM raw.model_performance_log WHERE auc_roc IS NULL
UNION ALL
SELECT log_id, 'training_claim_count NULL'
FROM raw.model_performance_log WHERE training_claim_count IS NULL
UNION ALL
SELECT log_id, 'evaluation_date NULL'
FROM raw.model_performance_log WHERE evaluation_date IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 04 — FOREIGN KEY VALIDITY
-- Re-validates FK integrity explicitly. Even with FK constraints in DDL, bulk
-- COPY loads with session_replication_role=replica bypass constraint checks.
-- These queries catch orphaned rows that slipped through during data loading.
-- ─────────────────────────────────────────────────────────────────────────────

-- 04-T01: claims.member_id must exist in raw.members
SELECT c.claim_id, c.member_id, 'ORPHAN: claims.member_id not in members' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.members m ON m.member_id = c.member_id
WHERE m.member_id IS NULL;

-- 04-T02: claims.provider_id must exist in raw.providers
SELECT c.claim_id, c.provider_id, 'ORPHAN: claims.provider_id not in providers' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.providers p ON p.provider_id = c.provider_id
WHERE p.provider_id IS NULL;

-- 04-T03: claims.diagnosis_code must exist in raw.icd10_codes
SELECT c.claim_id, c.diagnosis_code, 'ORPHAN: claims.diagnosis_code not in icd10_codes' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.icd10_codes i ON i.icd10_code = c.diagnosis_code
WHERE i.icd10_code IS NULL;

-- 04-T04: members.plan_code must exist in raw.plans
SELECT m.member_id, m.plan_code, 'ORPHAN: members.plan_code not in plans' AS failure_reason
FROM raw.members m
LEFT JOIN raw.plans p ON p.plan_code = m.plan_code
WHERE p.plan_code IS NULL;

-- 04-T05: members.employer_id must exist in raw.employers (when not null)
SELECT m.member_id, m.employer_id, 'ORPHAN: members.employer_id not in employers' AS failure_reason
FROM raw.members m
LEFT JOIN raw.employers e ON e.employer_id = m.employer_id
WHERE m.employer_id IS NOT NULL
  AND e.employer_id IS NULL;

-- 04-T06: prescriptions.drug_code must exist in raw.drug_formulary
SELECT p.prescription_id, p.drug_code, 'ORPHAN: prescriptions.drug_code not in drug_formulary' AS failure_reason
FROM raw.prescriptions p
LEFT JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
WHERE df.drug_code IS NULL;

-- 04-T07: stock_levels.drug_code must exist in raw.drug_formulary
SELECT s.stock_id, s.drug_code, 'ORPHAN: stock_levels.drug_code not in drug_formulary' AS failure_reason
FROM raw.stock_levels s
LEFT JOIN raw.drug_formulary df ON df.drug_code = s.drug_code
WHERE df.drug_code IS NULL;

-- 04-T08: stock_levels.facility_id must exist in raw.providers
SELECT s.stock_id, s.facility_id, 'ORPHAN: stock_levels.facility_id not in providers' AS failure_reason
FROM raw.stock_levels s
LEFT JOIN raw.providers p ON p.provider_id = s.facility_id
WHERE p.provider_id IS NULL;

-- 04-T09: claim_events.claim_id must exist in raw.claims
SELECT e.event_id, e.claim_id, 'ORPHAN: claim_events.claim_id not in claims' AS failure_reason
FROM raw.claim_events e
LEFT JOIN raw.claims c ON c.claim_id = e.claim_id
WHERE c.claim_id IS NULL;

-- 04-T10: vitals.member_id must match vitals.claim_id.member_id
-- Validates that the denormalised member_id on vitals agrees with the parent claim.
SELECT v.vitals_id, v.claim_id, v.member_id AS vitals_member_id, c.member_id AS claim_member_id,
       'MEMBER MISMATCH: vitals.member_id != claims.member_id' AS failure_reason
FROM raw.vitals v
JOIN raw.claims c ON c.claim_id = v.claim_id
WHERE v.member_id != c.member_id;

-- 04-T11: prescriptions.member_id must match prescriptions.claim_id.member_id
SELECT p.prescription_id, p.claim_id, p.member_id AS rx_member_id, c.member_id AS claim_member_id,
       'MEMBER MISMATCH: prescriptions.member_id != claims.member_id' AS failure_reason
FROM raw.prescriptions p
JOIN raw.claims c ON c.claim_id = p.claim_id
WHERE p.member_id != c.member_id;

-- 04-T12: fraud_investigation_outcomes.claim_id must match parent investigation.claim_id
SELECT o.outcome_id, o.claim_id AS outcome_claim_id, i.claim_id AS inv_claim_id,
       'CLAIM MISMATCH: outcome.claim_id != investigation.claim_id' AS failure_reason
FROM raw.fraud_investigation_outcomes o
JOIN raw.fraud_investigations i ON i.investigation_id = o.investigation_id
WHERE o.claim_id != i.claim_id;

-- 04-T13: fraud_investigations.prediction_id must exist in fraud_predictions (when not null)
SELECT fi.investigation_id, fi.prediction_id,
       'ORPHAN: fraud_investigations.prediction_id not in fraud_predictions' AS failure_reason
FROM raw.fraud_investigations fi
LEFT JOIN raw.fraud_predictions fp ON fp.prediction_id = fi.prediction_id
WHERE fi.prediction_id IS NOT NULL
  AND fp.prediction_id IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 05 — ENUM / CHECK CONSTRAINT VALIDATION
-- Re-evaluates every CHECK constraint as a SELECT so violations surface as rows
-- rather than silent data corruption. Necessary when data is bulk-loaded via
-- COPY (which bypasses constraint evaluation in some configurations).
-- ─────────────────────────────────────────────────────────────────────────────

-- 05-T01: raw.plans — plan_tier enum
SELECT plan_code, plan_tier, 'INVALID plan_tier' AS failure_reason
FROM raw.plans
WHERE plan_tier NOT IN ('basic', 'standard', 'premium', 'family', 'corporate');

-- 05-T02: raw.members — gender enum
SELECT member_id, gender, 'INVALID gender' AS failure_reason
FROM raw.members
WHERE gender NOT IN ('M', 'F');

-- 05-T03: raw.members — id_type enum
SELECT member_id, id_type, 'INVALID id_type' AS failure_reason
FROM raw.members
WHERE id_type NOT IN ('National ID', 'Passport', 'Alien ID', 'Military ID');

-- 05-T04: raw.providers — accreditation_status enum
SELECT provider_id, accreditation_status, 'INVALID accreditation_status' AS failure_reason
FROM raw.providers
WHERE accreditation_status NOT IN ('Active', 'Probation', 'Suspended', 'Terminated');

-- 05-T05: raw.claims — status enum
SELECT claim_id, status, 'INVALID claim status' AS failure_reason
FROM raw.claims
WHERE status NOT IN (
    'submitted', 'pending_review', 'approved', 'partially_approved',
    'rejected', 'paid', 'flagged_fraud', 'under_investigation', 'closed'
);

-- 05-T06: raw.claims — submission_channel enum
SELECT claim_id, submission_channel, 'INVALID submission_channel' AS failure_reason
FROM raw.claims
WHERE submission_channel NOT IN ('portal', 'mobile', 'api', 'paper');

-- 05-T07: raw.claim_events — event_type enum
SELECT event_id, event_type, 'INVALID event_type' AS failure_reason
FROM raw.claim_events
WHERE event_type NOT IN (
    'submitted', 'status_changed', 'approved', 'rejected', 'paid',
    'flagged', 'investigation_opened', 'investigation_closed', 'appealed', 'closed'
);

-- 05-T08: raw.claim_events — triggered_by enum
SELECT event_id, triggered_by, 'INVALID triggered_by' AS failure_reason
FROM raw.claim_events
WHERE triggered_by NOT IN ('system', 'auto_approve', 'investigator', 'api', 'airflow');

-- 05-T09: raw.payments — payment_method enum
SELECT payment_id, payment_method, 'INVALID payment_method' AS failure_reason
FROM raw.payments
WHERE payment_method NOT IN ('EFT', 'MPESA', 'Cheque', 'Cash');

-- 05-T10: raw.fraud_predictions — risk_tier enum
SELECT prediction_id, risk_tier, 'INVALID risk_tier' AS failure_reason
FROM raw.fraud_predictions
WHERE risk_tier NOT IN ('low', 'medium', 'high', 'critical');

-- 05-T11: raw.fraud_investigations — status enum
SELECT investigation_id, status, 'INVALID investigation status' AS failure_reason
FROM raw.fraud_investigations
WHERE status NOT IN ('open', 'in_progress', 'pending_review', 'closed', 'escalated');

-- 05-T12: raw.fraud_investigations — priority enum
SELECT investigation_id, priority, 'INVALID investigation priority' AS failure_reason
FROM raw.fraud_investigations
WHERE priority NOT IN ('low', 'medium', 'high', 'critical');

-- 05-T13: raw.fraud_investigation_outcomes — final_label enum
SELECT outcome_id, final_label, 'INVALID final_label' AS failure_reason
FROM raw.fraud_investigation_outcomes
WHERE final_label NOT IN ('confirmed_fraud', 'legitimate', 'inconclusive');

-- 05-T14: raw.fraud_investigation_outcomes — fraud_type enum
SELECT outcome_id, fraud_type, 'INVALID fraud_type' AS failure_reason
FROM raw.fraud_investigation_outcomes
WHERE fraud_type IS NOT NULL
  AND fraud_type NOT IN (
      'phantom_billing', 'threshold_manip', 'benefit_sharing', 'duplicate_claim', 'other'
  );

-- 05-T15: raw.providers — risk_score must be 0–1 when present
SELECT provider_id, risk_score, 'risk_score OUT OF RANGE 0-1' AS failure_reason
FROM raw.providers
WHERE risk_score IS NOT NULL
  AND risk_score NOT BETWEEN 0 AND 1;

-- 05-T16: raw.fraud_predictions — fraud_probability must be 0–1
SELECT prediction_id, fraud_probability, 'fraud_probability OUT OF RANGE 0-1' AS failure_reason
FROM raw.fraud_predictions
WHERE fraud_probability NOT BETWEEN 0 AND 1;

-- 05-T17: raw.model_performance_log — all metric columns must be 0–1
SELECT log_id, 'auc_roc OUT OF RANGE' AS failure_reason FROM raw.model_performance_log WHERE auc_roc NOT BETWEEN 0 AND 1
UNION ALL
SELECT log_id, 'precision_at_10pct OUT OF RANGE' FROM raw.model_performance_log WHERE precision_at_10pct IS NOT NULL AND precision_at_10pct NOT BETWEEN 0 AND 1
UNION ALL
SELECT log_id, 'recall OUT OF RANGE'              FROM raw.model_performance_log WHERE recall IS NOT NULL AND recall NOT BETWEEN 0 AND 1
UNION ALL
SELECT log_id, 'f1_score OUT OF RANGE'            FROM raw.model_performance_log WHERE f1_score IS NOT NULL AND f1_score NOT BETWEEN 0 AND 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 06 — CROSS-COLUMN ROW RULES
-- Business rules that require comparing multiple columns within the same row.
-- These are the rules that CHECK constraints can enforce but do not always
-- catch when expressed across three or more columns.
-- ─────────────────────────────────────────────────────────────────────────────

-- 06-T01: amount_approved must not exceed amount_claimed
SELECT claim_id, amount_claimed, amount_approved,
       'amount_approved EXCEEDS amount_claimed' AS failure_reason
FROM raw.claims
WHERE amount_approved IS NOT NULL
  AND amount_approved > amount_claimed;

-- 06-T02: amount_approved must only be set for terminal positive statuses
-- Rejected/flagged claims must not have an approved amount — this would
-- indicate a payment was authorised for a fraudulent claim.
SELECT claim_id, status, amount_approved,
       'amount_approved SET ON NON-APPROVED STATUS' AS failure_reason
FROM raw.claims
WHERE amount_approved IS NOT NULL
  AND status NOT IN ('approved', 'partially_approved', 'paid');

-- 06-T03: date_of_service must be <= date_submitted for every claim
SELECT claim_id, date_of_service, date_submitted,
       'date_of_service AFTER date_submitted' AS failure_reason
FROM raw.claims
WHERE date_of_service > date_submitted;

-- 06-T04: updated_at must be >= submitted_at
SELECT claim_id, submitted_at, updated_at,
       'updated_at BEFORE submitted_at' AS failure_reason
FROM raw.claims
WHERE updated_at < submitted_at;

-- 06-T05: Employer contract_end must be after contract_start
SELECT employer_id, contract_start, contract_end,
       'contract_end BEFORE OR EQUAL TO contract_start' AS failure_reason
FROM raw.employers
WHERE contract_end IS NOT NULL
  AND contract_end <= contract_start;

-- 06-T06: Member term_date must be after enrol_date
SELECT member_id, enrol_date, term_date,
       'term_date BEFORE OR EQUAL TO enrol_date' AS failure_reason
FROM raw.members
WHERE term_date IS NOT NULL
  AND term_date <= enrol_date;

-- 06-T07: Provider contract_end_date must be after contract_start_date
SELECT provider_id, contract_start_date, contract_end_date,
       'contract_end_date BEFORE OR EQUAL TO contract_start_date' AS failure_reason
FROM raw.providers
WHERE contract_end_date IS NOT NULL
  AND contract_end_date <= contract_start_date;

-- 06-T08: fraud_investigation closed_at must be >= opened_at
SELECT investigation_id, opened_at, closed_at,
       'closed_at BEFORE opened_at' AS failure_reason
FROM raw.fraud_investigations
WHERE closed_at IS NOT NULL
  AND closed_at < opened_at;

-- 06-T09: Plans — inpatient + outpatient limits should not exceed annual limit
-- A plan where sub-limits sum to more than the annual cap is financially incoherent.
SELECT plan_code,
       annual_limit_kes,
       inpatient_limit_kes,
       outpatient_limit_kes,
       (inpatient_limit_kes + outpatient_limit_kes) AS sum_of_sublimits,
       'SUBLIMITS EXCEED ANNUAL LIMIT' AS failure_reason
FROM raw.plans
WHERE (inpatient_limit_kes + outpatient_limit_kes) > annual_limit_kes;

-- 06-T10: Model training fraud count must not exceed total training claim count
SELECT log_id, training_claim_count, training_fraud_count,
       'training_fraud_count EXCEEDS training_claim_count' AS failure_reason
FROM raw.model_performance_log
WHERE training_fraud_count > training_claim_count;

-- 06-T11: amount_recovered_kes must not exceed estimated_loss_kes
-- Cannot recover more than was estimated to have been lost.
SELECT outcome_id, estimated_loss_kes, amount_recovered_kes,
       'amount_recovered EXCEEDS estimated_loss' AS failure_reason
FROM raw.fraud_investigation_outcomes
WHERE amount_recovered_kes IS NOT NULL
  AND estimated_loss_kes IS NOT NULL
  AND amount_recovered_kes > estimated_loss_kes;

-- 06-T12: DLQ — resolved_at must be set if and only if resolved = TRUE
SELECT dlq_id, resolved, resolved_at,
       'resolved=TRUE BUT resolved_at IS NULL' AS failure_reason
FROM raw.dlq_events
WHERE resolved = TRUE AND resolved_at IS NULL
UNION ALL
SELECT dlq_id, resolved, resolved_at,
       'resolved=FALSE BUT resolved_at IS SET'
FROM raw.dlq_events
WHERE resolved = FALSE AND resolved_at IS NOT NULL;

-- 06-T13: Model — promoted=TRUE requires both promotion_reason and s3_artefact_path
SELECT log_id, promoted, promotion_reason, s3_artefact_path,
       'PROMOTED MODEL MISSING promotion_reason OR s3_artefact_path' AS failure_reason
FROM raw.model_performance_log
WHERE promoted = TRUE
  AND (promotion_reason IS NULL OR s3_artefact_path IS NULL);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 07 — CROSS-TABLE BUSINESS RULES
-- Rules that span two or more tables. These cannot be expressed as CHECK
-- constraints and are the most commonly missed in schema-only reviews.
-- ─────────────────────────────────────────────────────────────────────────────

-- 07-T01: Claims submitted after a member's term_date are invalid
-- A lapsed member should not be able to generate active claims.
SELECT c.claim_id, c.member_id, c.date_of_service, m.term_date,
       'CLAIM AFTER MEMBER TERM DATE' AS failure_reason
FROM raw.claims c
JOIN raw.members m ON m.member_id = c.member_id
WHERE m.term_date IS NOT NULL
  AND c.date_of_service > m.term_date;

-- 07-T02: Claims submitted before a member's enrol_date are invalid
-- No claims should pre-date membership.
SELECT c.claim_id, c.member_id, c.date_of_service, m.enrol_date,
       'CLAIM BEFORE MEMBER ENROL DATE' AS failure_reason
FROM raw.claims c
JOIN raw.members m ON m.member_id = c.member_id
WHERE c.date_of_service < m.enrol_date;

-- 07-T03: Claims from Suspended or Terminated providers should be flagged
-- Accreditation checks are a primary fraud control. Claims from non-accredited
-- providers that are NOT in an investigation state are a governance failure.
SELECT c.claim_id, c.provider_id, p.accreditation_status, c.status,
       'CLAIM FROM SUSPENDED/TERMINATED PROVIDER NOT UNDER INVESTIGATION' AS failure_reason
FROM raw.claims c
JOIN raw.providers p ON p.provider_id = c.provider_id
WHERE p.accreditation_status IN ('Suspended', 'Terminated')
  AND c.status NOT IN ('flagged_fraud', 'under_investigation', 'rejected', 'closed');

-- 07-T04: Paid claims must have at least one payment record
-- A claim in status='paid' with no payment row means money was marked as paid
-- but no disbursement audit trail exists.
SELECT c.claim_id, c.status, c.amount_approved,
       'PAID CLAIM HAS NO PAYMENT RECORD' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.payments pay ON pay.claim_id = c.claim_id
WHERE c.status = 'paid'
  AND pay.payment_id IS NULL;

-- 07-T05: Rejected or fraud-flagged claims must have no payments
SELECT pay.payment_id, pay.claim_id, c.status,
       'PAYMENT EXISTS FOR REJECTED/FRAUD CLAIM' AS failure_reason
FROM raw.payments pay
JOIN raw.claims c ON c.claim_id = pay.claim_id
WHERE c.status IN ('rejected', 'flagged_fraud');

-- 07-T06: Total payments per claim must not exceed amount_approved
-- Protects against duplicate payment bugs where the same claim is paid twice.
SELECT c.claim_id, c.amount_approved,
       SUM(pay.amount_paid_kes) AS total_paid,
       'TOTAL PAYMENTS EXCEED AMOUNT APPROVED' AS failure_reason
FROM raw.payments pay
JOIN raw.claims c ON c.claim_id = pay.claim_id
WHERE c.amount_approved IS NOT NULL
GROUP BY c.claim_id, c.amount_approved
HAVING SUM(pay.amount_paid_kes) > c.amount_approved;

-- 07-T07: Every closed investigation must have exactly one outcome row
-- The 1:1 relationship between fraud_investigations and fraud_investigation_outcomes
-- is enforced by UNIQUE(investigation_id) on outcomes, but we also verify
-- closed investigations are not left without an outcome (the other direction).
SELECT fi.investigation_id, fi.status, fi.closed_at,
       'CLOSED INVESTIGATION HAS NO OUTCOME' AS failure_reason
FROM raw.fraud_investigations fi
LEFT JOIN raw.fraud_investigation_outcomes o ON o.investigation_id = fi.investigation_id
WHERE fi.status = 'closed'
  AND o.outcome_id IS NULL;

-- 07-T08: Open investigations must not have an outcome row
SELECT o.outcome_id, o.investigation_id, fi.status,
       'OUTCOME EXISTS FOR NON-CLOSED INVESTIGATION' AS failure_reason
FROM raw.fraud_investigation_outcomes o
JOIN raw.fraud_investigations fi ON fi.investigation_id = o.investigation_id
WHERE fi.status != 'closed';

-- 07-T09: fraud_type must be set when final_label = 'confirmed_fraud'
-- A confirmed fraud with no fraud_type cannot be routed to the correct
-- remediation workflow.
SELECT outcome_id, final_label, fraud_type,
       'confirmed_fraud OUTCOME MISSING fraud_type' AS failure_reason
FROM raw.fraud_investigation_outcomes
WHERE final_label = 'confirmed_fraud'
  AND fraud_type IS NULL;

-- 07-T10: fraud_type must be null when final_label = 'legitimate'
-- A legitimate claim with a fraud_type set is a data contradiction that would
-- corrupt ML training labels.
SELECT outcome_id, final_label, fraud_type,
       'legitimate OUTCOME HAS fraud_type SET' AS failure_reason
FROM raw.fraud_investigation_outcomes
WHERE final_label = 'legitimate'
  AND fraud_type IS NOT NULL;

-- 07-T11: Vitals member must be enrolled at time of service
SELECT v.vitals_id, v.member_id, v.date_taken, m.enrol_date,
       'VITALS RECORDED BEFORE MEMBER ENROL DATE' AS failure_reason
FROM raw.vitals v
JOIN raw.members m ON m.member_id = v.member_id
WHERE v.date_taken < m.enrol_date;

-- 07-T12: Prescriptions from controlled substances must reference valid claim
-- Extra traceability check for controlled drugs — every prescription must
-- have a corresponding claim with a recognised clinical diagnosis.
SELECT p.prescription_id, p.drug_code, df.is_controlled_substance,
       'CONTROLLED SUBSTANCE PRESCRIPTION WITH NO LINKED CLAIM' AS failure_reason
FROM raw.prescriptions p
JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
LEFT JOIN raw.claims c ON c.claim_id = p.claim_id
WHERE df.is_controlled_substance = TRUE
  AND c.claim_id IS NULL;

-- 07-T13: No duplicate open investigations per claim
-- A single claim should not have two open investigations simultaneously.
SELECT claim_id, COUNT(*) AS open_investigation_count,
       'MULTIPLE OPEN INVESTIGATIONS FOR SAME CLAIM' AS failure_reason
FROM raw.fraud_investigations
WHERE status IN ('open', 'in_progress', 'pending_review', 'escalated')
GROUP BY claim_id
HAVING COUNT(*) > 1;

-- 07-T14: Plan limit consistency — claims should not exceed annual limit per member per year
-- Provides early warning of benefit exhaustion anomalies or unbounded fraud.
SELECT
    c.member_id,
    p.plan_code,
    EXTRACT(YEAR FROM c.date_of_service) AS service_year,
    SUM(c.amount_claimed)                AS total_claimed,
    p.annual_limit_kes,
    'MEMBER ANNUAL CLAIMED AMOUNT EXCEEDS PLAN LIMIT' AS failure_reason
FROM raw.claims c
JOIN raw.members m  ON m.member_id = c.member_id
JOIN raw.plans   p  ON p.plan_code  = m.plan_code
WHERE c.status NOT IN ('rejected', 'flagged_fraud')
GROUP BY c.member_id, p.plan_code, EXTRACT(YEAR FROM c.date_of_service), p.annual_limit_kes
HAVING SUM(c.amount_claimed) > p.annual_limit_kes;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 08 — TEMPORAL CONSISTENCY
-- Date and timestamp plausibility rules. Invalid timestamps are a common
-- symptom of generator bugs, timezone handling errors, and ETL misconfiguration.
-- ─────────────────────────────────────────────────────────────────────────────

-- 08-T01: No claim dates in the future
SELECT claim_id, date_of_service, 'date_of_service IN FUTURE' AS failure_reason
FROM raw.claims
WHERE date_of_service > CURRENT_DATE;

-- 08-T02: No submission timestamps more than 90 days after date_of_service
-- Legitimate claims are submitted within 30–90 days. Older submissions indicate
-- backdating fraud or ETL errors.
SELECT claim_id, date_of_service, date_submitted,
       (date_submitted - date_of_service) AS lag_days,
       'SUBMISSION LAG > 90 DAYS' AS failure_reason
FROM raw.claims
WHERE (date_submitted - date_of_service) > 90;

-- 08-T03: Member date_of_birth must be in the past and plausible
SELECT member_id, date_of_birth,
       EXTRACT(YEAR FROM AGE(date_of_birth)) AS age_years,
       'IMPLAUSIBLE date_of_birth (future or age > 120)' AS failure_reason
FROM raw.members
WHERE date_of_birth >= CURRENT_DATE
   OR EXTRACT(YEAR FROM AGE(date_of_birth)) > 120;

-- 08-T04: Member enrol_date must not be in the future
SELECT member_id, enrol_date, 'enrol_date IN FUTURE' AS failure_reason
FROM raw.members
WHERE enrol_date > CURRENT_DATE;

-- 08-T05: Provider contract_start_date must not be in the future
SELECT provider_id, contract_start_date, 'contract_start_date IN FUTURE' AS failure_reason
FROM raw.providers
WHERE contract_start_date > CURRENT_DATE;

-- 08-T06: Payments must be dated after claim submission
SELECT pay.payment_id, pay.claim_id, pay.paid_at, c.submitted_at,
       'paid_at BEFORE claim submitted_at' AS failure_reason
FROM raw.payments pay
JOIN raw.claims c ON c.claim_id = pay.claim_id
WHERE pay.paid_at < c.submitted_at;

-- 08-T07: Fraud outcome confirmed_at must be after investigation opened_at
SELECT o.outcome_id, o.confirmed_at, fi.opened_at,
       'confirmed_at BEFORE investigation opened_at' AS failure_reason
FROM raw.fraud_investigation_outcomes o
JOIN raw.fraud_investigations fi ON fi.investigation_id = o.investigation_id
WHERE o.confirmed_at < fi.opened_at;

-- 08-T08: Stock snapshots must not be dated in the future
SELECT stock_id, facility_id, reported_date, 'reported_date IN FUTURE' AS failure_reason
FROM raw.stock_levels
WHERE reported_date > CURRENT_DATE;

-- 08-T09: Vitals date_taken must match or be close to claim date_of_service
-- Vitals recorded more than 1 day before or after the service date are suspect.
SELECT v.vitals_id, v.date_taken, c.date_of_service,
       ABS(v.date_taken - c.date_of_service) AS day_diff,
       'VITALS date_taken MORE THAN 1 DAY FROM claim date_of_service' AS failure_reason
FROM raw.vitals v
JOIN raw.claims c ON c.claim_id = v.claim_id
WHERE ABS(v.date_taken - c.date_of_service) > 1;

-- 08-T10: Fraud predictions must be generated after claim submission
SELECT fp.prediction_id, fp.predicted_at, c.submitted_at,
       'predicted_at BEFORE claim submitted_at' AS failure_reason
FROM raw.fraud_predictions fp
JOIN raw.claims c ON c.claim_id = fp.claim_id
WHERE fp.predicted_at < c.submitted_at;

-- 08-T11: Prescription prescribed_at must not be before claim date_of_service
SELECT p.prescription_id, p.prescribed_at, c.date_of_service,
       'prescribed_at BEFORE claim date_of_service' AS failure_reason
FROM raw.prescriptions p
JOIN raw.claims c ON c.claim_id = p.claim_id
WHERE p.prescribed_at::date < c.date_of_service;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 09 — CLINICAL PLAUSIBILITY
-- Validates that recorded clinical measurements are physiologically coherent.
-- These rules catch data entry errors, unit conversion bugs (e.g. °F vs °C),
-- and synthetic data generation issues before they corrupt ML features.
-- ─────────────────────────────────────────────────────────────────────────────

-- 09-T01: Systolic BP must always exceed diastolic BP
-- Physiologically impossible otherwise; indicates measurement or entry error.
SELECT vitals_id, systolic_bp, diastolic_bp,
       'systolic_bp <= diastolic_bp (PHYSIOLOGICALLY IMPOSSIBLE)' AS failure_reason
FROM raw.vitals
WHERE systolic_bp IS NOT NULL
  AND diastolic_bp IS NOT NULL
  AND systolic_bp <= diastolic_bp;

-- 09-T02: BMI must be consistent with recorded weight and height
-- Tolerance of ±1.0 kg/m² accounts for rounding differences.
SELECT vitals_id, weight_kg, height_cm, bmi,
       ROUND((weight_kg / POWER(height_cm / 100.0, 2))::numeric, 2) AS computed_bmi,
       ABS(bmi - ROUND((weight_kg / POWER(height_cm / 100.0, 2))::numeric, 2)) AS bmi_discrepancy,
       'BMI INCONSISTENT WITH WEIGHT AND HEIGHT (tolerance > 1.0)' AS failure_reason
FROM raw.vitals
WHERE weight_kg IS NOT NULL
  AND height_cm IS NOT NULL
  AND bmi IS NOT NULL
  AND ABS(bmi - ROUND((weight_kg / POWER(height_cm / 100.0, 2))::numeric, 2)) > 1.0;

-- 09-T03: Critically low SpO2 (< 90%) should trigger clinical review flag
-- These records are not invalid but require clinical annotation or review.
-- Surfacing them here enables proactive governance review.
SELECT vitals_id, claim_id, member_id, spo2_pct,
       'CRITICAL: SpO2 < 90% — requires clinical review annotation' AS failure_reason
FROM raw.vitals
WHERE spo2_pct IS NOT NULL
  AND spo2_pct < 90;

-- 09-T04: Temperature in Celsius — flag values that suggest °F was accidentally stored
-- 37°C is normal body temperature. Values > 45 suggest °F (e.g. 98.6°F stored as-is).
SELECT vitals_id, temperature_c,
       'TEMPERATURE > 45°C — possible unit error (°F stored as °C)' AS failure_reason
FROM raw.vitals
WHERE temperature_c IS NOT NULL
  AND temperature_c > 45;

-- 09-T05: Pulse rate outside survivable range
SELECT vitals_id, heart_rate,
       'heart_rate OUTSIDE PLAUSIBLE RANGE (20–300)' AS failure_reason
FROM raw.vitals
WHERE heart_rate IS NOT NULL
  AND (heart_rate < 20 OR heart_rate > 300);

-- 09-T06: Controlled substance prescriptions must not exceed 30 days duration
-- Regulatory requirement for opioids and controlled substances.
SELECT p.prescription_id, p.drug_code, p.duration_days, df.is_controlled_substance,
       'CONTROLLED SUBSTANCE PRESCRIBED FOR > 30 DAYS' AS failure_reason
FROM raw.prescriptions p
JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
WHERE df.is_controlled_substance = TRUE
  AND p.duration_days > 30;

-- 09-T07: Consultation claims for non-emergency visits should have vitals
-- Absence of vitals on a consultation claim is the primary phantom-billing signal.
-- This test surfaces the raw count for monitoring (not necessarily a hard failure
-- if the ML model is already flagging them — but required for governance audit).
SELECT c.claim_id, c.provider_id, c.status,
       'CONSULTATION CLAIM WITH NO VITALS RECORD (phantom billing signal)' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.vitals v ON v.claim_id = c.claim_id
WHERE c.procedure_code LIKE 'CONS%'
  AND c.is_emergency = FALSE
  AND c.status NOT IN ('rejected', 'flagged_fraud', 'under_investigation')
  AND v.vitals_id IS NULL;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 10 — FRAUD PIPELINE LOGIC
-- Validates the consistency and completeness of the fraud detection pipeline
-- tables. A corrupt fraud pipeline produces incorrect ML training labels, which
-- is worse than missing labels — it actively degrades model quality.
-- ─────────────────────────────────────────────────────────────────────────────

-- 10-T01: Risk tier must be consistent with fraud_probability thresholds
-- Thresholds: low < 0.3, medium 0.3–0.6, high 0.6–0.8, critical >= 0.8
SELECT prediction_id, fraud_probability, risk_tier,
       'risk_tier INCONSISTENT WITH fraud_probability' AS failure_reason
FROM raw.fraud_predictions
WHERE
    (risk_tier = 'low'      AND fraud_probability >= 0.30) OR
    (risk_tier = 'medium'   AND (fraud_probability < 0.30 OR fraud_probability >= 0.60)) OR
    (risk_tier = 'high'     AND (fraud_probability < 0.60 OR fraud_probability >= 0.80)) OR
    (risk_tier = 'critical' AND fraud_probability < 0.80);

-- 10-T02: feature_snapshot must be a JSON object, not an array or primitive
-- Flink must publish a JSON object keyed by feature name. An array or primitive
-- cannot be used for feature-level explainability or audit queries.
SELECT prediction_id,
       jsonb_typeof(feature_snapshot) AS snapshot_type,
       'feature_snapshot IS NOT A JSON OBJECT' AS failure_reason
FROM raw.fraud_predictions
WHERE jsonb_typeof(feature_snapshot) != 'object';

-- 10-T03: Fraud predictions must be generated within a reasonable window of claim submission
-- SLA: predictions should arrive within 60 minutes for operational phases.
-- Latency > 1 hour on a recent claim (within the last 30 days) indicates Flink lag.
SELECT fp.prediction_id, fp.claim_id,
       EXTRACT(EPOCH FROM (fp.predicted_at - c.submitted_at)) / 60 AS latency_minutes,
       'FRAUD PREDICTION LATENCY > 60 MIN (recent claim)' AS failure_reason
FROM raw.fraud_predictions fp
JOIN raw.claims c ON c.claim_id = fp.claim_id
WHERE c.submitted_at >= NOW() - INTERVAL '30 days'
  AND fp.predicted_at > c.submitted_at + INTERVAL '60 minutes';

-- 10-T04: Every claim should have at most one prediction per model version
-- Multiple predictions for the same (claim, model_version) indicate a Flink
-- exactly-once processing failure.
SELECT claim_id, model_version, COUNT(*) AS prediction_count,
       'DUPLICATE PREDICTIONS: same claim + model_version' AS failure_reason
FROM raw.fraud_predictions
GROUP BY claim_id, model_version
HAVING COUNT(*) > 1;

-- 10-T05: Promoted models must have AUC-ROC above minimum threshold (0.70)
-- Any promoted model below this threshold should never have been promoted.
SELECT log_id, model_version, auc_roc,
       'PROMOTED MODEL AUC-ROC BELOW MINIMUM THRESHOLD (0.70)' AS failure_reason
FROM raw.model_performance_log
WHERE promoted = TRUE
  AND auc_roc < 0.70;

-- 10-T06: At most one model should be promoted per major version prefix
-- Two promoted rows for 'v2.x' indicates a promotion state machine bug.
SELECT
    SPLIT_PART(model_version, '.', 1) AS major_version,
    COUNT(*) AS promoted_count,
    'MULTIPLE PROMOTED MODELS FOR SAME MAJOR VERSION' AS failure_reason
FROM raw.model_performance_log
WHERE promoted = TRUE
GROUP BY SPLIT_PART(model_version, '.', 1)
HAVING COUNT(*) > 1;

-- 10-T07: Training set fraud rate sanity check
-- A training set with > 50% fraud is unrealistic for a production model.
SELECT log_id, model_version,
       ROUND(100.0 * training_fraud_count / NULLIF(training_claim_count, 0), 2) AS fraud_rate_pct,
       'TRAINING FRAUD RATE > 50% (unrealistic training set)' AS failure_reason
FROM raw.model_performance_log
WHERE training_claim_count > 0
  AND (training_fraud_count::float / training_claim_count) > 0.50;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 11 — FINANCIAL INTEGRITY
-- Validates the monetary integrity of the claims and payments tables.
-- Financial errors (double payments, unapproved disbursements) have direct
-- P&L impact and regulatory consequences.
-- ─────────────────────────────────────────────────────────────────────────────

-- 11-T01: No zero-amount claims
SELECT claim_id, amount_claimed, 'ZERO OR NEGATIVE amount_claimed' AS failure_reason
FROM raw.claims
WHERE amount_claimed <= 0;

-- 11-T02: No zero-amount payments
SELECT payment_id, amount_paid_kes, 'ZERO OR NEGATIVE amount_paid_kes' AS failure_reason
FROM raw.payments
WHERE amount_paid_kes <= 0;

-- 11-T03: Negative drug unit costs are invalid
SELECT drug_code, unit_cost_kes, 'NEGATIVE unit_cost_kes' AS failure_reason
FROM raw.drug_formulary
WHERE unit_cost_kes IS NOT NULL
  AND unit_cost_kes < 0;

-- 11-T04: Claims above 50,000 KES without prior authorisation
-- SHA policy: amounts above the threshold require prior authorisation.
-- Claims without it should not be in approved/paid status.
SELECT claim_id, amount_claimed, prior_auth_number, status,
       'HIGH-VALUE CLAIM (>50,000 KES) APPROVED WITHOUT PRIOR AUTH' AS failure_reason
FROM raw.claims
WHERE amount_claimed > 50000
  AND prior_auth_number IS NULL
  AND status IN ('approved', 'paid', 'partially_approved');

-- 11-T05: Threshold manipulation signal — amounts suspiciously close to KES thresholds
-- This test surfaces claims in the manipulation band (92%–99% of 5k/10k/25k)
-- for governance audit. Not a hard failure but a required monitoring output.
SELECT claim_id, amount_claimed, status,
       CASE
           WHEN amount_claimed BETWEEN 4600 AND 4950 THEN '5,000 KES threshold band'
           WHEN amount_claimed BETWEEN 9200 AND 9900 THEN '10,000 KES threshold band'
           WHEN amount_claimed BETWEEN 23000 AND 24750 THEN '25,000 KES threshold band'
       END AS threshold_band,
       'AMOUNT IN THRESHOLD MANIPULATION BAND (governance review required)' AS failure_reason
FROM raw.claims
WHERE
    amount_claimed BETWEEN 4600 AND 4950   OR
    amount_claimed BETWEEN 9200 AND 9900   OR
    amount_claimed BETWEEN 23000 AND 24750;

-- 11-T06: Cumulative payments must not exceed estimated fraud loss on confirmed cases
SELECT o.outcome_id, o.claim_id, o.estimated_loss_kes,
       SUM(pay.amount_paid_kes) AS total_disbursed,
       'TOTAL DISBURSED EXCEEDS ESTIMATED FRAUD LOSS' AS failure_reason
FROM raw.fraud_investigation_outcomes o
JOIN raw.payments pay ON pay.claim_id = o.claim_id
WHERE o.final_label = 'confirmed_fraud'
  AND o.estimated_loss_kes IS NOT NULL
GROUP BY o.outcome_id, o.claim_id, o.estimated_loss_kes
HAVING SUM(pay.amount_paid_kes) > o.estimated_loss_kes;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 12 — EVENT LOG INTEGRITY
-- raw.claim_events is the immutable audit trail of every claim state change.
-- Its integrity is required for regulatory audit, SLA measurement, and the
-- event-sourcing pattern used by downstream consumers.
-- ─────────────────────────────────────────────────────────────────────────────

-- 12-T01: Every claim must have at least one event (the 'submitted' event)
SELECT c.claim_id, 'CLAIM HAS NO EVENTS IN claim_events' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.claim_events e ON e.claim_id = c.claim_id
WHERE e.event_id IS NULL;

-- 12-T02: The first event for every claim must be of type 'submitted'
-- Any other first event type indicates events arrived out of order or were
-- incorrectly generated.
WITH first_events AS (
    SELECT DISTINCT ON (claim_id)
        claim_id, event_type, event_at
    FROM raw.claim_events
    ORDER BY claim_id, event_at ASC
)
SELECT claim_id, event_type,
       'FIRST CLAIM EVENT IS NOT submitted' AS failure_reason
FROM first_events
WHERE event_type != 'submitted';

-- 12-T03: No events should exist after a claim reaches 'closed' status
-- The claim_events log is immutable; no further state changes are valid.
WITH closed_events AS (
    SELECT claim_id, event_at AS closed_at
    FROM raw.claim_events
    WHERE event_type = 'closed'
)
SELECT e.event_id, e.claim_id, e.event_at, ce.closed_at,
       'EVENT RECORDED AFTER CLAIM WAS CLOSED' AS failure_reason
FROM raw.claim_events e
JOIN closed_events ce ON ce.claim_id = e.claim_id
WHERE e.event_at > ce.closed_at
  AND e.event_type != 'closed';

-- 12-T04: Events for the same claim must have non-decreasing timestamps
-- Out-of-order event_at values break event-sourcing consumers and SLA metrics.
WITH ordered_events AS (
    SELECT
        event_id,
        claim_id,
        event_at,
        LAG(event_at) OVER (PARTITION BY claim_id ORDER BY event_at) AS prev_event_at
    FROM raw.claim_events
)
SELECT event_id, claim_id, prev_event_at, event_at,
       'EVENT TIMESTAMP EARLIER THAN PREVIOUS EVENT FOR SAME CLAIM' AS failure_reason
FROM ordered_events
WHERE prev_event_at IS NOT NULL
  AND event_at < prev_event_at;

-- 12-T05: Claims with status 'paid' must have a 'paid' event in the log
SELECT c.claim_id, c.status,
       'CLAIM STATUS IS paid BUT NO paid EVENT IN claim_events' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.claim_events e
    ON e.claim_id = c.claim_id
    AND e.event_type = 'paid'
WHERE c.status = 'paid'
  AND e.event_id IS NULL;

-- 12-T06: Claims with status 'flagged_fraud' must have a 'flagged' event
SELECT c.claim_id, c.status,
       'CLAIM STATUS IS flagged_fraud BUT NO flagged EVENT IN claim_events' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.claim_events e
    ON e.claim_id = c.claim_id
    AND e.event_type = 'flagged'
WHERE c.status = 'flagged_fraud'
  AND e.event_id IS NULL;

-- 12-T07: No duplicate event records (same claim_id + event_type + event_at)
-- Duplicate events indicate Kafka at-least-once delivery without deduplication.
SELECT claim_id, event_type, event_at, COUNT(*) AS duplicate_count,
       'DUPLICATE CLAIM EVENT (claim_id + event_type + event_at)' AS failure_reason
FROM raw.claim_events
GROUP BY claim_id, event_type, event_at
HAVING COUNT(*) > 1;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 13 — DATA FRESHNESS
-- Monitors _loaded_at lag to detect stalled pipelines. Kafka Connect, Airflow,
-- and Flink all populate _loaded_at. A table that has not received new rows
-- within its expected cadence indicates a pipeline failure.
-- ─────────────────────────────────────────────────────────────────────────────

-- 13-T01: Claims table must receive new rows at least once per hour (operational)
-- Adjust the interval to match expected claim submission volume.
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    NOW() - MAX(_loaded_at)                            AS lag,
    'raw.claims NOT UPDATED IN LAST HOUR'              AS failure_reason
FROM raw.claims
HAVING MAX(_loaded_at) < NOW() - INTERVAL '1 hour'
    OR MAX(_loaded_at) IS NULL;

-- 13-T02: Members table (full refresh daily) must have been loaded today
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    'raw.members NOT REFRESHED TODAY'                  AS failure_reason
FROM raw.members
HAVING MAX(_loaded_at)::date < CURRENT_DATE
    OR MAX(_loaded_at) IS NULL;

-- 13-T03: Providers table must have been loaded today
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    'raw.providers NOT REFRESHED TODAY'                AS failure_reason
FROM raw.providers
HAVING MAX(_loaded_at)::date < CURRENT_DATE
    OR MAX(_loaded_at) IS NULL;

-- 13-T04: Plans table must have been loaded today
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    'raw.plans NOT REFRESHED TODAY'                    AS failure_reason
FROM raw.plans
HAVING MAX(_loaded_at)::date < CURRENT_DATE
    OR MAX(_loaded_at) IS NULL;

-- 13-T05: Drug formulary table must have been loaded within the last 7 days
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    'raw.drug_formulary NOT REFRESHED IN 7 DAYS'       AS failure_reason
FROM raw.drug_formulary
HAVING MAX(_loaded_at) < NOW() - INTERVAL '7 days'
    OR MAX(_loaded_at) IS NULL;

-- 13-T06: Fraud predictions must be arriving continuously (within 2 hours)
-- A gap > 2 hours means Flink has stopped scoring claims.
SELECT
    MAX(_loaded_at)                                    AS last_loaded,
    NOW() - MAX(_loaded_at)                            AS lag,
    'raw.fraud_predictions NOT UPDATED IN 2 HOURS'     AS failure_reason
FROM raw.fraud_predictions
HAVING MAX(_loaded_at) < NOW() - INTERVAL '2 hours'
    OR MAX(_loaded_at) IS NULL;

-- 13-T07: DLQ — any unresolved events older than 7 days breach SLA
SELECT dlq_id, source_topic, failed_at, retry_count,
       NOW() - failed_at AS age,
       'UNRESOLVED DLQ EVENT OLDER THAN 7 DAYS' AS failure_reason
FROM raw.dlq_events
WHERE resolved = FALSE
  AND failed_at < NOW() - INTERVAL '7 days';


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 14 — REFERENTIAL COVERAGE (ORPHAN DETECTION)
-- Detects records that are technically valid but represent gaps in expected
-- coverage — things that should always have a corresponding record but don't.
-- ─────────────────────────────────────────────────────────────────────────────

-- 14-T01: Claims in approved/paid status with no vitals (for consultation codes)
-- Already covered in section 09 from a clinical angle; repeated here from
-- a referential completeness angle as a hard governance check.
SELECT c.claim_id, c.procedure_code, c.status,
       'APPROVED CONSULTATION CLAIM WITH NO VITALS' AS failure_reason
FROM raw.claims c
LEFT JOIN raw.vitals v ON v.claim_id = c.claim_id
WHERE c.procedure_code LIKE 'CONS%'
  AND c.status IN ('approved', 'paid')
  AND v.vitals_id IS NULL;

-- 14-T02: Members with employer_id pointing to an inactive employer
-- Coverage may not be valid if the employer's contract has lapsed.
SELECT m.member_id, m.employer_id, e.is_active, e.contract_end,
       'MEMBER LINKED TO INACTIVE EMPLOYER' AS failure_reason
FROM raw.members m
JOIN raw.employers e ON e.employer_id = m.employer_id
WHERE m.is_active = TRUE
  AND e.is_active = FALSE;

-- 14-T03: Providers with no claims in the last 90 days
-- Contracted providers with zero activity may indicate a data pipeline gap
-- or a provider that should be marked inactive.
SELECT p.provider_id, p.provider_name, p.accreditation_status,
       MAX(c.date_of_service) AS last_claim_date,
       'ACTIVE PROVIDER WITH NO CLAIMS IN 90 DAYS' AS failure_reason
FROM raw.providers p
LEFT JOIN raw.claims c
    ON c.provider_id = p.provider_id
    AND c.date_of_service >= CURRENT_DATE - 90
WHERE p.accreditation_status = 'Active'
GROUP BY p.provider_id, p.provider_name, p.accreditation_status
HAVING MAX(c.date_of_service) IS NULL;

-- 14-T04: Drug formulary codes referenced in prescriptions but absent from formulary
-- Already caught by FK test 04-T06; included here as a coverage summary.
SELECT DISTINCT p.drug_code,
       COUNT(*) AS prescription_count,
       'drug_code IN PRESCRIPTIONS NOT IN drug_formulary' AS failure_reason
FROM raw.prescriptions p
LEFT JOIN raw.drug_formulary df ON df.drug_code = p.drug_code
WHERE df.drug_code IS NULL
GROUP BY p.drug_code;

-- 14-T05: Claims with high fraud probability (> 0.80) but no investigation opened
-- High-confidence ML flags that were never acted on represent a governance failure.
SELECT fp.claim_id, fp.fraud_probability, fp.risk_tier, fp.predicted_at,
       'HIGH FRAUD PROBABILITY CLAIM WITH NO INVESTIGATION' AS failure_reason
FROM raw.fraud_predictions fp
LEFT JOIN raw.fraud_investigations fi ON fi.claim_id = fp.claim_id
WHERE fp.fraud_probability > 0.80
  AND fi.investigation_id IS NULL
  AND fp.predicted_at < NOW() - INTERVAL '7 days';  -- allow 7 days to act


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 15 — STATISTICAL ANOMALIES
-- Volume and rate checks that detect sudden changes in data patterns.
-- These tests fire on distributional problems that no row-level check can catch.
-- ─────────────────────────────────────────────────────────────────────────────

-- 15-T01: Daily claim volume drop > 50% vs 7-day average
-- A sudden volume drop indicates a Kafka Connect or producer outage.
WITH daily_volumes AS (
    SELECT
        date_submitted,
        COUNT(*) AS daily_count
    FROM raw.claims
    WHERE date_submitted >= CURRENT_DATE - 14
    GROUP BY date_submitted
),
stats AS (
    SELECT
        AVG(daily_count) FILTER (WHERE date_submitted < CURRENT_DATE) AS avg_7d,
        MAX(daily_count) FILTER (WHERE date_submitted = CURRENT_DATE)  AS today_count
    FROM daily_volumes
)
SELECT
    today_count,
    ROUND(avg_7d, 0) AS avg_7d_volume,
    ROUND(100.0 * (avg_7d - today_count) / NULLIF(avg_7d, 0), 1) AS pct_drop,
    'DAILY CLAIM VOLUME > 50% BELOW 7-DAY AVERAGE' AS failure_reason
FROM stats
WHERE today_count < avg_7d * 0.50
  AND avg_7d > 0;

-- 15-T02: Provider claim frequency outlier — > 3 standard deviations above mean
-- A single provider generating 3σ+ more claims than peers in 30 days is a
-- phantom-billing signal at the population level.
WITH provider_volumes AS (
    SELECT
        provider_id,
        COUNT(*) AS claim_count_30d
    FROM raw.claims
    WHERE date_of_service >= CURRENT_DATE - 30
    GROUP BY provider_id
),
stats AS (
    SELECT
        AVG(claim_count_30d)    AS mean_count,
        STDDEV(claim_count_30d) AS std_count
    FROM provider_volumes
)
SELECT
    pv.provider_id,
    pv.claim_count_30d,
    ROUND(stats.mean_count, 1)                                      AS mean_30d,
    ROUND(stats.std_count, 1)                                       AS stddev_30d,
    ROUND((pv.claim_count_30d - stats.mean_count)
          / NULLIF(stats.std_count, 0), 2)                          AS z_score,
    'PROVIDER CLAIM VOLUME > 3 STD DEV ABOVE MEAN (outlier)' AS failure_reason
FROM provider_volumes pv
CROSS JOIN stats
WHERE stats.std_count > 0
  AND (pv.claim_count_30d - stats.mean_count) / NULLIF(stats.std_count, 0) > 3.0;

-- 15-T03: Member visiting > 4 distinct providers within any 7-day window
-- Benefit-sharing fraud signal: same member at 4+ providers in 7 days.
WITH member_windows AS (
    SELECT
        c1.member_id,
        c1.claim_id,
        c1.date_of_service,
        COUNT(DISTINCT c2.provider_id) AS providers_in_window
    FROM raw.claims c1
    JOIN raw.claims c2
        ON  c2.member_id = c1.member_id
        AND c2.date_of_service BETWEEN c1.date_of_service AND c1.date_of_service + 6
    GROUP BY c1.member_id, c1.claim_id, c1.date_of_service
)
SELECT DISTINCT member_id, MAX(providers_in_window) AS max_providers_in_7d,
       'MEMBER VISITED > 4 PROVIDERS WITHIN 7 DAYS (benefit sharing signal)' AS failure_reason
FROM member_windows
WHERE providers_in_window > 4
GROUP BY member_id;

-- 15-T04: Potential duplicate claims — same (member, provider, diagnosis) within 48 hours
-- Duplicate claim fraud signal: resubmission of the same encounter.
SELECT
    c1.claim_id AS original_claim_id,
    c2.claim_id AS potential_duplicate_id,
    c1.member_id,
    c1.provider_id,
    c1.diagnosis_code,
    c1.date_of_service AS original_date,
    c2.date_of_service AS duplicate_date,
    ABS(c1.amount_claimed - c2.amount_claimed) / NULLIF(c1.amount_claimed, 0) AS amount_variance_pct,
    'POTENTIAL DUPLICATE CLAIM (same member/provider/diagnosis within 48h)' AS failure_reason
FROM raw.claims c1
JOIN raw.claims c2
    ON  c2.member_id       = c1.member_id
    AND c2.provider_id     = c1.provider_id
    AND c2.diagnosis_code  = c1.diagnosis_code
    AND c2.claim_id        != c1.claim_id
    AND ABS(c2.date_of_service - c1.date_of_service) <= 2
    AND ABS(c1.amount_claimed - c2.amount_claimed) / NULLIF(c1.amount_claimed, 0) < 0.05
WHERE c1.claim_id < c2.claim_id;  -- prevent symmetric duplicates

-- 15-T05: Fraud rate spike — daily confirmed fraud rate > 15%
-- A sudden spike above 15% suggests model mislabelling or data poisoning.
WITH daily_outcomes AS (
    SELECT
        o.confirmed_at::date AS outcome_date,
        COUNT(*)                                                    AS total_outcomes,
        SUM(CASE WHEN o.final_label = 'confirmed_fraud' THEN 1 ELSE 0 END) AS fraud_count
    FROM raw.fraud_investigation_outcomes o
    WHERE o.confirmed_at >= NOW() - INTERVAL '30 days'
    GROUP BY o.confirmed_at::date
)
SELECT
    outcome_date,
    total_outcomes,
    fraud_count,
    ROUND(100.0 * fraud_count / NULLIF(total_outcomes, 0), 2) AS fraud_rate_pct,
    'DAILY FRAUD INVESTIGATION RATE > 15%' AS failure_reason
FROM daily_outcomes
WHERE total_outcomes >= 5   -- exclude low-volume days
  AND (fraud_count::float / NULLIF(total_outcomes, 0)) > 0.15;

-- 15-T06: Stock level implausible week-over-week swing (> 300% increase)
-- A sudden 3× jump in reported stock without a corresponding delivery record
-- indicates data entry error or formulary gaming.
WITH weekly_stock AS (
    SELECT
        facility_id,
        drug_code,
        reported_date,
        quantity_on_hand,
        LAG(quantity_on_hand) OVER (
            PARTITION BY facility_id, drug_code
            ORDER BY reported_date
        ) AS prev_quantity
    FROM raw.stock_levels
)
SELECT facility_id, drug_code, reported_date,
       prev_quantity, quantity_on_hand,
       ROUND(100.0 * (quantity_on_hand - prev_quantity) / NULLIF(prev_quantity, 0), 1) AS pct_change,
       'STOCK LEVEL INCREASED > 300% WEEK-OVER-WEEK (anomalous)' AS failure_reason
FROM weekly_stock
WHERE prev_quantity > 0
  AND quantity_on_hand > prev_quantity * 4.0;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 16 — SECURITY AND GOVERNANCE
-- Verifies role-based access control, sensitive field handling, and audit
-- trail completeness. A failed security test represents a data governance
-- failure, not just a data quality failure.
-- ─────────────────────────────────────────────────────────────────────────────

-- 16-T01: kafka_connect must NOT have SELECT on marts tables
-- Kafka Connect should only write to raw. Read access to marts would allow
-- exfiltration of aggregated fraud intelligence.
SELECT grantee, table_schema, table_name, privilege_type,
       'SECURITY: kafka_connect HAS SELECT ON MARTS TABLE' AS failure_reason
FROM information_schema.role_table_grants
WHERE grantee      = 'kafka_connect'
  AND privilege_type = 'SELECT'
  AND table_schema = 'marts';

-- 16-T02: api_reader must NOT have any access to raw schema tables
-- FastAPI reads exclusively from marts. Direct access to raw would expose
-- unmasked PII and un-adjudicated fraud labels.
SELECT grantee, table_schema, table_name, privilege_type,
       'SECURITY: api_reader HAS PRIVILEGE ON RAW TABLE' AS failure_reason
FROM information_schema.role_table_grants
WHERE grantee      = 'api_reader'
  AND table_schema = 'raw';

-- 16-T03: api_reader must NOT have INSERT/UPDATE/DELETE on any table
-- The reporting API is strictly read-only. Write access indicates privilege creep.
SELECT grantee, table_schema, table_name, privilege_type,
       'SECURITY: api_reader HAS WRITE PRIVILEGE' AS failure_reason
FROM information_schema.role_table_grants
WHERE grantee        = 'api_reader'
  AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');

-- 16-T04: dbt_runner must NOT have INSERT/UPDATE/DELETE on raw tables
-- dbt reads raw, it must never write back to it.
SELECT grantee, table_schema, table_name, privilege_type,
       'SECURITY: dbt_runner HAS WRITE PRIVILEGE ON RAW' AS failure_reason
FROM information_schema.role_table_grants
WHERE grantee        = 'dbt_runner'
  AND table_schema   = 'raw'
  AND privilege_type IN ('INSERT', 'UPDATE', 'DELETE', 'TRUNCATE');

-- 16-T05: No sensitive PII columns should contain obviously invalid test data
-- Detect placeholder values (e.g. 'TEST', '0000000000') that indicate
-- non-production data was accidentally loaded into production.
SELECT member_id, id_number, 'SUSPICIOUS id_number VALUE (test data pattern)' AS failure_reason
FROM raw.members
WHERE id_number IN ('0000000000', '1111111111', '9999999999', 'TEST', 'DUMMY', 'N/A')
   OR id_number ~ '^(.)\1+$';   -- all same character repeated (e.g. '0000000000')

-- 16-T06: Every confirmed fraud outcome must have an audit trail in claim_events
-- Regulatory requirement: the fraud flag event must appear in the event log.
SELECT o.outcome_id, o.claim_id, o.final_label,
       'CONFIRMED FRAUD HAS NO flagged EVENT IN claim_events' AS failure_reason
FROM raw.fraud_investigation_outcomes o
LEFT JOIN raw.claim_events e
    ON e.claim_id  = o.claim_id
    AND e.event_type = 'flagged'
WHERE o.final_label = 'confirmed_fraud'
  AND e.event_id IS NULL;

-- 16-T07: Debezium publication must exist and cover fraud_investigation_outcomes
-- Without this publication the ML retraining feedback loop is silently broken.
SELECT
    CASE
        WHEN COUNT(*) = 0 THEN 'afyabima_debezium'
    END AS missing_publication,
    'CDC PUBLICATION afyabima_debezium DOES NOT EXIST' AS failure_reason
FROM pg_publication
WHERE pubname = 'afyabima_debezium'
HAVING COUNT(*) = 0;

-- 16-T08: Debezium publication must include fraud_investigation_outcomes
SELECT
    CASE WHEN COUNT(*) = 0 THEN 'fraud_investigation_outcomes' END AS missing_table,
    'fraud_investigation_outcomes NOT IN afyabima_debezium PUBLICATION' AS failure_reason
FROM pg_publication_tables
WHERE pubname    = 'afyabima_debezium'
  AND schemaname = 'raw'
  AND tablename  = 'fraud_investigation_outcomes'
HAVING COUNT(*) = 0;


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 17 — INDEX EFFECTIVENESS
-- Verifies that indexes have not been dropped and that the most critical
-- query paths are covered. Missing indexes on FK columns cause full table scans
-- that degrade linearly with data volume.
-- ─────────────────────────────────────────────────────────────────────────────

-- 17-T01: Critical single-column indexes on high-cardinality FK columns
SELECT expected.index_name, expected.table_name, expected.column_name,
       'MISSING CRITICAL INDEX' AS failure_reason
FROM (
    VALUES
        ('idx_claims_member_id',              'claims',                   'member_id'),
        ('idx_claims_provider_id',            'claims',                   'provider_id'),
        ('idx_claims_diagnosis_code',         'claims',                   'diagnosis_code'),
        ('idx_claims_date_of_service',        'claims',                   'date_of_service'),
        ('idx_claims_status',                 'claims',                   'status'),
        ('idx_claims_submitted_at',           'claims',                   'submitted_at'),
        ('idx_members_plan_code',             'members',                  'plan_code'),
        ('idx_members_employer_id',           'members',                  'employer_id'),
        ('idx_members_is_active',             'members',                  'is_active'),
        ('idx_vitals_claim_id',               'vitals',                   'claim_id'),
        ('idx_vitals_member_id',              'vitals',                   'member_id'),
        ('idx_prescriptions_claim_id',        'prescriptions',            'claim_id'),
        ('idx_prescriptions_member_id',       'prescriptions',            'member_id'),
        ('idx_prescriptions_drug_code',       'prescriptions',            'drug_code'),
        ('idx_payments_claim_id',             'payments',                 'claim_id'),
        ('idx_fraud_predictions_claim_id',    'fraud_predictions',        'claim_id'),
        ('idx_fraud_predictions_risk_tier',   'fraud_predictions',        'risk_tier'),
        ('idx_fraud_investigations_claim_id', 'fraud_investigations',     'claim_id'),
        ('idx_fraud_investigations_status',   'fraud_investigations',     'status'),
        ('idx_outcomes_claim_id',             'fraud_investigation_outcomes', 'claim_id'),
        ('idx_outcomes_investigation_id',     'fraud_investigation_outcomes', 'investigation_id'),
        ('idx_outcomes_final_label',          'fraud_investigation_outcomes', 'final_label'),
        ('idx_stock_levels_facility_id',      'stock_levels',             'facility_id'),
        ('idx_stock_levels_drug_code',        'stock_levels',             'drug_code')
) AS expected(index_name, table_name, column_name)
LEFT JOIN pg_indexes idx
    ON  idx.schemaname = 'raw'
    AND idx.indexname  = expected.index_name
WHERE idx.indexname IS NULL;

-- 17-T02: Composite duplicate-detection index must exist
-- This specific composite index supports the most expensive fraud feature query.
SELECT 'idx_claims_dup_detection' AS index_name,
       'MISSING COMPOSITE INDEX FOR DUPLICATE DETECTION' AS failure_reason
WHERE NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'raw'
      AND indexname  = 'idx_claims_dup_detection'
);

-- 17-T03: BRIN indexes must exist on append-only timestamp columns
-- BRIN indexes are physically smaller than B-tree indexes for monotonically
-- increasing data; their absence wastes significant storage.
SELECT expected.index_name,
       'MISSING BRIN INDEX ON APPEND-ONLY COLUMN' AS failure_reason
FROM (
    VALUES
        ('idx_claim_events_event_at'),
        ('idx_payments_paid_at'),
        ('idx_fraud_predictions_predicted_at'),
        ('idx_outcomes_confirmed_at'),
        ('idx_stock_levels_reported_date'),
        ('idx_dlq_failed_at')
) AS expected(index_name)
LEFT JOIN pg_indexes idx ON idx.indexname = expected.index_name
WHERE idx.indexname IS NULL;

-- 17-T04: GIN index on feature_snapshot JSONB must exist
-- Without the GIN index, filtering on feature keys in explainability queries
-- triggers full-table JSON parsing.
SELECT 'idx_fraud_predictions_features_gin' AS index_name,
       'MISSING GIN INDEX ON feature_snapshot JSONB' AS failure_reason
WHERE NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'raw'
      AND indexname  = 'idx_fraud_predictions_features_gin'
      AND indexdef   LIKE '%gin%'
);

-- 17-T05: Partial index on dlq_events (unresolved only) must exist
-- This partial index ensures Airflow's DLQ monitor query runs in microseconds
-- regardless of how many resolved events have accumulated.
SELECT 'idx_dlq_resolved' AS index_name,
       'MISSING PARTIAL INDEX ON dlq_events (resolved = FALSE)' AS failure_reason
WHERE NOT EXISTS (
    SELECT 1 FROM pg_indexes
    WHERE schemaname = 'raw'
      AND indexname  = 'idx_dlq_resolved'
);


-- ─────────────────────────────────────────────────────────────────────────────
-- SECTION 18 — CDC / DEBEZIUM INTEGRITY
-- The Debezium publication on fraud_investigation_outcomes is the mechanism
-- that closes the ML retraining feedback loop. Any failure here is silent —
-- the pipeline looks healthy but the model stops improving.
-- ─────────────────────────────────────────────────────────────────────────────

-- 18-T01: Logical replication slot for Debezium must exist and be active
SELECT slot_name, plugin, active,
       'DEBEZIUM REPLICATION SLOT NOT ACTIVE' AS failure_reason
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%'
  AND active = FALSE
UNION ALL
SELECT 'debezium_slot_missing' AS slot_name, NULL AS plugin, NULL AS active,
       'DEBEZIUM REPLICATION SLOT DOES NOT EXIST' AS failure_reason
WHERE NOT EXISTS (
    SELECT 1 FROM pg_replication_slots WHERE slot_name LIKE 'debezium%'
);

-- 18-T02: WAL level must be 'logical' for Debezium to function
-- If wal_level is not 'logical', Debezium silently produces no events.
SELECT name, setting,
       'wal_level IS NOT logical — Debezium CDC WILL NOT WORK' AS failure_reason
FROM pg_settings
WHERE name    = 'wal_level'
  AND setting != 'logical';

-- 18-T03: Replication lag must be below threshold (< 100 MB)
-- Excessive WAL lag means Debezium is falling behind and the feedback loop
-- is delayed, potentially causing the model to train on stale labels.
SELECT
    slot_name,
    pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) AS lag_bytes,
    pg_size_pretty(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn)) AS lag_pretty,
    'DEBEZIUM REPLICATION LAG > 100 MB' AS failure_reason
FROM pg_replication_slots
WHERE slot_name LIKE 'debezium%'
  AND pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn) > 100 * 1024 * 1024;

-- 18-T04: fraud_investigation_outcomes must not go more than 24h without a new row
-- During active operations, investigators close cases daily. A 24-hour silence
-- means either no cases were resolved (operationally suspicious) or the
-- INSERT path to this table is broken (technically critical).
SELECT
    MAX(confirmed_at)                                           AS last_outcome,
    NOW() - MAX(confirmed_at)                                  AS silence_duration,
    'NO NEW fraud_investigation_outcomes IN LAST 24 HOURS'     AS failure_reason
FROM raw.fraud_investigation_outcomes
HAVING MAX(confirmed_at) < NOW() - INTERVAL '24 hours'
    OR MAX(confirmed_at) IS NULL;

-- 18-T05: The afyabima_debezium publication must replicate INSERT events only
-- We do not want UPDATE or DELETE events on the outcomes table to trigger
-- retraining — only new verdicts should feed the feedback loop.
SELECT pubname, pubinsert, pubupdate, pubdelete,
       'DEBEZIUM PUBLICATION SCOPE INCORRECT (should be INSERT only)' AS failure_reason
FROM pg_publication
WHERE pubname    = 'afyabima_debezium'
  AND (pubupdate = TRUE OR pubdelete = TRUE);


-- =============================================================================
-- END OF TEST SUITE
-- =============================================================================
--
-- SUMMARY OF COVERAGE
-- ─────────────────────────────────────────────────────────────────────────────
-- Section  Tests  What it covers
-- ───────  ─────  ─────────────────────────────────────────────────────────────
--  01       3     Table/column/index existence and correct data types
--  02       7     Primary key uniqueness and composite key uniqueness
--  03       4     Not-null completeness on all mandatory columns
--  04      13     Foreign key orphan detection (including denorm consistency)
--  05      17     Enum and CHECK constraint re-validation
--  06      13     Cross-column business rules within single rows
--  07      14     Cross-table business rules spanning multiple tables
--  08      11     Date and timestamp plausibility and ordering
--  09       7     Clinical physiological plausibility
--  10       7     Fraud pipeline logic, ML audit, model promotion rules
--  11       6     Financial amounts, payment integrity, threshold bands
--  12       7     Event log sequence, completeness, and idempotency
--  13       7     Pipeline freshness / _loaded_at staleness
--  14       5     Referential coverage gaps (expected records that are absent)
--  15       6     Statistical anomaly detection (volume, rates, outliers)
--  16       8     RBAC enforcement, PII hygiene, CDC audit trail
--  17       5     Index existence (B-tree, BRIN, GIN, composite, partial)
--  18       5     Debezium / WAL replication health
-- ───────  ─────  ─────────────────────────────────────────────────────────────
-- TOTAL   145     Tests across 17 raw tables and the full fraud pipeline
-- =============================================================================
