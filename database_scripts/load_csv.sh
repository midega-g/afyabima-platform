#!/usr/bin/env bash
# =============================================================================
# AfyaBima — Scenario A: Initial Bulk Load from CSVs
# =============================================================================
#
# WHEN TO USE THIS
#   The platform has NOT been connected to Kafka yet.
#   You want to seed the database from the CSV files the Python generator wrote.
#
# USAGE
#   ./load_csv.sh [--data-dir PATH] [--host HOST] [--db DB] [--user USER]
#
# DEFAULTS
#   --data-dir  ./afyabima_data       (where the generator wrote its CSVs)
#   --host      localhost
#   --db        afyabima
#   --user      kafka_connect         (has INSERT on raw.*)
#
# EXAMPLES
#   ./load_csv.sh
#   ./load_csv.sh --data-dir /tmp/afyabima_data --host db.internal --db afyabima
#   PGPASSWORD=secret ./load_csv.sh --user postgres
#
# IDEMPOTENCY
#   Passing --clean truncates all raw tables in safe FK order before loading.
#   Without --clean, COPY appends (safe for a fresh DB, error on duplicates).
#
# HOW IT WORKS
#   Uses psql \copy (backslash-copy), which reads files from THIS machine
#   and streams them to the server. No need to put files on the DB host.
#   Each \copy corresponds to one table and lists only the columns present
#   in the CSV (_loaded_at is excluded — the DB fills it with DEFAULT now()).
# =============================================================================

set -euo pipefail

# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------
DATA_DIR="./afyabima_data"
DB_HOST="localhost"
DB_PORT="5432"
DB_NAME="afyabima"
DB_USER="kafka_connect"
CLEAN=false

# ---------------------------------------------------------------------------
# Parse arguments
# ---------------------------------------------------------------------------
while [[ $# -gt 0 ]]; do
    case "$1" in
        --data-dir)  DATA_DIR="$2";  shift 2 ;;
        --host)      DB_HOST="$2";   shift 2 ;;
        --port)      DB_PORT="$2";   shift 2 ;;
        --db)        DB_NAME="$2";   shift 2 ;;
        --user)      DB_USER="$2";   shift 2 ;;
        --clean)     CLEAN=true;     shift   ;;
        *) echo "Unknown option: $1"; exit 1 ;;
    esac
done

DATA_DIR="${DATA_DIR%/}"   # strip trailing slash

# ---------------------------------------------------------------------------
# Validate CSV files exist
# ---------------------------------------------------------------------------
REQUIRED_FILES=(
    "$DATA_DIR/plans.csv"
    "$DATA_DIR/employers.csv"
    "$DATA_DIR/drug_formulary.csv"
    "$DATA_DIR/providers.csv"
    "$DATA_DIR/seeds/diagnosis_code_lookup.csv"
    "$DATA_DIR/members.csv"
    "$DATA_DIR/stock_levels.csv"
    "$DATA_DIR/claims.csv"
    "$DATA_DIR/claim_events.csv"
    "$DATA_DIR/vitals.csv"
    "$DATA_DIR/prescriptions.csv"
    "$DATA_DIR/payments.csv"
    "$DATA_DIR/fraud_investigations.csv"
    "$DATA_DIR/fraud_investigation_outcomes.csv"
)

echo "=== AfyaBima Bulk CSV Loader ==="
echo "Data directory : $DATA_DIR"
echo "Target DB      : $DB_USER@$DB_HOST:$DB_PORT/$DB_NAME"
echo ""

MISSING=0
for f in "${REQUIRED_FILES[@]}"; do
    if [[ ! -f "$f" ]]; then
        echo "  MISSING: $f"
        MISSING=$((MISSING+1))
    fi
done

if [[ $MISSING -gt 0 ]]; then
    echo ""
    echo "ERROR: $MISSING required CSV file(s) missing."
    echo "Run the generator first: uv run python -m data_generation.main --output-dir $DATA_DIR"
    exit 1
fi

echo "All required CSV files found."
echo ""

# ---------------------------------------------------------------------------
# Helper: run psql with consistent connection args
# ---------------------------------------------------------------------------
psql_run() {
    psql \
        --host="$DB_HOST" \
        --port="$DB_PORT" \
        --username="$DB_USER" \
        --dbname="$DB_NAME" \
        --no-psqlrc \
        --set ON_ERROR_STOP=on \
        "$@"
}

# ---------------------------------------------------------------------------
# Optional clean: truncate in FK-safe reverse order
# ---------------------------------------------------------------------------
if [[ "$CLEAN" == true ]]; then
    echo "=== Cleaning existing data (--clean flag set) ==="
    psql_run <<'SQL'
SET client_min_messages = WARNING;
TRUNCATE raw.fraud_investigation_outcomes RESTART IDENTITY CASCADE;
TRUNCATE raw.fraud_investigations         RESTART IDENTITY CASCADE;
TRUNCATE raw.payments                     RESTART IDENTITY CASCADE;
TRUNCATE raw.prescriptions                RESTART IDENTITY CASCADE;
TRUNCATE raw.vitals                       RESTART IDENTITY CASCADE;
TRUNCATE raw.claim_events                 RESTART IDENTITY CASCADE;
TRUNCATE raw.claims                       RESTART IDENTITY CASCADE;
TRUNCATE raw.stock_levels                 RESTART IDENTITY CASCADE;
TRUNCATE raw.members                      RESTART IDENTITY CASCADE;
TRUNCATE raw.providers                    RESTART IDENTITY CASCADE;
TRUNCATE raw.icd10_codes                  RESTART IDENTITY CASCADE;
TRUNCATE raw.drug_formulary               RESTART IDENTITY CASCADE;
TRUNCATE raw.employers                    RESTART IDENTITY CASCADE;
TRUNCATE raw.plans                        RESTART IDENTITY CASCADE;
SQL
    echo "Tables truncated."
    echo ""
fi

# ---------------------------------------------------------------------------
# Load: Tier 1 — no FK dependencies
# ---------------------------------------------------------------------------
echo "=== Tier 1: Reference tables ==="

psql_run -c "\copy raw.plans (plan_code, plan_name, plan_tier, annual_limit_kes, inpatient_limit_kes, outpatient_limit_kes, premium_monthly_kes, is_family_plan) FROM '$DATA_DIR/plans.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ plans"

psql_run -c "\copy raw.employers (employer_id, employer_name, industry, county, member_count, contract_start, contract_end, is_active) FROM '$DATA_DIR/employers.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ employers"

psql_run -c "\copy raw.icd10_codes (icd10_code, description, clinical_category) FROM '$DATA_DIR/seeds/diagnosis_code_lookup.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ icd10_codes (from seeds/diagnosis_code_lookup.csv)"

psql_run -c "\copy raw.drug_formulary (drug_code, drug_name, drug_class, is_essential_medicine, is_controlled_substance, unit_cost_kes) FROM '$DATA_DIR/drug_formulary.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ drug_formulary"

psql_run -c "\copy raw.providers (provider_id, provider_name, facility_type, county, sub_county, license_number, accreditation_status, panel_capacity, contract_start_date, contract_end_date, risk_score) FROM '$DATA_DIR/providers.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ providers"

# ---------------------------------------------------------------------------
# Load: Tier 2 — depends on Tier 1
# ---------------------------------------------------------------------------
echo ""
echo "=== Tier 2: Depends on reference tables ==="

psql_run -c "\copy raw.members (member_id, first_name, last_name, date_of_birth, gender, id_type, id_number, phone_number, county, sub_county, enrol_date, term_date, plan_code, employer_id, is_active) FROM '$DATA_DIR/members.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ members"

psql_run -c "\copy raw.stock_levels (stock_id, facility_id, drug_code, quantity_on_hand, reorder_level, reported_date) FROM '$DATA_DIR/stock_levels.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ stock_levels"

# ---------------------------------------------------------------------------
# Load: Tier 3 — depends on Tier 2
# ---------------------------------------------------------------------------
echo ""
echo "=== Tier 3: Claims ==="

psql_run -c "\copy raw.claims (claim_id, member_id, provider_id, date_of_service, date_submitted, procedure_code, diagnosis_code, quantity, amount_claimed, amount_approved, status, submission_channel, is_emergency, prior_auth_number, submitted_at, updated_at) FROM '$DATA_DIR/claims.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ claims"

# ---------------------------------------------------------------------------
# Load: Tier 4 — depends on claims
# ---------------------------------------------------------------------------
echo ""
echo "=== Tier 4: Claim children & fraud lifecycle ==="

psql_run -c "\copy raw.claim_events (event_id, claim_id, event_type, previous_status, new_status, event_at, triggered_by, metadata) FROM '$DATA_DIR/claim_events.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ claim_events"

psql_run -c "\copy raw.vitals (vitals_id, claim_id, member_id, date_taken, systolic_bp, diastolic_bp, heart_rate, temperature_c, weight_kg, height_cm, bmi, spo2_pct, notes) FROM '$DATA_DIR/vitals.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ vitals"

psql_run -c "\copy raw.prescriptions (prescription_id, claim_id, member_id, drug_code, dosage, frequency, duration_days, quantity_dispensed, dispensed_on_site, prescribed_at) FROM '$DATA_DIR/prescriptions.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ prescriptions"

psql_run -c "\copy raw.payments (payment_id, claim_id, amount_paid_kes, payment_method, reference_number, paid_at) FROM '$DATA_DIR/payments.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ payments"

psql_run -c "\copy raw.fraud_investigations (investigation_id, claim_id, prediction_id, assigned_to, opened_at, closed_at, status, priority, notes) FROM '$DATA_DIR/fraud_investigations.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ fraud_investigations"

# ---------------------------------------------------------------------------
# Load: Tier 5 — depends on fraud_investigations
# ---------------------------------------------------------------------------
echo ""
echo "=== Tier 5: Investigation outcomes ==="

psql_run -c "\copy raw.fraud_investigation_outcomes (outcome_id, claim_id, investigation_id, final_label, fraud_type, confirmed_by, confirmed_at, estimated_loss_kes, amount_recovered_kes, notes) FROM '$DATA_DIR/fraud_investigation_outcomes.csv' WITH (FORMAT csv, HEADER true, NULL '')"
echo "  ✓ fraud_investigation_outcomes"

# ---------------------------------------------------------------------------
# Row count summary
# ---------------------------------------------------------------------------
echo ""
echo "=== Row count summary ==="
psql_run <<'SQL'
SELECT table_name, row_count
FROM (
    VALUES
        ('raw.plans',                       (SELECT COUNT(*) FROM raw.plans)),
        ('raw.employers',                   (SELECT COUNT(*) FROM raw.employers)),
        ('raw.icd10_codes',                 (SELECT COUNT(*) FROM raw.icd10_codes)),
        ('raw.drug_formulary',              (SELECT COUNT(*) FROM raw.drug_formulary)),
        ('raw.providers',                   (SELECT COUNT(*) FROM raw.providers)),
        ('raw.members',                     (SELECT COUNT(*) FROM raw.members)),
        ('raw.stock_levels',                (SELECT COUNT(*) FROM raw.stock_levels)),
        ('raw.claims',                      (SELECT COUNT(*) FROM raw.claims)),
        ('raw.claim_events',                (SELECT COUNT(*) FROM raw.claim_events)),
        ('raw.vitals',                      (SELECT COUNT(*) FROM raw.vitals)),
        ('raw.prescriptions',               (SELECT COUNT(*) FROM raw.prescriptions)),
        ('raw.payments',                    (SELECT COUNT(*) FROM raw.payments)),
        ('raw.fraud_investigations',        (SELECT COUNT(*) FROM raw.fraud_investigations)),
        ('raw.fraud_investigation_outcomes',(SELECT COUNT(*) FROM raw.fraud_investigation_outcomes))
) AS t(table_name, row_count)
ORDER BY table_name;
SQL

echo ""
echo "=== Load complete. Run db_tests.sql to validate. ==="
