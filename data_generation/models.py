"""
Dataclass definitions matching the AfyaBima PostgreSQL schema exactly.

Rules enforced here:
- No fraud label fields on ClaimSubmitted (they don't belong in Kafka).
- All date fields stored as ISO-format strings so asdict() serialises cleanly
  to CSV/JSON without custom converters.
- Optional fields use Optional[str] / Optional[float] rather than bare None.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass
class Member:
    """raw.members — one row per insured beneficiary."""

    member_id: str
    first_name: str
    last_name: str
    date_of_birth: str  # ISO date string, e.g. "1985-04-12"
    gender: str  # "M" or "F"
    id_type: str  # "National ID" | "Passport" | "Alien ID" | "Military ID"
    id_number: str
    phone_number: str
    county: str
    sub_county: str
    enrol_date: str  # ISO date
    term_date: str | None  # ISO date; None if still active
    plan_code: str  # FK → raw.plans
    employer_id: str | None  # FK → raw.employers; None if individual member
    is_active: bool


@dataclass
class Provider:
    """raw.providers — one row per contracted healthcare facility."""

    provider_id: str
    provider_name: str
    facility_type: str
    county: str
    sub_county: str
    license_number: str
    accreditation_status: str  # "Active" | "Probation" | "Suspended" | "Terminated"
    panel_capacity: int
    contract_start_date: str  # ISO date
    contract_end_date: str | None  # ISO date; None if contract is open
    risk_score: float  # 0.0–1.0, derived from claims history


@dataclass
class ClaimSubmitted:
    """
    raw.claims — one row per claim, upserted by Kafka Connect on claim_id.

    Deliberately contains NO fraud label fields.  Ground truth is recorded only
    in raw.fraud_investigation_outcomes weeks or months after the claim date.
    """

    claim_id: str
    member_id: str  # FK → raw.members
    provider_id: str  # FK → raw.providers
    date_of_service: str  # ISO date
    date_submitted: str  # ISO date
    procedure_code: str
    diagnosis_code: str  # FK → raw.icd10_codes
    quantity: int
    amount_claimed: float  # KES — renamed from billed_amount
    amount_approved: float | None  # KES; None until adjudicated
    status: str  # submitted | pending_review | approved | …
    submission_channel: str  # portal | mobile | api | paper
    is_emergency: bool
    prior_auth_number: str | None
    submitted_at: str  # ISO datetime
    updated_at: str  # ISO datetime


@dataclass
class Vitals:
    """
    raw.vitals — clinical measurements captured at the point of service.

    Absence of a vitals record for a consultation claim is the primary
    phantom-billing signal for the ML model.
    """

    vitals_id: str
    claim_id: str  # FK → raw.claims
    member_id: str  # FK → raw.members
    date_taken: str  # ISO date
    systolic_bp: int | None
    diastolic_bp: int | None
    heart_rate: int | None
    temperature_c: float | None  # renamed from temperature
    weight_kg: float | None  # renamed from weight
    height_cm: float | None  # renamed from height
    bmi: float | None
    spo2_pct: int | None  # blood oxygen saturation %
    notes: str | None


@dataclass
class Prescription:
    """
    raw.prescriptions — one row per drug prescribed per claim.

    Absence of a prescription for diagnoses that require medication is a
    clinically anomalous signal used in the ML feature set.
    drug_code is a FK → raw.drug_formulary, ensuring every prescribed drug
    exists in the approved formulary.
    """

    prescription_id: str
    claim_id: str  # FK → raw.claims
    member_id: str  # FK → raw.members
    drug_code: str  # FK → raw.drug_formulary (replaces medication_code/name)
    dosage: str  # e.g. "500mg"
    frequency: str  # e.g. "2× daily"  (renamed from dosage schedule)
    duration_days: int  # renamed from days_supply
    quantity_dispensed: int | None  # renamed from quantity; None if not yet dispensed
    dispensed_on_site: bool  # renamed from is_dispensed
    prescribed_at: str  # ISO datetime (renamed from prescribed_date)


@dataclass
class InvestigationOutcome:
    """
    raw.fraud_investigation_outcomes — final investigator verdict.

    Debezium watches this table and publishes every INSERT to the
    investigations.closed Kafka topic, closing the retraining feedback loop.

    Note: this dataclass represents the *outcome* row only.
    The parent raw.fraud_investigations row is represented by FraudInvestigation.
    """

    outcome_id: str  # PK (generated)
    claim_id: str  # FK → raw.claims
    investigation_id: str  # FK → raw.fraud_investigations (1:1)
    final_label: str  # confirmed_fraud | legitimate | inconclusive
    fraud_type: str | None  # phantom_billing | threshold_manip | benefit_sharing | duplicate_claim
    confirmed_by: str  # investigator user ID
    confirmed_at: str  # ISO datetime
    estimated_loss_kes: float | None  # KES; None unless confirmed_fraud
    amount_recovered_kes: float | None  # KES; None unless some recovery occurred
    notes: str | None


@dataclass
class FraudInvestigation:
    """
    raw.fraud_investigations — investigation lifecycle row.

    One row per investigation, updated as status changes.
    Created when a claim is flagged; closed when outcome is recorded.
    """

    investigation_id: str  # PK
    claim_id: str  # FK → raw.claims
    prediction_id: str | None  # FK → raw.fraud_predictions; None for manual flags
    assigned_to: str  # investigator user ID
    opened_at: str  # ISO datetime
    closed_at: str | None  # ISO datetime; None while still open
    status: str  # open | in_progress | pending_review | closed | escalated
    priority: str  # low | medium | high | critical
    notes: str | None


@dataclass
class Employer:
    """raw.employers — corporate clients sponsoring member coverage."""

    employer_id: str
    employer_name: str
    industry: str
    county: str
    member_count: int
    contract_start: str  # ISO date
    contract_end: str | None  # ISO date; None if contract is open
    is_active: bool


@dataclass
class Plan:
    """raw.plans — financial parameters per insurance product."""

    plan_code: str
    plan_name: str
    plan_tier: str  # basic | standard | premium | family | corporate
    annual_limit_kes: float
    inpatient_limit_kes: float
    outpatient_limit_kes: float
    premium_monthly_kes: float
    is_family_plan: bool


@dataclass
class DrugFormulary:
    """raw.drug_formulary — SHA-approved drugs."""

    drug_code: str
    drug_name: str
    drug_class: str
    is_essential_medicine: bool
    is_controlled_substance: bool
    unit_cost_kes: float | None


@dataclass
class ClaimEvent:
    """
    raw.claim_events — immutable event log, one row per claim state transition.
    Never updated after insert.
    """

    event_id: str
    claim_id: str  # FK → raw.claims
    event_type: str  # submitted | status_changed | approved | rejected | …
    previous_status: str | None
    new_status: str
    event_at: str  # ISO datetime
    triggered_by: str  # system | auto_approve | investigator | api | airflow
    metadata: str | None  # JSON string (serialised as text for CSV)


@dataclass
class Payment:
    """raw.payments — one row per payment disbursement."""

    payment_id: str
    claim_id: str  # FK → raw.claims
    amount_paid_kes: float
    payment_method: str  # EFT | MPESA | Cheque | Cash
    reference_number: str | None
    paid_at: str  # ISO datetime


@dataclass
class StockLevel:
    """raw.stock_levels — daily pharmacy stock snapshot per facility per drug."""

    stock_id: str
    facility_id: str  # FK → raw.providers
    drug_code: str  # FK → raw.drug_formulary
    quantity_on_hand: int
    reorder_level: int
    reported_date: str  # ISO date
