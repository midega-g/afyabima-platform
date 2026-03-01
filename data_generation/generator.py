"""
AfyaBimaKenyaGenerator — main orchestration class.

Responsibilities
----------------
- Initialise members, providers, and reference data.
- Delegate fraud pattern logic to FraudPatternEngine.
- Delegate temporal decisions to TemporalController.
- Generate claims, vitals, prescriptions, and investigations day by day.
- Run post-processing injections (benefit sharing, duplicates) after the
  full timeline is built.
- Return a structured DayResult / TimelineResult without exposing internal
  fraud tracking to callers.

The generator NEVER includes fraud labels in any ClaimSubmitted object.
Ground truth lives only in InvestigationOutcome and in the internal dicts
claim_fraud_status / claim_fraud_pattern which are used only within this
module.
"""

from __future__ import annotations

import datetime
import logging
import random
import uuid
from dataclasses import dataclass, field

import numpy as np
from faker import Faker

from data_generation.config import (
    COMPANY_PREFIX,
    DIAGNOSIS_CODES_KENYA,
    DIAGNOSIS_DRUG_CLASSES,
    DRUG_FORMULARY,
    EMPLOYER_COUNT,
    EMPLOYER_INDUSTRIES,
    FACILITY_PROCEDURE_PREFIXES,
    ID_TYPES,
    KENYA_COUNTIES,
    KENYA_FACILITY_TYPES,
    PLAN_LIMITS,
    PLAN_TYPES,
    PROCEDURE_CODES_KES,
    STOCK_REPORTING_FACILITY_TYPES,
    STOCK_SNAPSHOT_INTERVAL_DAYS,
    SUBMISSION_CHANNEL_WEIGHTS,
    SUBMISSION_CHANNELS,
    FraudPattern,
)
from data_generation.models import (
    ClaimEvent,
    ClaimSubmitted,
    DrugFormulary,
    Employer,
    FraudInvestigation,
    InvestigationOutcome,
    Member,
    Payment,
    Plan,
    Prescription,
    Provider,
    StockLevel,
    Vitals,
)
from data_generation.patterns import FraudPatternEngine
from data_generation.temporal import TemporalController

logger = logging.getLogger(__name__)

fake = Faker("en_KE")


# ---------------------------------------------------------------------------
# Result dataclasses (structured return types — no bare dicts)
# ---------------------------------------------------------------------------


@dataclass
class DayResult:
    """All entities generated for a single calendar day."""

    date: datetime.date
    phase: int
    claims: list[ClaimSubmitted] = field(default_factory=list)
    claim_events: list[ClaimEvent] = field(default_factory=list)
    vitals: list[Vitals] = field(default_factory=list)
    prescriptions: list[Prescription] = field(default_factory=list)
    payments: list[Payment] = field(default_factory=list)
    stock_levels: list[StockLevel] = field(default_factory=list)
    fraud_investigations: list[FraudInvestigation] = field(default_factory=list)
    investigations: list[InvestigationOutcome] = field(default_factory=list)
    fraud_count: int = 0


@dataclass
class TimelineResult:
    """All entities generated across the full 24-month timeline."""

    # Reference / master data
    employers: list[Employer] = field(default_factory=list)
    plans: list[Plan] = field(default_factory=list)
    drug_formulary: list[DrugFormulary] = field(default_factory=list)
    members: list[Member] = field(default_factory=list)
    providers: list[Provider] = field(default_factory=list)

    # Transactional event data
    claims: list[ClaimSubmitted] = field(default_factory=list)
    claim_events: list[ClaimEvent] = field(default_factory=list)
    vitals: list[Vitals] = field(default_factory=list)
    prescriptions: list[Prescription] = field(default_factory=list)
    payments: list[Payment] = field(default_factory=list)
    stock_levels: list[StockLevel] = field(default_factory=list)

    # Fraud pipeline (investigation lifecycle + outcomes)
    fraud_investigations: list[FraudInvestigation] = field(default_factory=list)
    investigations: list[InvestigationOutcome] = field(default_factory=list)


# ---------------------------------------------------------------------------
# Fraud rates by phase (proposal specification)
# ---------------------------------------------------------------------------

_FRAUD_RATE_BY_PHASE: dict[int, float] = {
    1: 0.025,
    2: 0.028,
    3: 0.030,
    4: 0.032,
}

# ---------------------------------------------------------------------------
# Main generator
# ---------------------------------------------------------------------------


class AfyaBimaKenyaGenerator:
    """
    Kenya-specific claims generator for the AfyaBima fraud detection platform.

    Parameters
    ----------
    temporal:
        A configured TemporalController.  If None, a default one is created
        (24 months ending approximately today).
    member_count:
        Number of members to initialise.  2 000 is appropriate for prototyping.
    provider_count:
        Number of providers to initialise.  200 is appropriate for prototyping.
    """

    def __init__(
        self,
        temporal: TemporalController | None = None,
        member_count: int = 2_000,
        provider_count: int = 200,
    ) -> None:
        self.temporal = temporal or TemporalController()

        # Reference data
        self.employers: dict[str, Employer] = {}
        self.plans: dict[str, Plan] = {}
        self.drug_formulary: dict[str, DrugFormulary] = {}
        self.members: dict[str, Member] = {}
        self.providers: dict[str, Provider] = {}

        # Transactional stores
        self._all_claims: dict[str, ClaimSubmitted] = {}
        self._all_claim_events: list[ClaimEvent] = []
        self._all_vitals: dict[str, Vitals] = {}
        self._all_prescriptions: dict[str, Prescription] = {}
        self._all_payments: list[Payment] = []
        self._all_stock_levels: list[StockLevel] = []

        # Fraud pipeline stores
        self._all_fraud_investigations: dict[str, FraudInvestigation] = {}
        self._all_investigations: dict[str, InvestigationOutcome] = {}

        # Fraud truth store (INTERNAL — never serialised to CSV/Kafka)
        self.claim_fraud_status: dict[str, bool] = {}
        self.claim_fraud_pattern: dict[str, str | None] = {}

        # Phase statistics
        self._claims_by_phase: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}
        self._fraud_by_phase: dict[int, int] = {1: 0, 2: 0, 3: 0, 4: 0}

        # Derived config lists
        self._diagnosis_codes = list(DIAGNOSIS_CODES_KENYA.keys())
        self._procedure_codes = list(PROCEDURE_CODES_KES.keys())
        self._plan_codes = list(PLAN_TYPES.keys())
        self._plan_weights = [v["weight"] for v in PLAN_TYPES.values()]
        self._drug_codes = list(DRUG_FORMULARY.keys())

        # Initialise reference data (order matters: employers before members)
        self._init_employers()
        self._init_plans()
        self._init_drug_formulary()
        self._init_providers(provider_count)
        self._init_members(member_count)

        # Pattern engine — must be created AFTER members and providers exist
        self._pattern_engine = FraudPatternEngine(
            all_member_ids=list(self.members.keys()),
            all_provider_ids=list(self.providers.keys()),
        )

    # ------------------------------------------------------------------
    # Initialisation
    # ------------------------------------------------------------------

    def _init_employers(self) -> None:
        """Generate employer records that members can be FK-linked to."""
        for i in range(EMPLOYER_COUNT):
            employer_id = f"EMP{i + 100:03d}"
            contract_start = self.temporal.start_date - datetime.timedelta(days=random.randint(180, 1_095))
            is_active = random.random() > 0.05  # 95% active
            contract_end = (
                None if is_active else (contract_start + datetime.timedelta(days=random.randint(365, 730)))
            )
            self.employers[employer_id] = Employer(
                employer_id=employer_id,
                employer_name=f"{fake.company()} {random.choice(COMPANY_PREFIX)}",
                industry=random.choice(EMPLOYER_INDUSTRIES),
                county=random.choice(KENYA_COUNTIES),
                member_count=random.randint(20, 500),
                contract_start=contract_start.isoformat(),
                contract_end=contract_end.isoformat() if contract_end else None,
                is_active=is_active,
            )
        logger.info("Initialised %d employers", len(self.employers))

    def _init_plans(self) -> None:
        """Build Plan records from PLAN_TYPES + PLAN_LIMITS config."""
        for code, info in PLAN_TYPES.items():
            limits = PLAN_LIMITS[code]
            self.plans[code] = Plan(
                plan_code=code,
                plan_name=info["name"],
                plan_tier=limits["tier"],
                annual_limit_kes=float(limits["annual"]),
                inpatient_limit_kes=float(limits["inpatient"]),
                outpatient_limit_kes=float(limits["outpatient"]),
                premium_monthly_kes=float(info["premium"]),
                is_family_plan=limits["family"],
            )
        logger.info("Initialised %d plans", len(self.plans))

    def _init_drug_formulary(self) -> None:
        """Build DrugFormulary records from the DRUG_FORMULARY config dict."""
        for code, info in DRUG_FORMULARY.items():
            self.drug_formulary[code] = DrugFormulary(
                drug_code=code,
                drug_name=info["name"],
                drug_class=info["class"],
                is_essential_medicine=info["essential"],
                is_controlled_substance=info["controlled"],
                unit_cost_kes=info["cost"],
            )
        logger.info("Initialised %d drug formulary entries", len(self.drug_formulary))

    def _init_members(self, count: int) -> None:
        """Populate self.members with Kenya-specific synthetic members."""
        employer_ids = list(self.employers.keys())

        for _ in range(count):
            member_id = f"MB{uuid.uuid4().hex[:8].upper()}"

            age = int(
                np.random.choice(
                    [5, 15, 25, 35, 45, 55, 65, 75],
                    p=[0.14, 0.20, 0.24, 0.16, 0.12, 0.08, 0.04, 0.02],
                )
            )
            dob = self.temporal.start_date - datetime.timedelta(days=age * 365 + random.randint(0, 364))

            enrol_days = random.randint(0, 729)
            enrol_date = self.temporal.start_date + datetime.timedelta(days=enrol_days)

            term_date: datetime.date | None = None
            is_active = True

            if random.random() < 0.15:
                term_offset = random.randint(30, 365)
                candidate_term = enrol_date + datetime.timedelta(days=term_offset)
                if candidate_term <= self.temporal.end_date:
                    term_date = candidate_term
                    is_active = False

            county = random.choice(KENYA_COUNTIES)

            # Use a real employer_id FK — 30% of members are employer-sponsored
            employer_id: str | None = None
            if random.random() < 0.30 and employer_ids:
                employer_id = random.choice(employer_ids)

            self.members[member_id] = Member(
                member_id=member_id,
                first_name=fake.first_name(),
                last_name=fake.last_name(),
                date_of_birth=dob.isoformat(),
                gender=random.choice(["M", "F"]),
                id_type=random.choice(ID_TYPES),
                id_number=fake.numerify(text="##########"),
                phone_number=fake.phone_number(),
                county=county,
                sub_county=fake.city(),
                enrol_date=enrol_date.isoformat(),
                term_date=term_date.isoformat() if term_date else None,
                plan_code=random.choices(self._plan_codes, weights=self._plan_weights)[0],
                employer_id=employer_id,
                is_active=is_active,
            )

    def _init_providers(self, count: int) -> None:
        """Populate self.providers with Kenya-specific synthetic providers."""
        facility_types = list(KENYA_FACILITY_TYPES.keys())
        facility_weights = list(KENYA_FACILITY_TYPES.values())

        for _ in range(count):
            provider_id = f"PV{uuid.uuid4().hex[:8].upper()}"
            facility_type = random.choices(facility_types, weights=facility_weights)[0]
            county = random.choice(KENYA_COUNTIES)

            # ~12 % of providers carry elevated fraud risk (phantom billers etc.)
            risk_score = random.uniform(0.60, 0.95) if random.random() < 0.12 else random.uniform(0.05, 0.30)

            suffix = random.choice(["Hospital", "Clinic", "Medical Centre", "Dispensary"])
            self.providers[provider_id] = Provider(
                provider_id=provider_id,
                provider_name=f"{fake.company()} {suffix}",
                facility_type=facility_type,
                county=county,
                sub_county=fake.city(),
                license_number=fake.numerify(text="LIC-####-####"),
                accreditation_status=random.choices(
                    ["Active", "Probation", "Suspended", "Terminated"],
                    weights=[0.85, 0.08, 0.05, 0.02],
                )[0],
                panel_capacity=random.randint(500, 5_000),
                contract_start_date=(
                    self.temporal.start_date - datetime.timedelta(days=random.randint(0, 365))
                ).isoformat(),
                contract_end_date=None,
                risk_score=round(risk_score, 3),
            )

    # ------------------------------------------------------------------
    # Procedure pool selection
    # ------------------------------------------------------------------

    def _procedure_pool(self, provider: Provider) -> list[str]:
        """
        Return the list of procedure codes appropriate for this provider's facility level.
        Level 3 health centres are excluded from major surgical procedures.
        """
        level_prefix = provider.facility_type[:7]  # e.g. "Level 1", "Level 3"

        if level_prefix in FACILITY_PROCEDURE_PREFIXES:
            allowed = FACILITY_PROCEDURE_PREFIXES[level_prefix]
            return [
                code for code in self._procedure_codes if any(code.startswith(p) for p in allowed)
            ] or self._procedure_codes  # fallback: full list if filter too aggressive
        return self._procedure_codes

    # ------------------------------------------------------------------
    # Claim generation
    # ------------------------------------------------------------------

    def _build_claim(
        self,
        member: Member,
        provider: Provider,
        service_date: datetime.date,
        fraud_pattern: FraudPattern | None,
    ) -> ClaimSubmitted:
        """
        Build a single ClaimSubmitted.  Fraud pattern controls amount only here;
        vitals/prescription suppression is handled in their respective methods.
        """
        pool = self._procedure_pool(provider)
        procedure = random.choice(pool)
        proc = PROCEDURE_CODES_KES[procedure]
        diagnosis = random.choice(self._diagnosis_codes)

        # Quantity
        if proc["category"] in ("consultation", "radiology"):
            quantity = 1
        elif proc["category"] == "lab":
            quantity = random.choices([1, 2, 3], weights=[0.70, 0.25, 0.05])[0]
        else:
            quantity = random.choices([1, 2], weights=[0.90, 0.10])[0]

        base_amount = proc["base_cost"] * quantity

        # Apply pattern-specific amount transformation
        if fraud_pattern == FraudPattern.THRESHOLD_MANIPULATION:
            amount_claimed = self._pattern_engine.apply_threshold_manipulation(base_amount)
        elif fraud_pattern == FraudPattern.BENEFIT_SHARING:
            amount_claimed = round(base_amount * random.uniform(1.10, 1.30) * 20) / 20
        else:
            amount_claimed = round(base_amount * 20) / 20

        # Submission lag: fraud submits faster (same-day skew)
        is_fraud = fraud_pattern is not None
        if is_fraud:
            days_lag = random.choices([0, 1, 2, 3], weights=[0.60, 0.25, 0.10, 0.05])[0]
        else:
            days_lag = random.choices(
                list(range(15)),
                weights=[
                    0.05,
                    0.10,
                    0.12,
                    0.10,
                    0.08,
                    0.07,
                    0.06,
                    0.05,
                    0.04,
                    0.03,
                    0.02,
                    0.02,
                    0.01,
                    0.01,
                    0.01,
                ],
            )[0]

        submit_date = service_date + datetime.timedelta(days=days_lag)
        submitted_at = f"{submit_date.isoformat()}T{random.randint(7, 17):02d}:{random.randint(0, 59):02d}:00"

        # Determine claim status and amount_approved
        if amount_claimed <= 5_000:
            # Auto-approve below lowest threshold
            status = random.choices(
                ["approved", "paid", "pending_review"],
                weights=[0.55, 0.30, 0.15],
            )[0]
        elif is_fraud:
            status = random.choices(
                ["submitted", "pending_review", "flagged_fraud", "under_investigation", "rejected"],
                weights=[0.25, 0.30, 0.20, 0.15, 0.10],
            )[0]
        else:
            status = random.choices(
                [
                    "submitted",
                    "pending_review",
                    "approved",
                    "partially_approved",
                    "rejected",
                    "paid",
                ],
                weights=[0.15, 0.20, 0.35, 0.10, 0.05, 0.15],
            )[0]

        amount_approved: float | None = None
        if status in ("approved", "paid"):
            amount_approved = amount_claimed
        elif status == "partially_approved":
            amount_approved = round(amount_claimed * random.uniform(0.50, 0.90) * 20) / 20

        prior_auth = (
            f"PA{uuid.uuid4().hex[:10].upper()}"
            if amount_claimed > 50_000 and random.random() < 0.80
            else None
        )

        # updated_at is submit time for new claims; would advance with status changes
        updated_at = submitted_at

        return ClaimSubmitted(
            claim_id=f"CLM{uuid.uuid4().hex[:12].upper()}",
            member_id=member.member_id,
            provider_id=provider.provider_id,
            date_of_service=service_date.isoformat(),
            date_submitted=submit_date.isoformat(),
            procedure_code=procedure,
            diagnosis_code=diagnosis,
            quantity=quantity,
            amount_claimed=amount_claimed,
            amount_approved=amount_approved,
            status=status,
            submission_channel=random.choices(SUBMISSION_CHANNELS, weights=SUBMISSION_CHANNEL_WEIGHTS)[0],
            is_emergency=(days_lag == 0 and random.random() < 0.30),
            prior_auth_number=prior_auth,
            submitted_at=submitted_at,
            updated_at=updated_at,
        )

    # ------------------------------------------------------------------
    # Vitals generation
    # ------------------------------------------------------------------

    def _build_vitals(
        self,
        claim: ClaimSubmitted,
        service_date: datetime.date,
        fraud_pattern: str | None,
    ) -> Vitals | None:
        """
        Generate vitals only for consultation claims.
        Phantom billing suppresses vitals based on the pattern engine's decision.
        All other fraud types produce normal vitals (that's what hides the fraud).
        """
        if not claim.procedure_code.startswith("CONS"):
            return None

        if self._pattern_engine.should_suppress_vitals(claim, fraud_pattern):
            return None

        weight = random.uniform(45.0, 100.0)
        height = random.uniform(148.0, 185.0)

        return Vitals(
            vitals_id=f"VT{uuid.uuid4().hex[:12].upper()}",
            claim_id=claim.claim_id,
            member_id=claim.member_id,
            date_taken=service_date.isoformat(),
            systolic_bp=random.randint(100, 145),
            diastolic_bp=random.randint(60, 95),
            heart_rate=random.randint(58, 105),
            temperature_c=round(random.uniform(36.0, 37.8), 1),
            weight_kg=round(weight, 1),
            height_cm=round(height, 1),
            bmi=round(weight / (height / 100) ** 2, 1),
            spo2_pct=random.randint(94, 100),
            notes=None,
        )

    # ------------------------------------------------------------------
    # Prescription generation
    # ------------------------------------------------------------------

    def _build_prescription(
        self,
        claim: ClaimSubmitted,
        service_date: datetime.date,
        fraud_pattern: str | None,
    ) -> Prescription | None:
        """
        Generate a prescription for consultation and procedure claims.
        Drug selection is diagnosis-aware: prefers drug classes that match the diagnosis.
        Phantom billing suppresses prescriptions based on the pattern engine's decision.
        drug_code is a real FK into raw.drug_formulary.
        """
        proc = PROCEDURE_CODES_KES.get(claim.procedure_code)
        category = proc["category"] if proc else ""

        if category not in ("consultation", "procedure") and random.random() > 0.30:
            return None

        if self._pattern_engine.should_suppress_prescription(claim, fraud_pattern):
            return None

        # Pick a drug that matches the diagnosis where possible
        preferred_classes = DIAGNOSIS_DRUG_CLASSES.get(claim.diagnosis_code, [])
        if preferred_classes:
            preferred_drug_codes = [
                code for code, info in DRUG_FORMULARY.items() if info["class"] in preferred_classes
            ]
            drug_code = (
                random.choice(preferred_drug_codes)
                if preferred_drug_codes
                else random.choice(self._drug_codes)
            )
        else:
            drug_code = random.choice(self._drug_codes)

        _drug_info = DRUG_FORMULARY[drug_code]
        prescribed_at = (
            f"{service_date.isoformat()}T{random.randint(7, 17):02d}:{random.randint(0, 59):02d}:00"
        )
        duration = random.randint(3, 14)
        qty_dispensed = random.randint(duration, duration * 2) if random.random() < 0.95 else None

        return Prescription(
            prescription_id=f"RX{uuid.uuid4().hex[:12].upper()}",
            claim_id=claim.claim_id,
            member_id=claim.member_id,
            drug_code=drug_code,
            dosage=random.choice(["500mg", "250mg", "100mg", "10mg", "5mg"]),
            frequency=random.choice(["1× daily", "2× daily", "3× daily", "as needed"]),
            duration_days=duration,
            quantity_dispensed=qty_dispensed,
            dispensed_on_site=random.random() < 0.85,
            prescribed_at=prescribed_at,
        )

    # ------------------------------------------------------------------
    # Investigation generation
    # ------------------------------------------------------------------

    def _build_investigation(
        self,
        claim: ClaimSubmitted,
        service_date: datetime.date,
        is_fraud: bool,
        fraud_pattern: str | None,
    ) -> tuple[FraudInvestigation, InvestigationOutcome] | None:
        """
        Probabilistically generate an investigation lifecycle row and outcome.

        Returns a (FraudInvestigation, InvestigationOutcome) pair, or None if
        the claim is not selected for investigation.

        The FraudInvestigation row models raw.fraud_investigations (lifecycle).
        The InvestigationOutcome row models raw.fraud_investigation_outcomes (verdict).
        Both are INSERT-only; Debezium watches the outcomes table.
        """
        if not self.temporal.should_investigate(service_date, is_fraud):
            return None

        lag = self.temporal.investigation_lag_days(service_date)
        if lag == 0:
            return None

        investigator_id = f"INV{random.randint(1, 50):03d}"
        investigation_id = f"INV{uuid.uuid4().hex[:10].upper()}"
        outcome_id = f"OUT{uuid.uuid4().hex[:10].upper()}"

        opened_at_date = service_date + datetime.timedelta(days=random.randint(1, 5))
        closed_at_date = service_date + datetime.timedelta(days=lag)
        opened_at = f"{opened_at_date.isoformat()}T09:00:00"
        closed_at = f"{closed_at_date.isoformat()}T16:00:00"

        # Priority driven by fraud probability and amount
        priority = (
            "critical"
            if is_fraud and claim.amount_claimed > 20_000
            else "high"
            if is_fraud
            else "medium"
            if claim.amount_claimed > 10_000
            else "low"
        )

        fraud_investigation = FraudInvestigation(
            investigation_id=investigation_id,
            claim_id=claim.claim_id,
            prediction_id=None,  # populated by Flink at runtime; None in synthetic data
            assigned_to=investigator_id,
            opened_at=opened_at,
            closed_at=closed_at,
            status="closed",
            priority=priority,
            notes=None,
        )

        # Investigator verdict
        if is_fraud:
            confirmed = random.random() < 0.85  # 85% detection rate
        else:
            confirmed = random.random() < 0.08  # 8% false-positive rate

        final_label = (
            "confirmed_fraud" if confirmed and is_fraud else "legitimate" if not confirmed else "inconclusive"
        )
        fraud_type = fraud_pattern if final_label == "confirmed_fraud" else None

        estimated_loss: float | None = None
        amount_recovered: float | None = None
        if final_label == "confirmed_fraud":
            estimated_loss = round(claim.amount_claimed, 2)
            amount_recovered = round(claim.amount_claimed * random.uniform(0.40, 1.00), 2)

        notes_pool = [
            "Provider had no vitals on file",
            "Amount suspiciously close to approval threshold",
            "Member visited multiple providers within one week",
            "Duplicate claim detected — same member/provider/diagnosis",
            "All documentation in order; services confirmed",
            "Patient confirmed services received",
        ]

        investigation_outcome = InvestigationOutcome(
            outcome_id=outcome_id,
            claim_id=claim.claim_id,
            investigation_id=investigation_id,
            final_label=final_label,
            fraud_type=fraud_type,
            confirmed_by=investigator_id,
            confirmed_at=closed_at,
            estimated_loss_kes=estimated_loss,
            amount_recovered_kes=amount_recovered,
            notes=random.choice(notes_pool),
        )

        return fraud_investigation, investigation_outcome

    def _build_claim_events(self, claim: ClaimSubmitted) -> list[ClaimEvent]:
        """
        Generate the immutable event log for a claim's lifecycle.

        Every claim starts with a 'submitted' event.  Claims that have advanced
        beyond 'submitted' status get additional transition events so dbt can
        reconstruct full state history.
        """
        events: list[ClaimEvent] = []
        submitted_at = claim.submitted_at

        # Event 1: always present — the submission itself
        events.append(
            ClaimEvent(
                event_id=f"EVT{uuid.uuid4().hex[:12].upper()}",
                claim_id=claim.claim_id,
                event_type="submitted",
                previous_status=None,
                new_status="submitted",
                event_at=submitted_at,
                triggered_by="api",
                metadata=None,
            )
        )

        # Event 2+: status transitions for non-submitted final states
        terminal_status = claim.status
        if terminal_status == "submitted":
            return events  # no further transitions yet

        # All non-submitted claims went through pending_review first
        review_offset_hours = random.randint(2, 24)
        review_dt = datetime.datetime.fromisoformat(submitted_at) + datetime.timedelta(
            hours=review_offset_hours
        )
        events.append(
            ClaimEvent(
                event_id=f"EVT{uuid.uuid4().hex[:12].upper()}",
                claim_id=claim.claim_id,
                event_type="status_changed",
                previous_status="submitted",
                new_status="pending_review",
                event_at=review_dt.isoformat(),
                triggered_by="system",
                metadata=None,
            )
        )

        if terminal_status == "pending_review":
            return events

        # Final status event
        final_offset_hours = random.randint(24, 72)
        final_dt = review_dt + datetime.timedelta(hours=final_offset_hours)
        event_type_map = {
            "approved": "approved",
            "partially_approved": "approved",
            "rejected": "rejected",
            "paid": "paid",
            "flagged_fraud": "flagged",
            "under_investigation": "flagged",
            "closed": "closed",
        }
        triggered_by_map = {
            "approved": "auto_approve",
            "partially_approved": "auto_approve",
            "rejected": "system",
            "paid": "system",
            "flagged_fraud": "system",
            "under_investigation": "investigator",
            "closed": "airflow",
        }
        events.append(
            ClaimEvent(
                event_id=f"EVT{uuid.uuid4().hex[:12].upper()}",
                claim_id=claim.claim_id,
                event_type=event_type_map.get(terminal_status, "status_changed"),
                previous_status="pending_review",
                new_status=terminal_status,
                event_at=final_dt.isoformat(),
                triggered_by=triggered_by_map.get(terminal_status, "system"),
                metadata=None,
            )
        )

        # Paid claims get an extra 'paid' event after approval
        if terminal_status == "paid":
            paid_dt = final_dt + datetime.timedelta(hours=random.randint(1, 48))
            events.append(
                ClaimEvent(
                    event_id=f"EVT{uuid.uuid4().hex[:12].upper()}",
                    claim_id=claim.claim_id,
                    event_type="paid",
                    previous_status="approved",
                    new_status="paid",
                    event_at=paid_dt.isoformat(),
                    triggered_by="system",
                    metadata=None,
                )
            )

        return events

    def _build_payment(self, claim: ClaimSubmitted) -> Payment | None:
        """
        Generate a payment record for approved/paid claims.
        Returns None for claims that have not been paid.
        """
        if claim.status not in ("approved", "paid") or claim.amount_approved is None:
            return None

        # paid_at is a few hours to 2 days after updated_at
        updated_dt = datetime.datetime.fromisoformat(claim.updated_at)
        paid_dt = updated_dt + datetime.timedelta(hours=random.randint(1, 48))

        return Payment(
            payment_id=f"PAY{uuid.uuid4().hex[:12].upper()}",
            claim_id=claim.claim_id,
            amount_paid_kes=claim.amount_approved,
            payment_method=random.choices(
                ["EFT", "MPESA", "Cheque", "Cash"],
                weights=[0.50, 0.35, 0.10, 0.05],
            )[0],
            reference_number=f"REF{uuid.uuid4().hex[:10].upper()}",
            paid_at=paid_dt.isoformat(),
        )

    def _build_stock_snapshots(self, snapshot_date: datetime.date) -> list[StockLevel]:
        """
        Generate weekly pharmacy stock snapshots for dispensing facilities.

        Only facilities that are likely to stock and dispense drugs generate
        stock records (dispensaries, health centres, clinics).
        """
        snapshots: list[StockLevel] = []
        drug_codes = list(self.drug_formulary.keys())

        reporting_providers = [
            p
            for p in self.providers.values()
            if any(p.facility_type.startswith(ft[:7]) for ft in STOCK_REPORTING_FACILITY_TYPES)
        ]

        for provider in reporting_providers:
            # Each facility stocks a subset of the formulary
            stocked_drugs = random.sample(drug_codes, min(random.randint(8, 20), len(drug_codes)))
            for drug_code in stocked_drugs:
                reorder = random.randint(20, 100)
                # Occasionally simulate stockout (quantity = 0)
                qty = 0 if random.random() < 0.05 else random.randint(0, reorder * 5)
                snapshots.append(
                    StockLevel(
                        stock_id=f"STK{uuid.uuid4().hex[:10].upper()}",
                        facility_id=provider.provider_id,
                        drug_code=drug_code,
                        quantity_on_hand=qty,
                        reorder_level=reorder,
                        reported_date=snapshot_date.isoformat(),
                    )
                )

        return snapshots

    # ------------------------------------------------------------------
    # Daily generation
    # ------------------------------------------------------------------

    def generate_day(self, target_date: datetime.date, claims_per_day: int) -> DayResult:
        """Generate all entities for a single day within the timeline."""
        phase_info = self.temporal.get_phase(target_date)
        fraud_rate = _FRAUD_RATE_BY_PHASE[phase_info.phase]

        result = DayResult(date=target_date, phase=phase_info.phase)

        member_list = list(self.members.values())
        provider_list = list(self.providers.values())

        for _ in range(claims_per_day):
            member = random.choice(member_list)
            provider = random.choice(provider_list)

            fraud_pattern: FraudPattern | None = None
            if random.random() < fraud_rate:
                if self._pattern_engine.is_phantom_provider(provider.provider_id):
                    fraud_pattern = FraudPattern.PHANTOM_BILLING
                else:
                    fraud_pattern = random.choice(
                        [
                            FraudPattern.THRESHOLD_MANIPULATION,
                            FraudPattern.PHANTOM_BILLING,
                        ]
                    )

            claim = self._build_claim(member, provider, target_date, fraud_pattern)
            is_fraud = fraud_pattern is not None
            pattern_str = fraud_pattern.value if fraud_pattern else None

            # Register claim
            self._all_claims[claim.claim_id] = claim
            self.claim_fraud_status[claim.claim_id] = is_fraud
            self.claim_fraud_pattern[claim.claim_id] = pattern_str
            self._pattern_engine.register_claim(claim)

            result.claims.append(claim)
            if is_fraud:
                result.fraud_count += 1

            self._claims_by_phase[phase_info.phase] += 1
            if is_fraud:
                self._fraud_by_phase[phase_info.phase] += 1

            # Claim events (state transition log)
            events = self._build_claim_events(claim)
            self._all_claim_events.extend(events)
            result.claim_events.extend(events)

            # Vitals
            vitals = self._build_vitals(claim, target_date, pattern_str)
            if vitals:
                self._all_vitals[vitals.vitals_id] = vitals
                result.vitals.append(vitals)

            # Prescriptions
            rx = self._build_prescription(claim, target_date, pattern_str)
            if rx:
                self._all_prescriptions[rx.prescription_id] = rx
                result.prescriptions.append(rx)

            # Payment
            payment = self._build_payment(claim)
            if payment:
                self._all_payments.append(payment)
                result.payments.append(payment)

            # Investigations (returns tuple or None)
            inv_pair = self._build_investigation(claim, target_date, is_fraud, pattern_str)
            if inv_pair:
                fraud_inv, outcome = inv_pair
                self._all_fraud_investigations[fraud_inv.investigation_id] = fraud_inv
                self._all_investigations[outcome.outcome_id] = outcome
                result.fraud_investigations.append(fraud_inv)
                result.investigations.append(outcome)

        # Weekly stock snapshots
        day_number = (target_date - self.temporal.start_date).days
        if day_number % STOCK_SNAPSHOT_INTERVAL_DAYS == 0:
            snapshots = self._build_stock_snapshots(target_date)
            self._all_stock_levels.extend(snapshots)
            result.stock_levels.extend(snapshots)

        return result

    # ------------------------------------------------------------------
    # Full timeline generation
    # ------------------------------------------------------------------

    def generate_full_timeline(self, claims_per_day: int = 2) -> TimelineResult:
        """
        Generate the complete 24-month timeline.

        Default claims_per_day=2 → ~1 460 claims total (prototype scale).
        Use --full-scale / claims_per_day=500 for ~365 000 claims.

        Post-processing steps after the daily loop:
        1. Inject benefit-sharing clusters (pre-designated actor members).
        2. Inject duplicate claim pairs.
        3. Generate investigations for post-processed claims.
        """
        all_dates = self.temporal.all_dates()
        total_days = len(all_dates)

        for i, date in enumerate(all_dates):
            if i > 0 and i % 30 == 0:
                month = i // 30
                phase = self.temporal.get_phase(date).phase
                logger.info("Generating month %d of 24 (Phase %d)...", month, phase)

            self.generate_day(date, claims_per_day)

        logger.info(
            "Daily generation complete: %d claims over %d days",
            len(self._all_claims),
            total_days,
        )

        # --- Post-processing step 1: benefit-sharing clusters ---
        logger.info("Injecting benefit-sharing clusters...")
        bs_claims = self._pattern_engine.inject_benefit_sharing_clusters(
            all_claims=list(self._all_claims.values()),
            all_providers=list(self.providers.keys()),
            fraud_status=self.claim_fraud_status,
            fraud_pattern_map={k: v for k, v in self.claim_fraud_pattern.items() if v is not None},
        )
        for claim in bs_claims:
            self._all_claims[claim.claim_id] = claim
            service_date = datetime.date.fromisoformat(claim.date_of_service)
            events = self._build_claim_events(claim)
            self._all_claim_events.extend(events)
            vitals = self._build_vitals(claim, service_date, FraudPattern.BENEFIT_SHARING.value)
            if vitals:
                self._all_vitals[vitals.vitals_id] = vitals
            rx = self._build_prescription(claim, service_date, FraudPattern.BENEFIT_SHARING.value)
            if rx:
                self._all_prescriptions[rx.prescription_id] = rx
            payment = self._build_payment(claim)
            if payment:
                self._all_payments.append(payment)
            inv_pair = self._build_investigation(
                claim, service_date, True, FraudPattern.BENEFIT_SHARING.value
            )
            if inv_pair:
                fraud_inv, outcome = inv_pair
                self._all_fraud_investigations[fraud_inv.investigation_id] = fraud_inv
                self._all_investigations[outcome.outcome_id] = outcome

        # --- Post-processing step 2: duplicate pairs ---
        logger.info("Injecting duplicate claim pairs...")
        dup_claims = self._pattern_engine.inject_duplicate_pairs(
            all_claims=list(self._all_claims.values()),
            fraud_status=self.claim_fraud_status,
            fraud_pattern_map={k: v for k, v in self.claim_fraud_pattern.items() if v is not None},
        )
        for claim in dup_claims:
            self._all_claims[claim.claim_id] = claim
            service_date = datetime.date.fromisoformat(claim.date_of_service)
            events = self._build_claim_events(claim)
            self._all_claim_events.extend(events)
            vitals = self._build_vitals(claim, service_date, None)
            if vitals:
                self._all_vitals[vitals.vitals_id] = vitals
            rx = self._build_prescription(claim, service_date, None)
            if rx:
                self._all_prescriptions[rx.prescription_id] = rx
            payment = self._build_payment(claim)
            if payment:
                self._all_payments.append(payment)
            inv_pair = self._build_investigation(
                claim, service_date, True, FraudPattern.DUPLICATE_CLAIM.value
            )
            if inv_pair:
                fraud_inv, outcome = inv_pair
                self._all_fraud_investigations[fraud_inv.investigation_id] = fraud_inv
                self._all_investigations[outcome.outcome_id] = outcome

        logger.info(
            "Timeline generation complete: %d total claims, %d investigations",
            len(self._all_claims),
            len(self._all_investigations),
        )

        return TimelineResult(
            employers=list(self.employers.values()),
            plans=list(self.plans.values()),
            drug_formulary=list(self.drug_formulary.values()),
            members=list(self.members.values()),
            providers=list(self.providers.values()),
            claims=list(self._all_claims.values()),
            claim_events=self._all_claim_events,
            vitals=list(self._all_vitals.values()),
            prescriptions=list(self._all_prescriptions.values()),
            payments=self._all_payments,
            stock_levels=self._all_stock_levels,
            fraud_investigations=list(self._all_fraud_investigations.values()),
            investigations=list(self._all_investigations.values()),
        )

    # ------------------------------------------------------------------
    # Summary statistics
    # ------------------------------------------------------------------

    def log_summary(self) -> None:
        """Log a breakdown of generated data by phase and fraud pattern."""
        total_claims = sum(self._claims_by_phase.values())
        total_fraud = sum(self._fraud_by_phase.values())

        logger.info("=" * 60)
        logger.info("GENERATION SUMMARY — 24-Month Timeline")
        logger.info("=" * 60)
        logger.info("Total claims: %d", total_claims)

        if total_claims:
            logger.info(
                "Fraudulent claims: %d (%.2f%%)",
                total_fraud,
                100.0 * total_fraud / total_claims,
            )

        logger.info("Breakdown by phase:")
        for phase in range(1, 5):
            claims = self._claims_by_phase.get(phase, 0)
            fraud = self._fraud_by_phase.get(phase, 0)
            if claims:
                logger.info(
                    "  Phase %d: %d claims, %d fraud (%.2f%%)",
                    phase,
                    claims,
                    fraud,
                    100.0 * fraud / claims,
                )

        logger.info("Fraud pattern distribution:")
        pattern_counts: dict[str, int] = {}
        for p in self.claim_fraud_pattern.values():
            if p:
                pattern_counts[p] = pattern_counts.get(p, 0) + 1

        for pattern, count in pattern_counts.items():
            pct = 100.0 * count / total_fraud if total_fraud else 0.0
            logger.info("  %s: %d (%.1f%%)", pattern, count, pct)

        total_investigations = len(self._all_investigations)
        logger.info("Investigations: %d", total_investigations)

        if total_investigations:
            confirmed = sum(
                1 for inv in self._all_investigations.values() if inv.final_label == "confirmed_fraud"
            )
            logger.info(
                "  Confirmed fraud: %d  |  False positives: %d",
                confirmed,
                total_investigations - confirmed,
            )
