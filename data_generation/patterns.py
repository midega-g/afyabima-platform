"""
FraudPatternEngine — correct implementation of all four proposal fraud patterns.

Design principles
-----------------
1. Benefit sharing and duplicate claims are PRE-PLANNED, not post-hoc.
   - A fixed set of member IDs is designated as benefit-sharing actors at init time.
   - A fixed set of provider IDs is designated as phantom billers at init time.
   - Duplicate pairs are injected during post-processing with correct amount variance.
2. No pattern is a stub — every pattern produces the statistical signal that
   int_claim_fraud_features is designed to detect.
3. This module only manipulates data; it never decides whether to investigate.
   That responsibility belongs to TemporalController.

Pattern → detectable signal mapping
------------------------------------
PHANTOM_BILLING         → consultation claim with no Vitals AND no Prescription record
THRESHOLD_MANIPULATION  → billed_amount in [0.92xT, 0.99xT) for T in KES_THRESHOLDS
BENEFIT_SHARING         → same member_id at ≥4 distinct provider_ids within 7 days,
                          same diagnosis_code
DUPLICATE_CLAIM         → same (member_id, provider_id, diagnosis_code) within 48 h,
                          billed_amount variance < 5 %
"""

from __future__ import annotations

import datetime
import logging
import random
import uuid

from data_generation.config import (
    BENEFIT_SHARING_ACTOR_COUNT,
    DUPLICATE_PAIR_COUNT,
    KES_THRESHOLDS,
    PHANTOM_PROVIDER_COUNT,
    THRESHOLD_LOWER_BOUND,
    THRESHOLD_UPPER_BOUND,
    FraudPattern,
)
from data_generation.models import ClaimSubmitted

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Internal type alias
# ---------------------------------------------------------------------------

# (claim_id, service_date, provider_id, diagnosis_code, billed_amount)
_ClaimIndex = tuple[str, datetime.date, str, str, float]


class FraudPatternEngine:
    """
    Responsible for:
      - Pre-designating benefit-sharing actor member IDs.
      - Pre-designating phantom-billing provider IDs.
      - Modifying a claim's amount for THRESHOLD_MANIPULATION.
      - Deciding whether a claim from a phantom provider should lack vitals/prescriptions.
      - Building duplicate claim pairs in post-processing.
      - Retroactively tagging benefit-sharing clusters after the full timeline is built.
    """

    def __init__(
        self,
        all_member_ids: list[str],
        all_provider_ids: list[str],
    ) -> None:
        if len(all_member_ids) < BENEFIT_SHARING_ACTOR_COUNT:
            raise ValueError(
                f"Need at least {BENEFIT_SHARING_ACTOR_COUNT} members for benefit-sharing actors, "
                f"got {len(all_member_ids)}"
            )
        if len(all_provider_ids) < PHANTOM_PROVIDER_COUNT:
            raise ValueError(
                f"Need at least {PHANTOM_PROVIDER_COUNT} providers for phantom billers, "
                f"got {len(all_provider_ids)}"
            )

        # Designate actors once — they stay fixed for the full 24-month run.
        self.benefit_sharing_actor_ids: set[str] = set(
            random.sample(all_member_ids, BENEFIT_SHARING_ACTOR_COUNT)
        )
        self.phantom_provider_ids: set[str] = set(random.sample(all_provider_ids, PHANTOM_PROVIDER_COUNT))

        logger.info(
            "FraudPatternEngine: %d benefit-sharing actors, %d phantom providers",
            BENEFIT_SHARING_ACTOR_COUNT,
            PHANTOM_PROVIDER_COUNT,
        )

        # Running index of all claims — used for benefit-sharing and duplicate detection.
        # Stored as list of _ClaimIndex tuples; appended to as claims are generated.
        self._claim_index: list[_ClaimIndex] = []

    # ------------------------------------------------------------------
    # Public helpers called by AfyaBimaKenyaGenerator
    # ------------------------------------------------------------------

    def is_benefit_sharing_actor(self, member_id: str) -> bool:
        return member_id in self.benefit_sharing_actor_ids

    def is_phantom_provider(self, provider_id: str) -> bool:
        return provider_id in self.phantom_provider_ids

    def apply_threshold_manipulation(self, base_amount: float) -> float:
        """
        Return an amount that sits in [0.92xT, 0.99xT) for a randomly chosen
        threshold T.  The threshold is chosen to be plausible relative to
        base_amount so the claim does not look absurdly over/under-priced.
        """
        # Prefer the threshold closest to base_amount from above.
        candidates = [t for t in KES_THRESHOLDS if t >= base_amount * 0.5]
        threshold = candidates[0] if candidates else KES_THRESHOLDS[-1]
        factor = random.uniform(THRESHOLD_LOWER_BOUND, THRESHOLD_UPPER_BOUND)
        return round(threshold * factor * 20) / 20  # round to nearest 0.05 KES

    def register_claim(self, claim: ClaimSubmitted) -> None:
        """Add a claim to the running index for pattern detection."""
        self._claim_index.append(
            (
                claim.claim_id,
                datetime.date.fromisoformat(claim.date_of_service),
                claim.provider_id,
                claim.diagnosis_code,
                claim.amount_claimed,
            )
        )

    # ------------------------------------------------------------------
    # Benefit-sharing cluster injection
    # ------------------------------------------------------------------

    def inject_benefit_sharing_clusters(
        self,
        all_claims: list[ClaimSubmitted],
        all_providers: list[str],
        fraud_status: dict[str, bool],
        fraud_pattern_map: dict[str, str],
    ) -> list[ClaimSubmitted]:
        """
        For each benefit-sharing actor, find windows in the timeline where they
        already have ≥1 claim, then inject additional claims at distinct providers
        within the same 7-day window with the same diagnosis to build a ≥4-provider
        cluster.

        Returns a list of NEW claims to be appended to all_claims.
        Called once after the full timeline is built.
        """
        # Index existing claims by member
        by_member: dict[str, list[ClaimSubmitted]] = {}
        for claim in all_claims:
            by_member.setdefault(claim.member_id, []).append(claim)

        new_claims: list[ClaimSubmitted] = []

        for actor_id in self.benefit_sharing_actor_ids:
            actor_claims = by_member.get(actor_id, [])
            if not actor_claims:
                logger.warning(
                    "Benefit-sharing actor %s has no claims — skipping cluster injection",
                    actor_id,
                )
                continue

            # Pick a random seed claim from phases 2–4 (investigations exist)
            eligible = [
                c
                for c in actor_claims
                if datetime.date.fromisoformat(c.date_of_service)
                >= (
                    datetime.date.fromisoformat(actor_claims[0].date_of_service)
                    + datetime.timedelta(days=180)
                )  # roughly month 7+
            ]
            if not eligible:
                eligible = actor_claims

            seed_claim = random.choice(eligible)
            seed_date = datetime.date.fromisoformat(seed_claim.date_of_service)
            shared_diagnosis = seed_claim.diagnosis_code

            # Collect providers already used in this window
            window_providers: set[str] = {seed_claim.provider_id}

            # We need ≥4 distinct providers total.  The seed gives us 1.
            # Inject 3 more claims at different providers within ±3 days.
            available_providers = [p for p in all_providers if p not in window_providers]
            if len(available_providers) < 3:
                logger.warning(
                    "Not enough distinct providers to build benefit-sharing cluster for %s",
                    actor_id,
                )
                continue

            injection_providers = random.sample(available_providers, 3)

            for _i, prov_id in enumerate(injection_providers):
                offset_days = random.randint(0, 3)
                service_date = seed_date + datetime.timedelta(days=offset_days)
                # Slight amount inflation — the signal, per proposal
                amount = seed_claim.amount_claimed * random.uniform(1.10, 1.30)
                amount = round(amount * 20) / 20

                new_claim = ClaimSubmitted(
                    claim_id=f"CLM{uuid.uuid4().hex[:12].upper()}",
                    member_id=actor_id,
                    provider_id=prov_id,
                    date_of_service=service_date.isoformat(),
                    date_submitted=(service_date + datetime.timedelta(days=random.randint(0, 1))).isoformat(),
                    procedure_code=seed_claim.procedure_code,
                    diagnosis_code=shared_diagnosis,
                    quantity=seed_claim.quantity,
                    amount_claimed=amount,
                    amount_approved=None,
                    status="submitted",
                    submission_channel="api",
                    submitted_at=service_date.isoformat() + "T08:00:00",
                    updated_at=service_date.isoformat() + "T08:00:00",
                    is_emergency=False,
                    prior_auth_number=None,
                )
                fraud_status[new_claim.claim_id] = True
                fraud_pattern_map[new_claim.claim_id] = FraudPattern.BENEFIT_SHARING.value
                new_claims.append(new_claim)

            # Also tag the seed claim as part of the pattern
            fraud_status[seed_claim.claim_id] = True
            fraud_pattern_map[seed_claim.claim_id] = FraudPattern.BENEFIT_SHARING.value

            logger.debug(
                "Benefit-sharing cluster for actor %s: seed %s + %d injected claims on %s",
                actor_id,
                seed_claim.claim_id,
                len(injection_providers),
                seed_date,
            )

        logger.info(
            "Benefit-sharing injection complete: %d new claims added",
            len(new_claims),
        )
        return new_claims

    # ------------------------------------------------------------------
    # Duplicate claim pair injection
    # ------------------------------------------------------------------

    def inject_duplicate_pairs(
        self,
        all_claims: list[ClaimSubmitted],
        fraud_status: dict[str, bool],
        fraud_pattern_map: dict[str, str],
    ) -> list[ClaimSubmitted]:
        """
        Create DUPLICATE_PAIR_COUNT duplicate claim pairs.

        Each duplicate:
        - Same (member_id, provider_id, diagnosis_code) as the original.
        - date_of_service within 48 hours of the original (whole-day granularity).
        - billed_amount differs by a random amount in (0 %, 5 %) — not exact
          duplicates, which would be caught by simple deduplication rules.

        Both the original and the duplicate are tagged as DUPLICATE_CLAIM so
        the pair is correctly represented in the fraud label store.
        """
        # Pick candidate claims from phases 2–4 (so investigations can be generated)
        # Prefer claims that are NOT already tagged as fraud.
        candidates = [c for c in all_claims if not fraud_status.get(c.claim_id, False)]

        if len(candidates) < DUPLICATE_PAIR_COUNT:
            logger.warning(
                "Only %d non-fraud candidate claims available for duplicate pairs "
                "(need %d); using all available",
                len(candidates),
                DUPLICATE_PAIR_COUNT,
            )

        chosen = random.sample(candidates, min(DUPLICATE_PAIR_COUNT, len(candidates)))
        new_claims: list[ClaimSubmitted] = []

        for original in chosen:
            original_date = datetime.date.fromisoformat(original.date_of_service)

            # Date offset: 0 or 1 day (keeping within 48-hour window)
            day_offset = random.randint(0, 1)
            dup_date = original_date + datetime.timedelta(days=day_offset)

            # Amount variance: strictly between 0 % and 5 % (not exactly equal)
            variance_pct = random.uniform(0.001, 0.049)
            direction = random.choice([-1, 1])
            dup_amount = original.amount_claimed * (1.0 + direction * variance_pct)
            dup_amount = max(1.0, round(dup_amount * 20) / 20)

            dup_claim = ClaimSubmitted(
                claim_id=f"CLM{uuid.uuid4().hex[:12].upper()}",
                member_id=original.member_id,
                provider_id=original.provider_id,
                date_of_service=dup_date.isoformat(),
                date_submitted=(dup_date + datetime.timedelta(days=random.randint(0, 2))).isoformat(),
                procedure_code=original.procedure_code,
                diagnosis_code=original.diagnosis_code,
                quantity=original.quantity,
                amount_claimed=dup_amount,
                amount_approved=None,
                status="submitted",
                submission_channel="api",
                submitted_at=dup_date.isoformat() + "T08:00:00",
                updated_at=dup_date.isoformat() + "T08:00:00",
                is_emergency=False,
                prior_auth_number=None,
            )

            # Tag BOTH the original and the duplicate
            fraud_status[original.claim_id] = True
            fraud_pattern_map[original.claim_id] = FraudPattern.DUPLICATE_CLAIM.value
            fraud_status[dup_claim.claim_id] = True
            fraud_pattern_map[dup_claim.claim_id] = FraudPattern.DUPLICATE_CLAIM.value

            new_claims.append(dup_claim)

            logger.debug(
                "Duplicate pair: original=%s dup=%s date_offset=%dd amount_variance=%.1f%%",
                original.claim_id,
                dup_claim.claim_id,
                day_offset,
                abs(variance_pct * 100),
            )

        logger.info(
            "Duplicate injection complete: %d duplicate claims added (%d pairs)",
            len(new_claims),
            len(new_claims),
        )
        return new_claims

    # ------------------------------------------------------------------
    # Vitals / prescription suppression (phantom billing signal)
    # ------------------------------------------------------------------

    def should_suppress_vitals(self, claim: ClaimSubmitted, fraud_pattern: str | None) -> bool:
        """
        Return True if vitals should be withheld for this claim.

        Only phantom billing suppresses vitals — all other fraud types have normal
        clinical documentation (that's what makes them hard to catch).
        """
        if fraud_pattern != FraudPattern.PHANTOM_BILLING.value:
            return False
        # Suppress vitals for 80 % of phantom billing consultations
        return random.random() < 0.80

    def should_suppress_prescription(self, claim: ClaimSubmitted, fraud_pattern: str | None) -> bool:
        """
        Return True if the prescription record should be withheld.

        Only phantom billing suppresses prescriptions.
        """
        if fraud_pattern != FraudPattern.PHANTOM_BILLING.value:
            return False
        return random.random() < 0.65
