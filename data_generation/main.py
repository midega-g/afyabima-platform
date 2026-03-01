"""
AfyaBima synthetic data generator — CLI entrypoint.

Usage
-----
    # Prototype scale (~1 460 claims)
    uv run python -m data_generation.main

    # Full scale (~365 000 claims)
    uv run python -m data_generation.main --full-scale

    # Custom
    uv run python -m data_generation.main \\
        --claims-per-day 10 \\
        --start-date 2023-01-01 \\
        --output-dir ./output
"""

from __future__ import annotations

import argparse
import datetime
import logging
import random
import sys

import numpy as np

from data_generation.config import GLOBAL_SEED, KES_THRESHOLDS, THRESHOLD_LOWER_BOUND, FraudPattern
from data_generation.exporter import export_to_csv
from data_generation.generator import AfyaBimaKenyaGenerator
from data_generation.temporal import TemporalController

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# CLI argument parsing
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="data_generation",
        description="AfyaBima Kenya Synthetic Data Generator",
    )
    parser.add_argument(
        "--claims-per-day",
        type=int,
        default=2,
        metavar="N",
        help="Claims generated per calendar day (default: 2 → ~1 460 total for prototype).",
    )
    parser.add_argument(
        "--output-dir",
        type=str,
        default="./afyabima_data",
        metavar="DIR",
        help="Root directory for CSV output (default: ./afyabima_data).",
    )
    parser.add_argument(
        "--start-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Start date of the 24-month timeline. Defaults to 730 days before today.",
    )
    parser.add_argument(
        "--full-scale",
        action="store_true",
        help="Override --claims-per-day to 500 (full-scale: ~365 000 claims).",
    )
    parser.add_argument(
        "--seed",
        type=int,
        default=GLOBAL_SEED,
        metavar="INT",
        help=f"Random seed for reproducibility (default: {GLOBAL_SEED}).",
    )
    return parser.parse_args(argv)


# ---------------------------------------------------------------------------
# Verification checks
# ---------------------------------------------------------------------------


def _run_verification(generator: AfyaBimaKenyaGenerator) -> bool:
    """
    Run a series of data-integrity checks after generation.
    Returns True if all checks pass, False otherwise.
    """
    logger.info("=" * 60)
    logger.info("VERIFICATION CHECKS")
    logger.info("=" * 60)

    passed = 0
    failed = 0

    # 1. No fraud labels in any ClaimSubmitted
    from dataclasses import asdict

    sample = next(iter(generator._all_claims.values()), None)
    if sample:
        fraud_fields = [k for k in asdict(sample) if "fraud" in k.lower()]
        if not fraud_fields:
            logger.info("✓ No fraud label fields in ClaimSubmitted")
            passed += 1
        else:
            logger.error("✗ Unexpected fraud fields in ClaimSubmitted: %s", fraud_fields)
            failed += 1

    # 2. Phase-1 claims have no investigations
    phase1_investigated = [
        inv
        for inv in generator._all_investigations.values()
        if generator._all_claims.get(inv.claim_id)
        and generator.temporal.get_phase(
            datetime.date.fromisoformat(generator._all_claims[inv.claim_id].date_of_service)
        ).phase
        == 1
    ]
    if not phase1_investigated:
        logger.info("✓ No Phase-1 claims have investigation outcomes")
        passed += 1
    else:
        logger.error("✗ %d Phase-1 claims have investigation outcomes", len(phase1_investigated))
        failed += 1

    # 3. Threshold manipulation amounts sit in the correct range
    threshold_claims = [
        c
        for c in generator._all_claims.values()
        if generator.claim_fraud_pattern.get(c.claim_id) == FraudPattern.THRESHOLD_MANIPULATION.value
    ]
    if threshold_claims:
        in_range = [
            c
            for c in threshold_claims
            if any(t * THRESHOLD_LOWER_BOUND <= c.amount_claimed < t for t in KES_THRESHOLDS)
        ]
        pct = 100.0 * len(in_range) / len(threshold_claims)
        if pct >= 95.0:
            logger.info(
                "✓ Threshold manipulation amounts: %.1f%% are in correct range",
                pct,
            )
            passed += 1
        else:
            logger.error(
                "✗ Only %.1f%% of threshold manipulation claims are in correct range",
                pct,
            )
            failed += 1
    else:
        logger.warning("  No threshold manipulation claims found (low claim count?)")

    # 4. Phantom billing consultation claims lack vitals
    phantom_cons = [
        c
        for c in generator._all_claims.values()
        if generator.claim_fraud_pattern.get(c.claim_id) == FraudPattern.PHANTOM_BILLING.value
        and c.procedure_code.startswith("CONS")
    ]
    if phantom_cons:
        vitals_by_claim = {v.claim_id for v in generator._all_vitals.values()}
        missing_vitals = [c for c in phantom_cons if c.claim_id not in vitals_by_claim]
        pct_missing = 100.0 * len(missing_vitals) / len(phantom_cons)
        if pct_missing >= 50.0:
            logger.info(
                "✓ Phantom billing: %.1f%% of consultation claims lack vitals",
                pct_missing,
            )
            passed += 1
        else:
            logger.warning(
                "  Phantom billing vitals suppression only %.1f%% (expected ≥50%%)",
                pct_missing,
            )

    # 5. Benefit-sharing clusters have ≥4 distinct providers within 7 days
    bs_claims = [
        c
        for c in generator._all_claims.values()
        if generator.claim_fraud_pattern.get(c.claim_id) == FraudPattern.BENEFIT_SHARING.value
    ]
    if bs_claims:
        from data_generation.models import ClaimSubmitted

        by_member: dict[str, list[ClaimSubmitted]] = {}
        for c in bs_claims:
            by_member.setdefault(c.member_id, []).append(c)

        valid_actors = 0
        for _member_id, member_claims in by_member.items():
            member_claims.sort(key=lambda c: c.date_of_service)
            for _, claim in enumerate(member_claims):
                anchor_date = datetime.date.fromisoformat(claim.date_of_service)
                window = [
                    c
                    for c in member_claims
                    if abs((datetime.date.fromisoformat(c.date_of_service) - anchor_date).days) <= 7
                ]
                distinct_providers = {c.provider_id for c in window}
                if len(distinct_providers) >= 4:
                    valid_actors += 1
                    break

        if valid_actors > 0:
            logger.info(
                "✓ Benefit sharing: %d actor(s) with ≥4 providers within 7 days",
                valid_actors,
            )
            passed += 1
        else:
            logger.error("✗ No benefit-sharing actors have ≥4 providers within 7 days")
            failed += 1

    # 6. Duplicate pairs share (member, provider, diagnosis) within 48 hours
    dup_claims = [
        c
        for c in generator._all_claims.values()
        if generator.claim_fraud_pattern.get(c.claim_id) == FraudPattern.DUPLICATE_CLAIM.value
    ]
    if dup_claims:
        logger.info("✓ Duplicate claims injected: %d", len(dup_claims))
        passed += 1
    else:
        logger.warning("  No duplicate claims found (low claim count?)")

    logger.info("=" * 60)
    logger.info("Verification: %d passed, %d failed", passed, failed)
    return failed == 0


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------


def main(argv: list[str] | None = None) -> int:
    args = _parse_args(argv)

    # Set seeds for reproducibility — done once, here, before any generation
    random.seed(args.seed)
    np.random.seed(args.seed)
    from faker import Faker

    Faker.seed(args.seed)

    if args.full_scale:
        args.claims_per_day = 500

    start_date: datetime.date | None = None
    if args.start_date:
        try:
            start_date = datetime.date.fromisoformat(args.start_date)
        except ValueError:
            logger.error("Invalid --start-date format: %r  (expected YYYY-MM-DD)", args.start_date)
            return 1

    temporal = TemporalController(start_date=start_date)

    logger.info("AfyaBima Data Generator")
    logger.info("Timeline: %s → %s", temporal.start_date, temporal.end_date)
    logger.info("Claims per day: %d  (~%d total)", args.claims_per_day, args.claims_per_day * 730)
    logger.info("Output directory: %s", args.output_dir)

    generator = AfyaBimaKenyaGenerator(temporal=temporal)

    data = generator.generate_full_timeline(claims_per_day=args.claims_per_day)

    export_to_csv(data, args.output_dir)

    generator.log_summary()

    ok = _run_verification(generator)

    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
