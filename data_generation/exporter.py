"""
CSV exporter for AfyaBima generated data.

Exports each entity type to its own CSV file, matching the PostgreSQL schema
column names exactly so the files can be loaded directly via COPY or Kafka Connect.

Seed reference files (diagnosis codes, procedure codes, plans) are written to
a sibling seeds/ directory so dbt can consume them as seed models.
"""

from __future__ import annotations

import logging
from dataclasses import asdict
from pathlib import Path

import pandas as pd

from data_generation.config import (
    DIAGNOSIS_CODES_KENYA,
    ICD10_CLINICAL_CATEGORIES,
    PROCEDURE_CODES_KES,
)
from data_generation.generator import TimelineResult

logger = logging.getLogger(__name__)


def export_to_csv(result: TimelineResult, output_dir: str | Path) -> None:
    """
    Write all entities in *result* to CSV files under *output_dir*.

    Directory structure created:
        output_dir/
            # Reference / master data
            employers.csv
            plans.csv
            drug_formulary.csv
            members.csv
            providers.csv
            # Transactional event data
            claims.csv
            claim_events.csv
            vitals.csv
            prescriptions.csv
            payments.csv
            stock_levels.csv
            # Fraud pipeline
            fraud_investigations.csv
            fraud_investigation_outcomes.csv
            seeds/
                diagnosis_code_lookup.csv
                procedure_code_lookup.csv

    Parameters
    ----------
    result:
        A TimelineResult returned by AfyaBimaKenyaGenerator.generate_full_timeline().
    output_dir:
        Root directory for output files.  Created if it does not exist.
    """
    root = Path(output_dir)
    seeds_dir = root / "seeds"
    root.mkdir(parents=True, exist_ok=True)
    seeds_dir.mkdir(parents=True, exist_ok=True)

    # --- Reference / master data ---
    _write_entities(root / "employers.csv", [asdict(e) for e in result.employers], "employers")
    _write_entities(root / "plans.csv", [asdict(p) for p in result.plans], "plans")
    _write_entities(root / "drug_formulary.csv", [asdict(d) for d in result.drug_formulary], "drug_formulary")
    _write_entities(root / "members.csv", [asdict(m) for m in result.members], "members")
    _write_entities(root / "providers.csv", [asdict(p) for p in result.providers], "providers")

    # --- Transactional event data ---
    _write_entities(root / "claims.csv", [asdict(c) for c in result.claims], "claims")
    _write_entities(root / "claim_events.csv", [asdict(e) for e in result.claim_events], "claim_events")
    _write_entities(root / "vitals.csv", [asdict(v) for v in result.vitals], "vitals")
    _write_entities(root / "prescriptions.csv", [asdict(p) for p in result.prescriptions], "prescriptions")
    _write_entities(root / "payments.csv", [asdict(p) for p in result.payments], "payments")
    _write_entities(root / "stock_levels.csv", [asdict(s) for s in result.stock_levels], "stock_levels")

    # --- Fraud pipeline ---
    _write_entities(
        root / "fraud_investigations.csv",
        [asdict(i) for i in result.fraud_investigations],
        "fraud_investigations",
    )
    _write_entities(
        root / "fraud_investigation_outcomes.csv",
        [asdict(i) for i in result.investigations],
        "fraud_investigation_outcomes",
    )

    # --- Seed / reference files (for dbt seeds) ---
    _write_diagnosis_codes(seeds_dir / "diagnosis_code_lookup.csv")
    _write_procedure_codes(seeds_dir / "procedure_code_lookup.csv")

    logger.info("Export complete. Output directory: %s", root.resolve())


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------


def _write_entities(path: Path, rows: list[dict[str, object]], label: str) -> None:
    if not rows:
        logger.warning("No %s records to export — skipping %s", label, path.name)
        return
    df = pd.DataFrame(rows)
    # When a column contains int | None, pandas upcasts to float64 (e.g. 8 → 8.0).
    # PostgreSQL INT columns reject "8.0". Convert any float64 column whose
    # non-null values are all whole numbers back to nullable integer dtype Int64.
    for col in df.select_dtypes(include="float64").columns:
        non_null = df[col].dropna()
        if len(non_null) > 0 and (non_null == non_null.astype("int64")).all():
            df[col] = df[col].astype("Int64")
    df.to_csv(path, index=False)
    logger.info("Exported %d %s → %s", len(rows), label, path.name)


def _write_diagnosis_codes(path: Path) -> None:
    rows = [
        {
            "diagnosis_code": code,
            "description": desc,
            "clinical_category": ICD10_CLINICAL_CATEGORIES.get(code, "Other"),
        }
        for code, desc in DIAGNOSIS_CODES_KENYA.items()
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    logger.info("Exported %d diagnosis codes → %s", len(rows), path.name)


def _write_procedure_codes(path: Path) -> None:
    rows = [
        {
            "procedure_code": code,
            "description": details["desc"],
            "category": details["category"],
            "base_cost_kes": details["base_cost"],
        }
        for code, details in PROCEDURE_CODES_KES.items()
    ]
    pd.DataFrame(rows).to_csv(path, index=False)
    logger.info("Exported %d procedure codes → %s", len(rows), path.name)
