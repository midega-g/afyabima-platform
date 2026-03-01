# `data_generation/` — AfyaBima Synthetic Data Generator

## What this package does

This package generates a complete, realistic 24-month dataset of Kenyan healthcare
insurance claims. Its output is a set of CSV files that load directly into the AfyaBima
PostgreSQL database, matching the production schema column-for-column.

The data is not random noise. Every record — every claim, every prescription, every
vitals reading — is generated according to real Kenyan Ministry of Health classifications,
SHA (Social Health Authority) fee schedules, ICD-10 diagnosis codes, and approved drug
formulary standards. A controlled subset of the data contains synthetic fraud, embedded
with mathematically precise signals that the downstream ML model is designed to detect.

---

## Why this package exists

Building a fraud detection model requires labelled training data: claims that are
definitively known to be fraudulent or legitimate. In a real insurance system, that ground
truth accumulates slowly as investigators close cases — often months or years after the
claim date. This package compresses that timeline. It generates 24 months of claims history
in seconds, with four distinct fraud patterns already embedded and labelled, so the ML
model has a complete, realistic training dataset from day one.

---

## The four fraud patterns

Every design decision in this package ultimately serves one goal: producing data that
contains these four fraud signals in a form that a machine learning model can learn to
detect. Understanding the patterns before reading any individual file is essential.

| Pattern | What it looks like in the data | The detectable signal |
| --- | --- | --- |
| **Phantom Billing** | A consultation claim exists but no vitals record and no prescription record exist for the same claim | Missing clinical documentation for a claim that should have it |
| **Threshold Manipulation** | The billed amount sits between 92% and 99% of a known auto-approval threshold (KES 5k, 10k, or 25k) | Amount clusters just below the boundaries where human review is triggered |
| **Benefit Sharing** | The same member visits 4 or more distinct providers within 7 days, all with the same diagnosis code | Abnormally high provider count per member per time window |
| **Duplicate Claim** | The same (member, provider, diagnosis) combination appears twice within 48 hours, with less than 5% difference in billed amount | Near-identical claims in a very short window |

---

## Package structure

```txt
data_generation/
│
├── __init__.py       (20 lines)   Declares the public API
├── config.py        (693 lines)   All constants: codes, thresholds, drugs, geography
├── models.py        (250 lines)   Data shapes: one @dataclass per database table
├── temporal.py      (203 lines)   24-month phase timeline and investigation scheduling
├── patterns.py      (369 lines)   Fraud pattern logic and injection engine
├── generator.py    (1080 lines)   Core assembly: builds all entities day by day
├── exporter.py      (134 lines)   Writes generator output to CSV files
└── main.py          (289 lines)   CLI entrypoint: argument parsing and orchestration
```

**Total: ~3,038 lines across 8 files.**

---

## How the files depend on each other

The dependency graph is a strict one-way hierarchy. No circular imports exist. When reading
a file, you only ever need to understand files that appear above it in this diagram.

```ascii
                     ┌─────────────┐
                     │  config.py  │  ← no project dependencies
                     └──────┬──────┘
                            │ imported by
          ┌─────────────────┼─────────────────┐
          ▼                 ▼                 ▼
    ┌──────────┐     ┌────────────┐    ┌─────────────┐
    │ models.py│     │temporal.py │    │ patterns.py │ ← imports config + models
    └────┬─────┘     └──────┬─────┘    └──────┬──────┘
         │                  │                  │
         └──────────────────┴──────────────────┘
                            │  all imported by
                     ┌──────▼──────┐
                     │ generator.py│  ← the core assembly engine
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │ exporter.py │  ← takes generator output, writes CSVs
                     └──────┬──────┘
                            │
                     ┌──────▼──────┐
                     │   main.py   │  ← orchestrates everything via CLI
                     └─────────────┘
```

**Recommended reading order: `config → models → temporal → patterns → generator → exporter → main`**

This order mirrors the dependency graph: each file you read only references concepts
and code you have already encountered.

---

## File-by-file summary

| File | Single-line purpose | Key names exported |
| --- | --- | --- |
| `config.py` | Every constant the project uses | `FraudPattern`, `KES_THRESHOLDS`, `DRUG_FORMULARY`, `DIAGNOSIS_CODES_KENYA`, `GLOBAL_SEED` |
| `models.py` | Python mirror of the PostgreSQL schema | `Member`, `Provider`, `ClaimSubmitted`, `Vitals`, `Prescription`, `Payment`, `ClaimEvent`, `FraudInvestigation`, `InvestigationOutcome`, `Plan`, `DrugFormulary`, `StockLevel` |
| `temporal.py` | 24-month phase timeline and investigation scheduling | `TemporalController`, `PhaseInfo`, `PhaseSpec` |
| `patterns.py` | Fraud pattern injection engine | `FraudPatternEngine` |
| `generator.py` | Assembles all entities day by day | `AfyaBimaKenyaGenerator`, `DayResult`, `TimelineResult` |
| `exporter.py` | Serialises `TimelineResult` to CSV files | `export_to_csv` |
| `main.py` | CLI: parse args, seed RNG, run, export, verify | `main()` |
| `__init__.py` | Public API declaration | Re-exports `AfyaBimaKenyaGenerator`, `TemporalController`, `export_to_csv` |

---

## Running the generator

```bash
# Install dependencies
uv sync

# Prototype scale — approximately 1,460 claims over 720 days
uv run python -m data_generation.main --output-dir ./afyabima_data

# Full scale — approximately 365,000 claims
uv run python -m data_generation.main --full-scale --output-dir ./afyabima_data

# Custom volume and date range
uv run python -m data_generation.main \
    --claims-per-day 10 \
    --start-date 2023-01-01 \
    --output-dir ./afyabima_data

# Fully reproducible output — same seed always produces identical CSVs
uv run python -m data_generation.main --seed 42
```

---

## What the generator produces

Running at prototype scale creates these files under `--output-dir`:

```txt
afyabima_data/
│
├── plans.csv                            8 rows  — insurance products
├── employers.csv                       30 rows  — corporate clients
├── drug_formulary.csv                  30 rows  — SHA-approved drugs
├── providers.csv                      ~50 rows  — healthcare facilities
├── members.csv                       ~200 rows  — insured beneficiaries
│
├── claims.csv                       ~1,460 rows — submitted claims
├── claim_events.csv               ~3,000+ rows  — 2–4 state transitions per claim
├── vitals.csv                        ~70% of claims have a record
├── prescriptions.csv                 ~60% of claims have a record
├── payments.csv                      ~35% of claims have a record
├── stock_levels.csv                 weekly snapshots per dispensing facility per drug
│
├── fraud_investigations.csv          ~20% of Phase 2+ claims
├── fraud_investigation_outcomes.csv  one row per investigation
│
└── seeds/
    ├── diagnosis_code_lookup.csv     loads raw.icd10_codes in PostgreSQL
    └── procedure_code_lookup.csv     informational reference only
```

---

## Core design principles

**Single source of truth.**
Every code, threshold, rate, and domain constant lives in `config.py`. No other file
defines domain values — they all import from config. Changing the SHA auto-approval
threshold from KES 5,000 to KES 8,000 requires editing exactly one line in one file.

**Schema-first data models.**
The dataclasses in `models.py` were designed so that their field names exactly match
PostgreSQL column names. Calling `dataclasses.asdict()` on any instance produces a
dictionary whose keys are valid column names — no transformation layer needed between
generation and loading.

**Fraud is structural, not a flag.**
Each fraud pattern is not produced by setting `is_fraud = True` on a record. Instead,
each pattern creates a structural anomaly in the relationships between tables — a missing
vitals row, an amount in a specific numerical range, a provider-count pattern. The ML
model learns to detect features, not labels.

**Deterministic by default.**
`GLOBAL_SEED = 42` in `config.py` ensures the same seed always produces byte-identical
CSV output. Reproducibility is essential for ML experiments and for debugging regressions
between generator versions.

**Batch-only by design.**
Two fraud patterns (benefit sharing and duplicate claim injection) require scanning the
entire claim history before they can be applied. The generator is therefore a batch tool.
The companion `stream_producer.py` handles the Kafka transition by replaying the generated
CSVs as time-ordered events — the database ends up with identical rows either way.

---

## Per-file documentation

Each file has its own detailed README. Read them in this order:

1. [`config.md`](./config.md) — Start here. Every name in every other file originates here.
2. [`models.md`](./models.md) — The data shapes everything produces and consumes.
3. [`temporal.md`](./temporal.md) — The 24-month timeline that governs when things happen.
4. [`patterns.md`](./patterns.md) — How fraud is structurally embedded into the data.
5. `generator.md` — The core assembly engine (to follow).
6. `exporter.md` — How output reaches CSV files.
7. `main.md` — Running the entire system end to end.
