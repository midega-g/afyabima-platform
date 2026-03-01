# `config.py` — The Single Source of Truth

## What this file is

`config.py` is the domain knowledge of the entire AfyaBima project written in Python. It contains no functions, no logic, and no classes that *do* anything. Its only job is to define values — and to define them in one place.

Every other file in the generator imports from here. If you need to know what the SHA auto-approval threshold is, what drugs are in the formulary, what facility types exist in Kenya, or how many duplicate claims to inject, you read this file. If you need to change any of those values, you change them here, and only here. That is the whole design principle.

The file is 694 lines and defines 29 named constants and 3 type-describing classes. Reading it is equivalent to reading the data dictionary of the project.

---

## Where it fits

```txt
                        config.py
                       (no imports from this project)
                              │
          ┌───────────────────┼────────────────────────────────────────
          │                   │                      │                │
     patterns.py         generator.py           exporter.py        main.py
     imports 7 names     imports 18 names        imports 6 names    imports 4 names
```

Nothing imports *into* `config.py`. It is the root of the dependency tree.

**What each consumer takes from it:**

| File | Names it imports |
|---|---|
| `patterns.py` | `FraudPattern`, `KES_THRESHOLDS`, `THRESHOLD_LOWER_BOUND`, `THRESHOLD_UPPER_BOUND`, `BENEFIT_SHARING_ACTOR_COUNT`, `PHANTOM_PROVIDER_COUNT`, `DUPLICATE_PAIR_COUNT` |
| `generator.py` | `FraudPattern`, all geography, facility, plan, procedure, diagnosis, drug, employer, and submission channel constants |
| `exporter.py` | `PLAN_TYPES`, `PLAN_LIMITS`, `PROCEDURE_CODES_KES`, `DIAGNOSIS_CODES_KENYA`, `ICD10_CLINICAL_CATEGORIES`, `DRUG_FORMULARY` |
| `main.py` | `FraudPattern`, `GLOBAL_SEED`, `KES_THRESHOLDS`, `THRESHOLD_LOWER_BOUND` |

---

## Prerequisites

Only Python's standard library is needed:

```python
from __future__ import annotations  # type system — explained fully below
from enum import Enum               # powers FraudPattern
from typing import Any, TypedDict   # powers PlanInfo, ProcedureInfo
```

No third-party packages. No database. No I/O of any kind. This file is safe to import in any environment.

---

## The type system in this file

Three Python type-system tools appear here. They are worth understanding before reading the constants, because they show up throughout the project and each one carries a specific design intention.

### `from __future__ import annotations`

This line sits at the top of every file in the project. Its purpose is to enable modern type annotation syntax on older Python versions.

Python normally evaluates type annotations at the moment the code runs. This creates a problem called a *forward reference*: if an annotation refers to a class that hasn't been defined yet in the file, Python raises a `NameError`. It also means that syntax like `list[str]` and `str | None` — which are native only in Python 3.10 and above — would fail on 3.8 and 3.9.

The import fixes both problems at once:

```python
from __future__ import annotations
# After this, Python treats ALL annotations as plain strings.
# They are stored, not evaluated — resolved only if explicitly requested.
# This means:
#   list[str]   → works on Python 3.8+  (not just 3.9+)
#   str | None  → works on Python 3.8+  (not just 3.10+)
#   dict[str, float] → works everywhere
```

Because of this single line, every file in the project can use clean, modern annotation syntax regardless of the Python version it runs on. You will see `list[str]`, `dict[str, float]`, and `str | None` throughout the codebase — all of it enabled by this import.

### `TypedDict`

`TypedDict` lets you give a Python dictionary a named, typed shape. It is a type-checking tool only — at runtime, a `TypedDict` is still just an ordinary `dict`.

```python
class PlanInfo(TypedDict):
    name: str
    premium: int
    weight: float
```

What this buys you: any tool that checks types (like `mypy` or Pyright in your editor) will now flag mistakes when accessing `PLAN_TYPES` dictionaries. Accessing a key that doesn't exist or treating a value as the wrong type becomes an error before the code runs, not after.

```python
# Without TypedDict — both bugs are invisible until runtime:
PLAN_TYPES["NAT-001"]["premimum"]        # KeyError at runtime
PLAN_TYPES["NAT-001"]["premium"] + "x"  # TypeError at runtime

# With TypedDict — caught at the type-checking stage:
PLAN_TYPES["NAT-001"]["premimum"]        # type checker: no key 'premimum' in PlanInfo
PLAN_TYPES["NAT-001"]["premium"] + "x"  # type checker: int + str is invalid
```

At runtime, nothing changes. The value is still a `dict`:

```python
val: PlanInfo = {"name": "National Scheme", "premium": 500, "weight": 0.45}
type(val)             # <class 'dict'>
isinstance(val, dict) # True
```

`config.py` uses `TypedDict` for `PlanInfo` (the shape of each entry in `PLAN_TYPES`) and `ProcedureInfo` (the shape of each entry in `PROCEDURE_CODES_KES`). Both are accessed by key in multiple files, so the type contract is genuinely useful.

### `Enum`

`Enum` creates a closed set of named constants. Each member has two identities: a Python attribute name and a stored value.

```python
class FraudPattern(Enum):
    PHANTOM_BILLING = "phantom_billing"
```

You use it like this:

```python
FraudPattern.PHANTOM_BILLING          # the Enum member
FraudPattern.PHANTOM_BILLING.value    # "phantom_billing" — the string written to the database
FraudPattern.PHANTOM_BILLING.name     # "PHANTOM_BILLING" — the Python attribute name

# Iterate all four:
for p in FraudPattern:
    print(p.name, "→", p.value)
```

The critical difference from a plain string constant:

```python
# Plain string — typo is a silent bug:
fraud_type = "phantom_billng"          # no error, wrong value everywhere it propagates

# Enum — typo is an immediate error:
fraud_type = FraudPattern.PHANTOM_BILLNG  # AttributeError: raised immediately
```

In `patterns.py` and `generator.py`, `FraudPattern` members appear in conditional branches and dictionary values. A silently misspelled fraud type would result in claims being mislabelled in the training data — a hard-to-trace data quality problem that would corrupt the ML model. The `Enum` makes that class of error impossible.

---

## Constants, section by section

The 29 constants appear below in the order they are defined in the file.

---

### `FraudPattern` — lines 18–31

```python
class FraudPattern(Enum):
    PHANTOM_BILLING        = "phantom_billing"
    THRESHOLD_MANIPULATION = "threshold_manip"
    BENEFIT_SHARING        = "benefit_sharing"
    DUPLICATE_CLAIM        = "duplicate_claim"
```

This is the project's core vocabulary. These four values are what every fraud-related decision in the codebase references. The `.value` strings are what appear in the `fraud_type` column of `raw.fraud_investigation_outcomes` in the database.

To understand what each pattern *is*, you need to understand the signal it produces in the data — because the ML model never sees a `is_fraud` flag. It only sees features derived from the relationships between tables. Each pattern is designed to leave a specific detectable trace:

| Member | `.value` in DB | What makes it detectable in the data |
|---|---|---|
| `PHANTOM_BILLING` | `"phantom_billing"` | A consultation claim exists, but `raw.vitals` has no row for it *and* `raw.prescriptions` has no row for it. The patient visit was never real. |
| `THRESHOLD_MANIPULATION` | `"threshold_manip"` | The `amount_claimed` falls between 92% and 99% of a known SHA auto-approval threshold. The fraudster stays just below the amount that would trigger human review. |
| `BENEFIT_SHARING` | `"benefit_sharing"` | The same `member_id` appears at 4 or more distinct `provider_id` values within 7 days, all with the same `diagnosis_code`. Multiple clinics are all billing for visits that did not happen. |
| `DUPLICATE_CLAIM` | `"duplicate_claim"` | Two `raw.claims` rows share the same `(member_id, provider_id, diagnosis_code)` within 48 hours, with amounts differing by less than 5%. The same claim was submitted twice with a tiny amount variation to evade exact-duplicate checks. |

These four patterns were specified in the project proposal. The code enforces them exactly.

**Used in:** `patterns.py` (7 times), `generator.py` (12 times), `main.py` (5 times).

---

### `KES_THRESHOLDS`, `THRESHOLD_LOWER_BOUND`, `THRESHOLD_UPPER_BOUND` — lines 38–42

```python
KES_THRESHOLDS: list[int] = [5_000, 10_000, 25_000]
THRESHOLD_LOWER_BOUND = 0.92
THRESHOLD_UPPER_BOUND = 0.99
```

The SHA (Social Health Authority) automatically approves claims below KES 5,000, KES 10,000, and KES 25,000 without assigning them to a human reviewer. The `THRESHOLD_MANIPULATION` fraud pattern exploits this: by submitting a claim just below one of these values, a fraudster guarantees it is never looked at.

The two bound constants define the manipulation window — the range of amounts that are suspicious rather than just legitimately small:

| Threshold | Manipulation window (92%–99%) |
|---|---|
| KES 5,000 | KES 4,600 – KES 4,950 |
| KES 10,000 | KES 9,200 – KES 9,900 |
| KES 25,000 | KES 23,000 – KES 24,750 |

Why 92% as the floor? Anything above 92% is close enough to the threshold to be suspicious. Anything below would look like a routine low-value claim and would not cluster in a way the model can learn from.

The underscores in `5_000` are Python's numeric literal separator — `5_000 == 5000` is `True`. They exist only for readability.

**Used in:** `patterns.py` (manipulating amounts), `main.py` (verifying generated amounts are in range during post-generation checks).

---

### `KENYA_COUNTIES` — lines 48–96

```python
KENYA_COUNTIES: list[str]  # 47 items — all counties of Kenya
```

All 47 Kenyan counties. Assigned to members and providers as their `county` field. The `Faker` library then generates a matching sub-county.

Geography is not probability-weighted here — counties are sampled uniformly. In a production fraud model, geographic weighting by population would matter. For synthetic training data, uniform sampling avoids introducing population-bias artifacts into the ML features.

**Used in:** `generator.py` (4 times — initialising members and providers).

---

### `KENYA_FACILITY_TYPES` — lines 102–109

```python
KENYA_FACILITY_TYPES: dict[str, float] = {
    "Level 6 Hospital (National Referral)": 0.02,
    "Level 5 Hospital (County Referral)":   0.05,
    "Level 4 Hospital (Sub-county)":        0.15,
    "Level 3 Health Centre":                0.25,
    "Level 2 Dispensary":                   0.35,
    "Level 1 Clinic (Private/Community)":   0.18,
}
```

Kenya's Ministry of Health classifies all health facilities on a six-level scale. The `float` values are probability weights used by `generator.py` when creating providers:

```python
random.choices(list(KENYA_FACILITY_TYPES.keys()), weights=list(KENYA_FACILITY_TYPES.values()))
```

The weights deliberately reflect the real Kenyan health landscape: the majority of facilities (35%) are Level 2 dispensaries and village clinics; national referral hospitals (Level 6) are rare at 2%.

**Used in:** `generator.py` (3 times — when building the provider population).

---

### `FACILITY_PROCEDURE_PREFIXES` — lines 113–119

```python
FACILITY_PROCEDURE_PREFIXES: dict[str, tuple[str, ...]] = {
    "Level 1": ("CONS", "LAB"),
    "Level 2": ("CONS", "LAB", "RAD"),
    "Level 3": ("CONS", "LAB", "RAD", "MAT", "DENT", "OPT", "PROC-001", "PROC-002"),
    "Level 4": ("CONS", "LAB", "RAD", "MAT", "DENT", "OPT", "PROC"),
    # Level 5 and 6 get everything
}
```

Each entry maps a facility level prefix to the procedure code prefixes that facility is allowed to bill. Level 1 clinics can only do consultations and basic lab tests. Level 4 hospitals can do everything up to major surgical procedures. Level 5 and Level 6 are handled by an `else` branch in `generator.py` that allows all procedures.

This constraint prevents a village dispensary from billing a KES 12,000 CT scan (`RAD-005`) or a KES 40,000 appendectomy (`PROC-007`). Those combinations would be obvious synthetic artifacts and would teach the ML model false patterns.

The tuple type (`tuple[str, ...]`) signals that these are fixed sequences — ordered, immutable, not to be modified at runtime.

**Used in:** `generator.py` (3 times — to filter available procedure codes per provider).

---

### `ID_TYPES` — line 125

```python
ID_TYPES: list[str] = ["National ID", "Passport", "Alien ID", "Military ID"]
```

The four national identity document types issued in Kenya. Populates the `id_type` column on each `Member` row.

**Used in:** `generator.py` (2 times).

---

### `PlanInfo` and `PLAN_TYPES` — lines 133–148

```python
class PlanInfo(TypedDict):
    name: str
    premium: int
    weight: float

PLAN_TYPES: dict[str, PlanInfo] = {
    "NAT-001": {"name": "National Scheme",          "premium": 500, "weight": 0.45},
    "CS-001":  {"name": "Civil Service",            "premium": 650, "weight": 0.15},
    "CG-001":  {"name": "County Government",        "premium": 600, "weight": 0.10},
    "POL-001": {"name": "Police Force",             "premium": 550, "weight": 0.05},
    "MIL-001": {"name": "Armed Forces",             "premium": 550, "weight": 0.03},
    "HISP-001":{"name": "Health Insurance Subsidy", "premium":   0, "weight": 0.12},
    "EDU-001": {"name": "Educated Programme",       "premium": 300, "weight": 0.05},
    "RET-001": {"name": "Retail/Civilian",          "premium": 550, "weight": 0.05},
}
```

Eight insurance plans, each keyed by a plan code. The `weight` field is the enrollment selection probability — when `generator.py` creates a new member, it calls `random.choices()` with these weights to assign a plan. The weights sum to exactly 1.0.

The most important detail: `PLAN_TYPES` stores identity and enrollment weight. The financial limits (annual cap, inpatient cap, etc.) are stored separately in `PLAN_LIMITS`, described below. The same plan code keys both dictionaries, and `generator.py` merges them when building `Plan` dataclass instances.

**Used in:** `generator.py` (5 times — member assignment and plan row generation), `exporter.py` (1 time — plan CSV export).

---

### `ProcedureInfo` and `PROCEDURE_CODES_KES` — lines 155–218

```python
class ProcedureInfo(TypedDict):
    desc: str
    base_cost: int
    category: str

PROCEDURE_CODES_KES: dict[str, ProcedureInfo]  # 33 entries
```

33 procedure codes from the SHA's ~5,000-code official fee schedule, across eight clinical categories:

| Category | Codes | Cost range (KES) |
|---|---|---|
| Consultation | `CONS-001` – `CONS-005` | 350 – 2,000 |
| Laboratory | `LAB-001` – `LAB-008` | 200 – 1,500 |
| Radiology | `RAD-001` – `RAD-006` | 1,000 – 25,000 |
| Procedure (surgical) | `PROC-001` – `PROC-007` | 1,500 – 40,000 |
| Maternity | `MAT-001` – `MAT-002` | 350 – 450 |
| Dental | `DENT-001` – `DENT-003` | 800 – 2,500 |
| Optical | `OPT-001` – `OPT-002` | 500 – 3,500 |

The `base_cost` is the starting point for the `amount_claimed` on each generated claim. Fraud patterns then modify this: `THRESHOLD_MANIPULATION` overrides it entirely; `BENEFIT_SHARING` inflates it by 10–30%.

The consultation codes (`CONS-001` to `CONS-004`) escalate with facility level: `CONS-001` (KES 350) is for Level 2, `CONS-004` (KES 1,200) is for Level 5/6. A claim that mixes a Level 2 consultation code with a Level 5 referral hospital is a data anomaly the ML model can learn to detect.

**Used in:** `generator.py` (4 times — procedure assignment and seeding), `exporter.py` (2 times — procedure seed CSV).

---

### `DIAGNOSIS_CODES_KENYA` — lines 224–260

```python
DIAGNOSIS_CODES_KENYA: dict[str, str]  # 27 entries: ICD-10 code → description
```

27 ICD-10 diagnosis codes representing the most prevalent conditions in Kenya's health system:

| Group | Codes | Conditions |
|---|---|---|
| Infectious | `A01.0`, `A09`, `A15.0`, `B50.0`, `B54`, `B20` | Typhoid, TB, Malaria, HIV |
| Respiratory | `J02.9`, `J03.9`, `J06.9`, `J15.9`, `J45.9` | Pharyngitis, Pneumonia, Asthma |
| Cardiovascular | `I10`, `I20.9`, `I50.9` | Hypertension, Angina, Heart failure |
| Endocrine | `E10.9`, `E11.9` | Type 1 and 2 Diabetes |
| Maternal | `O20.9`, `O80.0`, `O82.0` | Pregnancy complications, Delivery |
| Injury | `S00.9`, `S52.9`, `S82.0` | Head injury, Fractures |
| Nutritional | `E44.0`, `E44.1` | Malnutrition |
| Other NCDs | `N39.0`, `M54.5`, `K29.7` | UTI, Back pain, Gastritis |

These codes populate the `diagnosis_code` column of `raw.claims`, which is a foreign key to `raw.icd10_codes`. The `exporter.py` writes this dictionary directly to `seeds/diagnosis_code_lookup.csv`.

**Used in:** `generator.py` (2 times — assigning diagnoses to claims), `exporter.py` (2 times — seed CSV generation).

---

### `BENEFIT_SHARING_ACTOR_COUNT`, `PHANTOM_PROVIDER_COUNT`, `DUPLICATE_PAIR_COUNT` — lines 267–273

```python
BENEFIT_SHARING_ACTOR_COUNT = 3
PHANTOM_PROVIDER_COUNT      = 3
DUPLICATE_PAIR_COUNT        = 12
```

These three integers are the fraud injection budget. They control exactly how much of each fraud pattern gets embedded into the dataset:

- **3 members** are pre-designated at generator startup as benefit-sharing actors. Each will have a cluster of claims at 4+ providers injected into their history.
- **3 providers** are pre-designated as phantom billers. Their consultation claims will have vitals and prescriptions suppressed probabilistically.
- **12 duplicate pairs** are created in post-processing: 12 original claims each get a near-identical duplicate injected alongside them.

At prototype scale (~1,460 total claims), these numbers produce a fraud rate of roughly 2–5%, which intentionally mirrors real-world conditions. Training a model on 50% fraud data produces a model that over-detects fraud in production.

**Used in:** `patterns.py` (5 times each — in `FraudPatternEngine.__init__()` and the injection methods).

---

### `GLOBAL_SEED` — line 279

```python
GLOBAL_SEED = 42
```

The random seed applied to Python's `random` module, NumPy, and the `Faker` library. It is set exactly once, in `main.py`, before any data is generated. No other module sets or changes it.

This makes the entire dataset **reproducible**: running the generator twice with the same seed produces byte-for-byte identical CSVs. That reproducibility matters for ML experiments — you need to be able to regenerate the exact same training data and get the same model results. It also makes debugging deterministic: any specific claim in the output can be reproduced by re-running with the same seed.

The value 42 is conventional in data science but arbitrary.

**Used in:** `main.py` (3 times — seeding `random`, `numpy.random`, and `Faker`).

---

### `EMPLOYER_INDUSTRIES`, `EMPLOYER_COUNT`, `COMPANY_PREFIX` — lines 285–302

```python
EMPLOYER_INDUSTRIES: list[str]  # 12 industries
EMPLOYER_COUNT = 30
COMPANY_PREFIX = ["Ltd", "Kenya", "Group", "Corp"]
```

12 industry sectors and a target count of 30 define the employer population. `generator.py` uses `Faker` for realistic Kenyan company names.

One honest note: `COMPANY_PREFIX` is defined here but is **not currently imported by any other module**. It documents intended behaviour — company names would be constructed as `Faker().company() + " " + random.choice(COMPANY_PREFIX)` to produce names like "Kariuki Logistics Ltd" — but the generator currently uses `Faker` for name generation without this suffix. It exists as documented intent, not active logic.

**Used in:** `generator.py` — `EMPLOYER_INDUSTRIES` (2 times), `EMPLOYER_COUNT` (2 times). `COMPANY_PREFIX` is not imported externally.

---

### `PLAN_LIMITS` — lines 309–366

```python
PLAN_LIMITS: dict[str, dict[str, Any]]  # 8 entries, one per plan_code
```

Financial parameters for each plan, keyed by the same plan codes used in `PLAN_TYPES`:

| Plan code | Tier | Annual limit | Inpatient | Outpatient | Family |
|---|---|---|---|---|---|
| `NAT-001` | basic | KES 100,000 | 60,000 | 40,000 | No |
| `CS-001` | premium | KES 300,000 | 200,000 | 100,000 | Yes |
| `CG-001` | standard | KES 200,000 | 130,000 | 70,000 | Yes |
| `POL-001` | standard | KES 200,000 | 130,000 | 70,000 | Yes |
| `MIL-001` | premium | KES 300,000 | 200,000 | 100,000 | Yes |
| `HISP-001` | basic | KES 50,000 | 30,000 | 20,000 | No |
| `EDU-001` | basic | KES 80,000 | 50,000 | 30,000 | No |
| `RET-001` | standard | KES 150,000 | 100,000 | 50,000 | No |

`PLAN_LIMITS` uses `dict[str, Any]` as its type rather than a `TypedDict` because the values include mixed types (strings for tier, ints for limits, bool for family). This is the only place in `config.py` where `Any` is used — a trade-off of type safety for practical convenience.

`generator.py` merges `PLAN_TYPES` and `PLAN_LIMITS` by plan code to build each `Plan` dataclass instance. The split between the two dictionaries reflects a real-world pattern: plan identity and financial parameters often come from different source systems and are maintained separately.

**Used in:** `generator.py` (3 times — building `Plan` rows), `exporter.py` (1 time — plan CSV export).

---

### `DRUG_FORMULARY` — lines 373–595

```python
DRUG_FORMULARY: dict[str, dict[str, Any]]  # 30 entries
```

30 SHA-approved drugs, each with a code, name, drug class, essential medicine flag, controlled substance flag, and unit cost in KES:

| Class | Count | Cost range (KES) |
|---|---|---|
| Antimalarial | 3 | 20 – 180 |
| Antibiotic | 4 | 12 – 95 |
| Analgesic | 4 | 5 – 120 |
| Antihypertensive | 3 | 8 – 30 |
| Antidiabetic | 3 | 15 – 850 |
| Antiretroviral | 2 | 280 – 350 |
| Bronchodilator | 2 | 320 – 480 |
| Supplement | 3 | 8 – 35 |
| Antifungal | 2 | 75 – 85 |
| Anti-inflammatory | 2 | 12 – 28 |
| Anthelminthic | 2 | 25 – 30 |

Drug codes follow the pattern `DRG-{CLASS_PREFIX}-{NUMBER}`: `DRG-AM-001` (Antimalarial 1), `DRG-AB-003` (Antibiotic 3), and so on. The prefix abbreviations are: `AM` = Antimalarial, `AB` = Antibiotic, `AN` = Analgesic, `AH` = Antihypertensive, `AD` = Antidiabetic, `AR` = Antiretroviral, `BD` = Bronchodilator, `SU` = Supplement, `AF` = Antifungal, `AI` = Anti-inflammatory, `AE` = Anthelminthic.

The single drug with `"controlled": True` is **Morphine Sulfate 10mg** (`DRG-AN-004`). This flag matters beyond generation: `db_tests.sql` Section 9 enforces that any prescription for a controlled substance must have `duration_days ≤ 30`, which is the actual clinical governance rule in Kenya.

**Used in:** `generator.py` (6 times — building prescriptions and drug formulary rows), `exporter.py` (1 time — drug formulary CSV export).

---

### `DIAGNOSIS_DRUG_CLASSES` — lines 598–626

```python
DIAGNOSIS_DRUG_CLASSES: dict[str, list[str]]  # 27 entries
```

This dictionary bridges diagnosis to prescription. For each ICD-10 code, it lists the drug classes that are clinically appropriate:

```python
"B50.0": ["Antimalarial", "Analgesic"],   # Malaria → treat infection + manage pain
"I10":   ["Antihypertensive"],             # Hypertension → blood pressure medication only
"B20":   ["Antiretroviral", "Antibiotic"], # HIV → ARVs + prophylactic antibiotics
"J45.9": ["Bronchodilator", "Anti-inflammatory"],  # Asthma → airway + inflammation
```

The generator uses this mapping in `_build_prescription()`:

1. Take the claim's `diagnosis_code`
2. Look up `DIAGNOSIS_DRUG_CLASSES[diagnosis_code]` → list of appropriate drug classes
3. Filter `DRUG_FORMULARY` to entries matching those classes
4. Pick one drug from the filtered list at random

Without this mapping, a malaria patient could be prescribed Metformin (a diabetes drug). That clinically incoherent combination would be an obvious synthetic artifact — the ML model might learn to detect "malaria + Metformin" as fraud rather than learning the actual fraud patterns. This dictionary prevents that.

**Used in:** `generator.py` (2 times — in `_build_prescription()`).

---

### `ICD10_CLINICAL_CATEGORIES` — lines 632–660

```python
ICD10_CLINICAL_CATEGORIES: dict[str, str]  # 27 entries: ICD-10 code → category name
```

Maps each of the 27 diagnosis codes to a broader clinical category: `"Infectious Disease"`, `"Respiratory"`, `"Cardiovascular"`, `"Endocrine"`, `"Maternal Health"`, `"Injury/Trauma"`, `"Nutritional"`, `"Urological"`, `"Musculoskeletal"`, `"Gastrointestinal"`.

These categories populate the `clinical_category` column of `raw.icd10_codes`, written to `seeds/diagnosis_code_lookup.csv` by `exporter.py`. Downstream dbt models use them to group claims by clinical domain for aggregate fraud rate analysis.

**Used in:** `generator.py` (1 time — ICD-10 reference row construction), `exporter.py` (2 times — seed CSV generation).

---

### `CLAIM_STATUS_WEIGHTS` — lines 668–675

```python
CLAIM_STATUS_WEIGHTS: dict[str, float] = {
    "submitted":          0.30,
    "pending_review":     0.25,
    "approved":           0.30,
    "partially_approved": 0.05,
    "rejected":           0.05,
    "paid":               0.05,
}
```

Intended probability weights for initial claim status at submission. The six values sum to 1.0.

This constant is defined here but is **not currently imported by any other module**. The generator assigns claim statuses using its own internal logic tied to claim amounts and fraud flags. The dictionary documents the intended distribution of claim statuses across the dataset and is available for future use or to validate the generator's actual output.

**Not imported externally.**

---

### `SUBMISSION_CHANNELS` and `SUBMISSION_CHANNEL_WEIGHTS` — lines 677–678

```python
SUBMISSION_CHANNELS: list[str]         = ["portal", "mobile", "api", "paper"]
SUBMISSION_CHANNEL_WEIGHTS: list[float] = [0.45, 0.30, 0.20, 0.05]
```

Two parallel lists designed to be passed directly to `random.choices()`:

```python
# How generator.py uses these:
channel = random.choices(SUBMISSION_CHANNELS, weights=SUBMISSION_CHANNEL_WEIGHTS, k=1)[0]
```

45% of claims arrive via the web portal, 30% via mobile app, 20% via API (bulk submission from provider EHR systems), 5% via paper. The paper figure is low because paper claims are expensive to process and Kenya's SHA system actively discourages them.

The `submission_channel` column on each claim is an ML feature: paper claims from a provider that normally uses the API is an anomaly worth investigating.

**Used in:** `generator.py` (2 times each).

---

### `STOCK_REPORTING_FACILITY_TYPES` and `STOCK_SNAPSHOT_INTERVAL_DAYS` — lines 685–693

```python
STOCK_REPORTING_FACILITY_TYPES: list[str] = [
    "Level 2 Dispensary",
    "Level 3 Health Centre",
    "Level 4 Hospital (Sub-county)",
    "Level 1 Clinic (Private/Community)",
]

STOCK_SNAPSHOT_INTERVAL_DAYS = 7
```

Only four facility types generate pharmacy stock snapshots. Level 5 and 6 hospitals are excluded — their inventory tracking is more complex and managed outside this simulation.

`STOCK_SNAPSHOT_INTERVAL_DAYS = 7` means one stock record per drug per reporting facility per week. The daily generator loop checks:

```python
if day_number % STOCK_SNAPSHOT_INTERVAL_DAYS == 0:
    snapshots = self._build_stock_snapshots(target_date)
```

**Used in:** `generator.py` (2 times each).

---

## How to interact with this file

`config.py` is never run directly. You import from it:

```python
# Inspect fraud pattern names and their DB values
from data_generation.config import FraudPattern
for p in FraudPattern:
    print(f"{p.name:30s} → stored in DB as: {p.value!r}")

# Check the manipulation bands
from data_generation.config import KES_THRESHOLDS, THRESHOLD_LOWER_BOUND, THRESHOLD_UPPER_BOUND
for t in KES_THRESHOLDS:
    lo, hi = t * THRESHOLD_LOWER_BOUND, t * THRESHOLD_UPPER_BOUND
    print(f"KES {t:>8,}: manipulation band  KES {lo:>8,.0f}  –  KES {hi:>8,.0f}")

# Confirm plan enrollment weights sum to 1.0
from data_generation.config import PLAN_TYPES
total = sum(p["weight"] for p in PLAN_TYPES.values())
print(f"Plan weights total: {total}")  # must be 1.0

# Count drugs per class
from data_generation.config import DRUG_FORMULARY
from collections import Counter
class_counts = Counter(d["class"] for d in DRUG_FORMULARY.values())
for cls, count in sorted(class_counts.items()):
    print(f"  {cls}: {count}")

# Verify all diagnosis codes have both a drug mapping and a clinical category
from data_generation.config import (
    DIAGNOSIS_CODES_KENYA, DIAGNOSIS_DRUG_CLASSES, ICD10_CLINICAL_CATEGORIES
)
codes = set(DIAGNOSIS_CODES_KENYA)
print("Codes missing drug mapping:    ", codes - set(DIAGNOSIS_DRUG_CLASSES))
print("Codes missing clinical category:", codes - set(ICD10_CLINICAL_CATEGORIES))
```

---

## Key design decisions

**One file for all constants.** The alternative — scattering constants across the modules that need them — creates hidden coupling. If `THRESHOLD_LOWER_BOUND` lived in `patterns.py`, someone writing a verification check in `main.py` would either have to import from `patterns.py` (coupling two unrelated concerns) or duplicate the value (creating drift). `config.py` makes the single source of truth explicit and structural, not just a convention.

**`Enum` for fraud patterns.** A plain string constant `PHANTOM_BILLING = "phantom_billing"` would function, but it cannot protect against typos. Across 12 usages of `FraudPattern` in `generator.py` alone, even a single misspelling would silently assign the wrong label to a fraud cluster, corrupting the training data in a way that would not raise any error and would be difficult to diagnose. `Enum` makes that impossible.

**`TypedDict` for `PlanInfo` and `ProcedureInfo`.** Both are accessed by key in multiple modules. Without `TypedDict`, a reader of `generator.py` would need to open `config.py` to know what keys are available and what types they hold. With `TypedDict`, a type-aware editor shows that information inline, at the point of use.

**`PLAN_TYPES` and `PLAN_LIMITS` are separate dictionaries.** Both contain plan data, both share the same keys, but they exist for different reasons. `PLAN_TYPES` is consulted at member-creation time to assign a plan probabilistically. `PLAN_LIMITS` is consulted at export time to populate financial columns on `Plan` rows. Merging them would create a large dictionary accessed in different parts of the system for different purposes. The split reflects how insurance plan data is typically structured in production systems — identity metadata and financial parameters come from different source tables.

**`GLOBAL_SEED` as a named constant.** The number `42` appearing three times in `main.py` would be a magic number — something that could be changed in one place but not the others, breaking reproducibility silently. Naming it `GLOBAL_SEED = 42` in `config.py` makes the intent explicit, makes it easy to find, and makes it easy to change in a single, obvious location.

---

## What to read next

The next file in the reading order is [`models.py`](./models.md). Where `config.py` defines the *values* the system works with, `models.py` defines the *shapes* those values are packaged into before they reach the database. The two files together are the complete domain model of the project.

---

## Things worth highlighting

**The domain specificity is deliberate.** Every ICD-10 code, every SHA threshold, every facility level, every drug in the formulary reflects how Kenya's health insurance system actually operates. This is not cosmetic realism for its own sake — it matters for the ML model. A model trained on clinically coherent data learns to distinguish genuine anomalies from normal variation. A model trained on arbitrary codes has nothing real to learn from.

**The three-level prescription chain.** `DIAGNOSIS_CODES_KENYA` → `DIAGNOSIS_DRUG_CLASSES` → `DRUG_FORMULARY` is a pipeline: an ICD-10 code maps to appropriate drug classes, which map to specific approved drugs. This chain is what makes prescriptions in the generated data clinically plausible. The *absence* of a prescription for a diagnosis that requires medication then becomes a meaningful ML feature — not just a gap, but a signal.

**Two constants here are defined but not consumed externally.** `CLAIM_STATUS_WEIGHTS` and `COMPANY_PREFIX` are both defined in `config.py` but are not imported by any other module. Both document intended logic — one for claim status distribution, one for company name construction. Their presence is not an error; it is living documentation of future or alternative design. Anyone extending the generator knows where to look.

**The `PLAN_TYPES` weights must sum to 1.0.** This is a silent contract with `random.choices()`. If someone adds a new plan and forgets to rebalance the weights, member enrollment will be incorrectly distributed without any error being raised. It is worth running `sum(p["weight"] for p in PLAN_TYPES.values())` whenever the plan list changes.
