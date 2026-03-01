"""
Kenya-specific configuration and reference data for the AfyaBima generator.

All constants live here so other modules import from a single source of truth.
Procedure costs are in KES. Thresholds are the SHA auto-approval limits.
"""

from __future__ import annotations

from enum import Enum
from typing import Any, TypedDict

# ---------------------------------------------------------------------------
# Fraud patterns (exactly as defined in the proposal)
# ---------------------------------------------------------------------------


class FraudPattern(Enum):
    """Four fraud patterns specified in the AfyaBima proposal."""

    PHANTOM_BILLING = "phantom_billing"
    """Provider claims a consultation with no matching vitals or prescription."""

    THRESHOLD_MANIPULATION = "threshold_manip"
    """Billed amount sits 1-8 % below a SHA auto-approval threshold."""

    BENEFIT_SHARING = "benefit_sharing"
    """Same member visits 4+ distinct providers within 7 days, same diagnosis."""

    DUPLICATE_CLAIM = "duplicate_claim"
    """Same (member, provider, diagnosis) within 48 hours, < 5 % amount variance."""


# ---------------------------------------------------------------------------
# KES auto-approval thresholds (SHA manual)
# ---------------------------------------------------------------------------

KES_THRESHOLDS: list[int] = [5_000, 10_000, 25_000]

# Fraction below threshold that counts as manipulation (92 %-99 % of threshold)
THRESHOLD_LOWER_BOUND = 0.92
THRESHOLD_UPPER_BOUND = 0.99

# ---------------------------------------------------------------------------
# Kenyan geography
# ---------------------------------------------------------------------------

KENYA_COUNTIES: list[str] = [
    "Nairobi",
    "Mombasa",
    "Kisumu",
    "Nakuru",
    "Kiambu",
    "Uasin Gishu",
    "Kakamega",
    "Kilifi",
    "Machakos",
    "Meru",
    "Garissa",
    "Kisii",
    "Kitui",
    "Mandera",
    "Migori",
    "Bungoma",
    "Busia",
    "Elgeyo Marakwet",
    "Embu",
    "Homa Bay",
    "Isiolo",
    "Kajiado",
    "Kericho",
    "Kirinyaga",
    "Kwale",
    "Laikipia",
    "Lamu",
    "Makueni",
    "Marsabit",
    "Murang'a",
    "Nandi",
    "Narok",
    "Nyamira",
    "Nyandarua",
    "Nyeri",
    "Samburu",
    "Siaya",
    "Taita Taveta",
    "Tana River",
    "Tharaka Nithi",
    "Trans Nzoia",
    "Turkana",
    "Vihiga",
    "Wajir",
    "West Pokot",
    "Baringo",
    "Bomet",
]

# ---------------------------------------------------------------------------
# Facility types (Kenyan Ministry of Health classification)
# ---------------------------------------------------------------------------

KENYA_FACILITY_TYPES: dict[str, float] = {
    "Level 6 Hospital (National Referral)": 0.02,
    "Level 5 Hospital (County Referral)": 0.05,
    "Level 4 Hospital (Sub-county)": 0.15,
    "Level 3 Health Centre": 0.25,
    "Level 2 Dispensary": 0.35,
    "Level 1 Clinic (Private/Community)": 0.18,
}

# Procedure categories available per facility level prefix.
# Level 3 health centres are excluded from major surgical procedures.
FACILITY_PROCEDURE_PREFIXES: dict[str, tuple[str, ...]] = {
    "Level 1": ("CONS", "LAB"),
    "Level 2": ("CONS", "LAB", "RAD"),
    "Level 3": ("CONS", "LAB", "RAD", "MAT", "DENT", "OPT", "PROC-001", "PROC-002"),
    "Level 4": ("CONS", "LAB", "RAD", "MAT", "DENT", "OPT", "PROC"),
    # Level 5 and 6 get everything (handled by else-branch in generator)
}

# ---------------------------------------------------------------------------
# National ID types
# ---------------------------------------------------------------------------

ID_TYPES: list[str] = ["National ID", "Passport", "Alien ID", "Military ID"]

# ---------------------------------------------------------------------------
# SHA / NHIF plan types
# plan_code -> {name, monthly_premium_kes, selection_weight}
# ---------------------------------------------------------------------------


class PlanInfo(TypedDict):
    name: str
    premium: int
    weight: float


PLAN_TYPES: dict[str, PlanInfo] = {
    "NAT-001": {"name": "National Scheme", "premium": 500, "weight": 0.45},
    "CS-001": {"name": "Civil Service", "premium": 650, "weight": 0.15},
    "CG-001": {"name": "County Government", "premium": 600, "weight": 0.10},
    "POL-001": {"name": "Police Force", "premium": 550, "weight": 0.05},
    "MIL-001": {"name": "Armed Forces", "premium": 550, "weight": 0.03},
    "HISP-001": {"name": "Health Insurance Subsidy", "premium": 0, "weight": 0.12},
    "EDU-001": {"name": "Educated Programme", "premium": 300, "weight": 0.05},
    "RET-001": {"name": "Retail/Civilian", "premium": 550, "weight": 0.05},
}


# ---------------------------------------------------------------------------
# Procedure codes (representative subset of SHA ~5 000-code schedule)
# All costs in KES.
# ---------------------------------------------------------------------------
class ProcedureInfo(TypedDict):
    desc: str
    base_cost: int
    category: str


PROCEDURE_CODES_KES: dict[str, ProcedureInfo] = {
    # Consultations
    "CONS-001": {
        "desc": "General consultation - Level 2",
        "base_cost": 350,
        "category": "consultation",
    },
    "CONS-002": {
        "desc": "General consultation - Level 3",
        "base_cost": 550,
        "category": "consultation",
    },
    "CONS-003": {
        "desc": "General consultation - Level 4",
        "base_cost": 850,
        "category": "consultation",
    },
    "CONS-004": {
        "desc": "General consultation - Level 5/6",
        "base_cost": 1_200,
        "category": "consultation",
    },
    "CONS-005": {"desc": "Specialist consultation", "base_cost": 2_000, "category": "consultation"},
    # Laboratory
    "LAB-001": {"desc": "Malaria rapid test", "base_cost": 250, "category": "lab"},
    "LAB-002": {"desc": "Blood glucose", "base_cost": 200, "category": "lab"},
    "LAB-003": {"desc": "Urinalysis", "base_cost": 300, "category": "lab"},
    "LAB-004": {"desc": "Full hemogram", "base_cost": 600, "category": "lab"},
    "LAB-005": {"desc": "Liver function tests", "base_cost": 1_200, "category": "lab"},
    "LAB-006": {"desc": "Renal function tests", "base_cost": 1_200, "category": "lab"},
    "LAB-007": {"desc": "HIV test", "base_cost": 350, "category": "lab"},
    "LAB-008": {"desc": "COVID-19 test", "base_cost": 1_500, "category": "lab"},
    # Radiology
    "RAD-001": {"desc": "Chest X-ray", "base_cost": 1_200, "category": "radiology"},
    "RAD-002": {"desc": "Limb X-ray", "base_cost": 1_000, "category": "radiology"},
    "RAD-003": {"desc": "Abdominal ultrasound", "base_cost": 3_500, "category": "radiology"},
    "RAD-004": {"desc": "Obstetric ultrasound", "base_cost": 3_000, "category": "radiology"},
    "RAD-005": {"desc": "CT scan (single region)", "base_cost": 12_000, "category": "radiology"},
    "RAD-006": {"desc": "MRI (single region)", "base_cost": 25_000, "category": "radiology"},
    # Procedures
    "PROC-001": {"desc": "Wound suturing (minor)", "base_cost": 1_500, "category": "procedure"},
    "PROC-002": {"desc": "Incision and drainage", "base_cost": 2_000, "category": "procedure"},
    "PROC-003": {"desc": "Plaster cast application", "base_cost": 2_500, "category": "procedure"},
    "PROC-004": {"desc": "C-section delivery", "base_cost": 35_000, "category": "procedure"},
    "PROC-005": {"desc": "Normal delivery", "base_cost": 15_000, "category": "procedure"},
    "PROC-006": {"desc": "Hernia repair", "base_cost": 25_000, "category": "procedure"},
    "PROC-007": {"desc": "Appendectomy", "base_cost": 40_000, "category": "procedure"},
    # Maternity
    "MAT-001": {"desc": "Antenatal visit", "base_cost": 450, "category": "maternity"},
    "MAT-002": {"desc": "Postnatal visit", "base_cost": 350, "category": "maternity"},
    # Dental
    "DENT-001": {"desc": "Tooth extraction", "base_cost": 800, "category": "dental"},
    "DENT-002": {"desc": "Dental filling", "base_cost": 2_000, "category": "dental"},
    "DENT-003": {"desc": "Scaling and polishing", "base_cost": 2_500, "category": "dental"},
    # Optical
    "OPT-001": {"desc": "Eye examination", "base_cost": 500, "category": "optical"},
    "OPT-002": {"desc": "Prescription glasses", "base_cost": 3_500, "category": "optical"},
}

# ---------------------------------------------------------------------------
# ICD-10 codes common in Kenya
# ---------------------------------------------------------------------------

DIAGNOSIS_CODES_KENYA: dict[str, str] = {
    # Infectious
    "A01.0": "Typhoid fever",
    "A09": "Infectious gastroenteritis",
    "A15.0": "Tuberculosis of lung",
    "B50.0": "Plasmodium falciparum malaria",
    "B54": "Unspecified malaria",
    "B20": "HIV disease",
    # Respiratory
    "J02.9": "Acute pharyngitis",
    "J03.9": "Acute tonsillitis",
    "J06.9": "Acute URTI",
    "J15.9": "Bacterial pneumonia",
    "J45.9": "Asthma",
    # Cardiovascular
    "I10": "Essential hypertension",
    "I20.9": "Angina pectoris",
    "I50.9": "Heart failure",
    # Endocrine
    "E10.9": "Type 1 diabetes",
    "E11.9": "Type 2 diabetes",
    # Maternal
    "O20.9": "Early pregnancy hemorrhage",
    "O80.0": "Spontaneous vertex delivery",
    "O82.0": "Delivery by C-section",
    # Injuries
    "S00.9": "Superficial head injury",
    "S52.9": "Forearm fracture",
    "S82.0": "Patella fracture",
    # Malnutrition
    "E44.0": "Moderate malnutrition",
    "E44.1": "Mild malnutrition",
    # NCDs
    "N39.0": "UTI",
    "M54.5": "Lower back pain",
    "K29.7": "Gastritis",
}

# ---------------------------------------------------------------------------
# Fraud pattern counts (for pre-designated actors)
# ---------------------------------------------------------------------------

# How many members are designated as benefit-sharing actors
BENEFIT_SHARING_ACTOR_COUNT = 3

# Providers designated as phantom billers (index into sorted provider list)
PHANTOM_PROVIDER_COUNT = 3

# Exact duplicate pairs to create during post-processing
DUPLICATE_PAIR_COUNT = 12

# ---------------------------------------------------------------------------
# RNG seeds (set once in main; modules must not re-seed)
# ---------------------------------------------------------------------------

GLOBAL_SEED = 42

# ---------------------------------------------------------------------------
# Employer reference data
# ---------------------------------------------------------------------------

EMPLOYER_INDUSTRIES: list[str] = [
    "Government",
    "Healthcare",
    "Education",
    "Manufacturing",
    "Finance",
    "Telecommunications",
    "Agriculture",
    "NGO/Non-profit",
    "Retail",
    "Construction",
    "Transport",
    "Hospitality",
]

EMPLOYER_COUNT = 30  # Total employers to generate

COMPANY_PREFIX = ["Co.", "Inc.", "PLC", "LLP", "Group", "Corp"]

# ---------------------------------------------------------------------------
# Plan tier / financial limits (KES) — extends PLAN_TYPES with schema columns
# plan_code -> {tier, annual_limit, inpatient_limit, outpatient_limit, is_family}
# ---------------------------------------------------------------------------

PLAN_LIMITS: dict[str, dict[str, Any]] = {
    "NAT-001": {
        "tier": "basic",
        "annual": 100_000,
        "inpatient": 60_000,
        "outpatient": 40_000,
        "family": False,
    },
    "CS-001": {
        "tier": "premium",
        "annual": 300_000,
        "inpatient": 200_000,
        "outpatient": 100_000,
        "family": True,
    },
    "CG-001": {
        "tier": "standard",
        "annual": 200_000,
        "inpatient": 130_000,
        "outpatient": 70_000,
        "family": True,
    },
    "POL-001": {
        "tier": "standard",
        "annual": 200_000,
        "inpatient": 130_000,
        "outpatient": 70_000,
        "family": True,
    },
    "MIL-001": {
        "tier": "premium",
        "annual": 300_000,
        "inpatient": 200_000,
        "outpatient": 100_000,
        "family": True,
    },
    "HISP-001": {
        "tier": "basic",
        "annual": 50_000,
        "inpatient": 30_000,
        "outpatient": 20_000,
        "family": False,
    },
    "EDU-001": {
        "tier": "basic",
        "annual": 80_000,
        "inpatient": 50_000,
        "outpatient": 30_000,
        "family": False,
    },
    "RET-001": {
        "tier": "standard",
        "annual": 150_000,
        "inpatient": 100_000,
        "outpatient": 50_000,
        "family": False,
    },
}

# ---------------------------------------------------------------------------
# Drug formulary — representative SHA-approved drugs
# drug_code -> {name, drug_class, is_essential, is_controlled, unit_cost_kes}
# ---------------------------------------------------------------------------

DRUG_FORMULARY: dict[str, dict[str, Any]] = {
    # Antimalarials
    "DRG-AM-001": {
        "name": "Artemether/Lumefantrine 20/120mg",
        "class": "Antimalarial",
        "essential": True,
        "controlled": False,
        "cost": 180.0,
    },
    "DRG-AM-002": {
        "name": "Quinine Sulfate 300mg",
        "class": "Antimalarial",
        "essential": True,
        "controlled": False,
        "cost": 45.0,
    },
    "DRG-AM-003": {
        "name": "Chloroquine Phosphate 250mg",
        "class": "Antimalarial",
        "essential": True,
        "controlled": False,
        "cost": 20.0,
    },
    # Antibiotics
    "DRG-AB-001": {
        "name": "Amoxicillin 500mg",
        "class": "Antibiotic",
        "essential": True,
        "controlled": False,
        "cost": 15.0,
    },
    "DRG-AB-002": {
        "name": "Azithromycin 500mg",
        "class": "Antibiotic",
        "essential": True,
        "controlled": False,
        "cost": 95.0,
    },
    "DRG-AB-003": {
        "name": "Ciprofloxacin 500mg",
        "class": "Antibiotic",
        "essential": True,
        "controlled": False,
        "cost": 35.0,
    },
    "DRG-AB-004": {
        "name": "Metronidazole 400mg",
        "class": "Antibiotic",
        "essential": True,
        "controlled": False,
        "cost": 12.0,
    },
    # Analgesics
    "DRG-AN-001": {
        "name": "Paracetamol 500mg",
        "class": "Analgesic",
        "essential": True,
        "controlled": False,
        "cost": 5.0,
    },
    "DRG-AN-002": {
        "name": "Ibuprofen 400mg",
        "class": "Analgesic",
        "essential": True,
        "controlled": False,
        "cost": 18.0,
    },
    "DRG-AN-003": {
        "name": "Diclofenac 50mg",
        "class": "Analgesic",
        "essential": True,
        "controlled": False,
        "cost": 22.0,
    },
    "DRG-AN-004": {
        "name": "Morphine Sulfate 10mg",
        "class": "Analgesic",
        "essential": True,
        "controlled": True,
        "cost": 120.0,
    },
    # Antihypertensives
    "DRG-AH-001": {
        "name": "Amlodipine 5mg",
        "class": "Antihypertensive",
        "essential": True,
        "controlled": False,
        "cost": 25.0,
    },
    "DRG-AH-002": {
        "name": "Enalapril 10mg",
        "class": "Antihypertensive",
        "essential": True,
        "controlled": False,
        "cost": 30.0,
    },
    "DRG-AH-003": {
        "name": "Hydrochlorothiazide 25mg",
        "class": "Antihypertensive",
        "essential": True,
        "controlled": False,
        "cost": 8.0,
    },
    # Antidiabetics
    "DRG-AD-001": {
        "name": "Metformin 500mg",
        "class": "Antidiabetic",
        "essential": True,
        "controlled": False,
        "cost": 20.0,
    },
    "DRG-AD-002": {
        "name": "Glibenclamide 5mg",
        "class": "Antidiabetic",
        "essential": True,
        "controlled": False,
        "cost": 15.0,
    },
    "DRG-AD-003": {
        "name": "Insulin Regular 100IU/ml",
        "class": "Antidiabetic",
        "essential": True,
        "controlled": False,
        "cost": 850.0,
    },
    # Antiretrovirals
    "DRG-AR-001": {
        "name": "Tenofovir/Lamivudine/Efavirenz",
        "class": "Antiretroviral",
        "essential": True,
        "controlled": False,
        "cost": 350.0,
    },
    "DRG-AR-002": {
        "name": "Zidovudine/Lamivudine 300/150mg",
        "class": "Antiretroviral",
        "essential": True,
        "controlled": False,
        "cost": 280.0,
    },
    # Bronchodilators
    "DRG-BD-001": {
        "name": "Salbutamol Inhaler 100mcg",
        "class": "Bronchodilator",
        "essential": True,
        "controlled": False,
        "cost": 320.0,
    },
    "DRG-BD-002": {
        "name": "Beclomethasone Inhaler 250mcg",
        "class": "Bronchodilator",
        "essential": True,
        "controlled": False,
        "cost": 480.0,
    },
    # Supplements
    "DRG-SU-001": {
        "name": "Ferrous Sulfate 200mg",
        "class": "Iron supplement",
        "essential": True,
        "controlled": False,
        "cost": 10.0,
    },
    "DRG-SU-002": {
        "name": "Folic Acid 5mg",
        "class": "Vitamin supplement",
        "essential": True,
        "controlled": False,
        "cost": 8.0,
    },
    "DRG-SU-003": {
        "name": "Vitamin B Complex",
        "class": "Vitamin supplement",
        "essential": False,
        "controlled": False,
        "cost": 35.0,
    },
    # Antifungals
    "DRG-AF-001": {
        "name": "Fluconazole 150mg",
        "class": "Antifungal",
        "essential": True,
        "controlled": False,
        "cost": 75.0,
    },
    "DRG-AF-002": {
        "name": "Clotrimazole Cream 1%",
        "class": "Antifungal",
        "essential": True,
        "controlled": False,
        "cost": 85.0,
    },
    # Anti-inflammatory
    "DRG-AI-001": {
        "name": "Prednisolone 5mg",
        "class": "Anti-inflammatory",
        "essential": True,
        "controlled": False,
        "cost": 12.0,
    },
    "DRG-AI-002": {
        "name": "Dexamethasone 4mg",
        "class": "Anti-inflammatory",
        "essential": True,
        "controlled": False,
        "cost": 28.0,
    },
    # Anthelminthics
    "DRG-AE-001": {
        "name": "Albendazole 400mg",
        "class": "Anthelminthic",
        "essential": True,
        "controlled": False,
        "cost": 30.0,
    },
    "DRG-AE-002": {
        "name": "Mebendazole 500mg",
        "class": "Anthelminthic",
        "essential": True,
        "controlled": False,
        "cost": 25.0,
    },
}

# Diagnosis → most likely drug class(es) for realistic prescription matching
DIAGNOSIS_DRUG_CLASSES: dict[str, list[str]] = {
    "A01.0": ["Antibiotic", "Analgesic"],
    "A09": ["Antibiotic", "Analgesic"],
    "A15.0": ["Antibiotic"],
    "B50.0": ["Antimalarial", "Analgesic"],
    "B54": ["Antimalarial", "Analgesic"],
    "B20": ["Antiretroviral", "Antibiotic"],
    "J02.9": ["Antibiotic", "Analgesic"],
    "J03.9": ["Antibiotic", "Analgesic"],
    "J06.9": ["Antibiotic", "Analgesic"],
    "J15.9": ["Antibiotic", "Analgesic"],
    "J45.9": ["Bronchodilator", "Anti-inflammatory"],
    "I10": ["Antihypertensive"],
    "I20.9": ["Analgesic", "Antihypertensive"],
    "I50.9": ["Antihypertensive"],
    "E10.9": ["Antidiabetic"],
    "E11.9": ["Antidiabetic"],
    "O20.9": ["Iron supplement", "Analgesic"],
    "O80.0": ["Analgesic", "Iron supplement"],
    "O82.0": ["Analgesic", "Antibiotic"],
    "S00.9": ["Analgesic"],
    "S52.9": ["Analgesic", "Anti-inflammatory"],
    "S82.0": ["Analgesic", "Anti-inflammatory"],
    "E44.0": ["Vitamin supplement", "Iron supplement"],
    "E44.1": ["Vitamin supplement", "Iron supplement"],
    "N39.0": ["Antibiotic"],
    "M54.5": ["Analgesic", "Anti-inflammatory"],
    "K29.7": ["Analgesic", "Antibiotic"],
}

# ---------------------------------------------------------------------------
# ICD-10 clinical categories (for raw.icd10_codes)
# ---------------------------------------------------------------------------

ICD10_CLINICAL_CATEGORIES: dict[str, str] = {
    "A01.0": "Infectious Disease",
    "A09": "Infectious Disease",
    "A15.0": "Infectious Disease",
    "B50.0": "Infectious Disease",
    "B54": "Infectious Disease",
    "B20": "Infectious Disease",
    "J02.9": "Respiratory",
    "J03.9": "Respiratory",
    "J06.9": "Respiratory",
    "J15.9": "Respiratory",
    "J45.9": "Respiratory",
    "I10": "Cardiovascular",
    "I20.9": "Cardiovascular",
    "I50.9": "Cardiovascular",
    "E10.9": "Endocrine",
    "E11.9": "Endocrine",
    "O20.9": "Maternal Health",
    "O80.0": "Maternal Health",
    "O82.0": "Maternal Health",
    "S00.9": "Injury/Trauma",
    "S52.9": "Injury/Trauma",
    "S82.0": "Injury/Trauma",
    "E44.0": "Nutritional",
    "E44.1": "Nutritional",
    "N39.0": "Urological",
    "M54.5": "Musculoskeletal",
    "K29.7": "Gastrointestinal",
}

# ---------------------------------------------------------------------------
# Claim status transition logic
# ---------------------------------------------------------------------------

# Probability weights for initial claim status at submission
# (auto-approve rate depends on amount vs threshold)
CLAIM_STATUS_WEIGHTS: dict[str, float] = {
    "submitted": 0.30,
    "pending_review": 0.25,
    "approved": 0.30,
    "partially_approved": 0.05,
    "rejected": 0.05,
    "paid": 0.05,
}

SUBMISSION_CHANNELS: list[str] = ["portal", "mobile", "api", "paper"]
SUBMISSION_CHANNEL_WEIGHTS: list[float] = [0.45, 0.30, 0.20, 0.05]

# ---------------------------------------------------------------------------
# Stock level configuration
# ---------------------------------------------------------------------------

# How many providers generate stock snapshots (dispensaries + clinics)
STOCK_REPORTING_FACILITY_TYPES: list[str] = [
    "Level 2 Dispensary",
    "Level 3 Health Centre",
    "Level 4 Hospital (Sub-county)",
    "Level 1 Clinic (Private/Community)",
]

# Snapshot frequency: generate one stock record per reporting day interval
STOCK_SNAPSHOT_INTERVAL_DAYS = 7  # weekly snapshots
