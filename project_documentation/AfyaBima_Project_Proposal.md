# AfyaBima

## End-to-End Health Insurance Data Platform with Real-Time Fraud Intelligence

> *"Afya" — Swahili for health. "Bima" — Swahili for insurance.*
> A modern data platform built for health insurance operations in emerging markets.

---

**Document Type:** Project Proposal and Technical Architecture
**Version:** 1.0 — 2026
**Audience:** Executive Leadership · Product and Engineering Teams · Technical Partners
**Status:** Approved for Development

---

## Table of Contents

1. [Executive Summary](#1-executive-summary)
2. [Background and Problem Statement](#2-background-and-problem-statement)
3. [What AfyaBima Does](#3-what-afyabima-does)
4. [Who This Is Built For](#4-who-this-is-built-for)
5. [System Architecture](#5-system-architecture)
6. [Database Schema Design](#6-database-schema-design)
7. [Technology Stack and Rationale](#7-technology-stack-and-rationale)
8. [Implementation Phases](#8-implementation-phases)
9. [Data Generation Strategy](#9-data-generation-strategy)
10. [Analytics and Reporting Layer](#10-analytics-and-reporting-layer)
11. [Machine Learning and MLOps Pipeline](#11-machine-learning-and-mlops-pipeline)
12. [API Serving Layer](#12-api-serving-layer)
13. [Future Expansion](#13-future-expansion)
14. [Risks and Mitigations](#14-risks-and-mitigations)
15. [Appendix — Skill Coverage Map](#15-appendix--skill-coverage-map)

---

## 1. Executive Summary

AfyaBima is a production-grade, cloud-native data platform purpose-built for health insurance operations in emerging markets. It ingests claims data from multiple operational systems in real time, transforms it into reliable analytics, detects and scores fraudulent claims before they are approved, and exposes everything through a clean API for investigators, analysts, and downstream consumers.

The platform is end-to-end by design. It covers every stage of the data lifecycle — from raw event ingestion through streaming infrastructure, batch transformation, analytical reporting, machine learning, and a REST API serving layer. It is deployed on AWS using Kubernetes, orchestrated with Airflow, and built to the engineering standard expected in a production insurance operation.

While AfyaBima is a proof of concept, every component is production-ready: tested, documented, version-controlled, and observable. It demonstrates the full range of capabilities expected of a data generalist — analytics engineering, data pipeline design, real-time streaming, MLOps, API development, and cloud infrastructure.

**The platform does five things well:**

- Collects and unifies claims data from multiple microservices through a real-time event streaming backbone
- Transforms raw data into clean, tested, lineage-documented analytical assets using dbt
- Produces business intelligence dashboards and operational reports covering claims performance, provider analytics, member utilisation, and financial exposure
- Detects fraudulent claims in real time using stateful pattern detection and a trained machine learning model, scoring each claim before the approval decision is made
- Closes the loop — investigator verdicts flow back through the pipeline, retraining the model weekly so detection improves over time

---

## 2. Background and Problem Statement

### 2.1 The Health Insurance Data Gap

Health insurance in Sub-Saharan Africa and South Asia is growing fast. Employer-sponsored schemes, national health coverage programmes, and mobile-first micro-insurance products have brought millions of new members into formal insurance in the last decade. The operations processing their claims, however, have not kept pace.

Most insurers in these markets are still running on fragmented systems — a claims management platform here, a member registration portal there, a provider portal that does not talk to either, and a finance system that reconciles everything in Excel at month-end. Data is siloed, stale, and manually assembled. Decisions about approval rates, provider performance, member utilisation, and financial exposure are made on reports that are days or weeks old, assembled by hand, and often inconsistent with each other.

The consequences are real and measurable:

- Claims that should be approved are delayed because no analyst has time to assemble the relevant context
- Claims that should be rejected are approved because no system is watching the patterns
- Providers are paid without anyone knowing their approval rate, their average claim amount relative to peers, or whether their claim frequency has spiked in the last 48 hours
- Members exhaust their annual limits without the system surfacing this to their case manager
- Finance closes the month with numbers that do not match the claims system because nobody noticed three days of missing data

### 2.2 The Fraud Problem Specifically

Health insurance fraud accounts for an estimated 5 to 10 percent of total claim expenditure across most markets. Unlike credit card fraud — where a cardholder disputes a transaction within days — health insurance fraud surfaces weeks or months after a claim is paid, and only after manual investigation. By the time the pattern is visible, money has left the fund and recovery is difficult.

The deeper problem is structural. Fraud detection systems built on batch analysis are retrospective by nature. They identify abuse after payment. Even when patterns are found, the investigation team is working through a backlog with no prioritisation system, no model to guide their attention to the highest-risk cases first, and no mechanism to feed their findings back into detection.

AfyaBima addresses both problems — the general data quality and analytics gap, and the specific fraud detection challenge — within a single, coherent platform.

### 2.3 Why a Platform, Not a Point Solution

A fraud scoring model without reliable data infrastructure is worthless. A data warehouse without real-time ingestion produces stale analytics. An analytics layer without a feedback loop produces insights that never improve.

The decision to build AfyaBima as an end-to-end platform rather than a collection of point solutions is deliberate. Each layer depends on the others:

- The ML model depends on clean, tested features produced by the transformation layer
- The transformation layer depends on reliable, structured data arriving from the streaming layer
- The streaming layer depends on a well-designed event schema agreed across microservices
- The feedback loop that makes the model improve over time depends on investigators using the API, whose experience depends on the analytics layer surfacing the right context

Building any one of these in isolation produces a component that works in a demo but fails in production. AfyaBima is designed as a system, not a collection of scripts.

---

## 3. What AfyaBima Does

At the operational level, AfyaBima does five things:

### 3.1 Real-Time Claims Ingestion

Every claim event — submission, status update, payment, investigation outcome — is published to a Kafka event bus the moment it occurs. Multiple microservices (claims submission portal, member portal, provider portal, investigator portal) publish events independently. AfyaBima consumes them all, validates their schemas, persists them to PostgreSQL, and archives them to S3 simultaneously.

No microservice writes directly to the database. The event bus is the single point of entry, which means every event is captured, every event is replayable, and no event is lost if any downstream system is temporarily unavailable.

### 3.2 Batch Transformation and Analytics

A dbt project running daily via Airflow transforms raw ingested data into clean, tested, documented analytical assets. Staging models clean and standardise raw data. Intermediate models apply business logic and join related entities. Mart models produce the wide, denormalised tables that power dashboards, reports, and the API.

The transformation layer covers the full claims lifecycle: what was claimed, what was approved, how long it took, which providers are performing above or below their peers, which members are approaching their annual limits, and where the financial exposure is concentrated.

### 3.3 Fraud Detection

Apache Flink consumes the real-time claims stream, maintains stateful windows per provider and per member, and detects anomaly patterns that indicate fraudulent activity. Each incoming claim is also scored by a trained machine learning model before the adjudication decision is made.

The fraud layer operates in two modes simultaneously. Rule-based detection catches known patterns immediately — a provider submitting 40 claims in 6 hours, amounts clustering just below approval thresholds, the same member appearing at five different providers in a week. The ML model catches the subtler patterns that rules cannot express — the combination of signals that individually look normal but together indicate something is wrong.

### 3.4 Investigation Workflow Support

The FastAPI serving layer exposes endpoints that support the investigation team directly. Investigators can query claims by risk tier, view the feature context behind each fraud prediction, submit verdicts, and track the status of open investigations. Every verdict is persisted and — critically — fed back into the model retraining pipeline.

### 3.5 Operational Reporting

Beyond fraud, AfyaBima produces the operational reports that an insurance finance and operations team needs: monthly claim volumes and approval rates, provider leaderboards, member utilisation by plan tier, financial exposure by employer group, claims processing time distributions, and stockout alerts for dispensing facilities. These reports are available through the API and as exportable datasets for BI tools.

---

## 4. Who This Is Built For

### The Finance Director

Needs to know: total claims liability this month, approval rate trends, which employer groups are driving above-average claim spend, and what the fraud exposure looks like. AfyaBima surfaces these through the analytics marts and the reporting API without requiring any manual data assembly.

### The Fraud Investigation Team

Needs to know: which claims are highest risk today, what signals triggered the flag, what the member's and provider's history looks like, and where to focus the investigation queue. AfyaBima surfaces the ranked claim queue, the feature context for each prediction, and the investigation workflow tools through the FastAPI portal.

### The Claims Operations Manager

Needs to know: processing time by provider tier, approval rates by diagnosis category, pending claim volumes by status, and which providers have unusually high rejection rates. The dbt marts and API endpoints cover all of this.

### The Data and Engineering Team

Needs to know: how data flows through the system, where each table comes from, what tests are in place, how the ML model is versioned, and how to add a new data source or a new report. The dbt lineage documentation, Airflow DAG observability, MLflow experiment tracking, and GitHub Actions CI/CD cover all of this.

---

## 5. System Architecture

### 5.1 Logical Data Flow

```txt
┌─────────────────────────────────────────────────────────────────┐
│                        SOURCE SYSTEMS                           │
│  Claims Portal · Member Portal · Provider Portal · Investigator │
└───────────────────────────┬─────────────────────────────────────┘
                            │  Avro events (Schema Registry enforced)
                            ▼
┌─────────────────────────────────────────────────────────────────┐
│                       APACHE KAFKA                              │
│  claims.submitted · claims.status_updated · claims.paid         │
│  members.registered · providers.updated                         │
│  investigations.opened · investigations.closed                  │
└──────┬────────────────────┬────────────────────┬────────────────┘
       │                    │                    │
       ▼                    ▼                    ▼
┌─────────────┐   ┌──────────────────┐  ┌──────────────────┐
│ Kafka       │   │  Apache Flink    │  │  Kafka Connect   │
│ Connect     │   │                  │  │  S3 Sink         │
│ JDBC Sink   │   │  · Enrichment    │  │                  │
│             │   │  · Anomaly       │  │  → AWS S3        │
│ → Postgres  │   │    Detection     │  │   (raw archive)  │
│   raw.*     │   │  · ML Scoring    │  └──────────────────┘
└─────────────┘   └────────┬─────────┘
       │                   │ fraud signals
       │                   ▼
       │          ┌──────────────────┐
       │          │  Kafka Connect   │
       │          │  JDBC Sink       │
       │          │  → raw.fraud_    │
       │          │    predictions   │
       │          └──────────────────┘
       │
       ▼
┌─────────────────────────────────────────────────────────────────┐
│                      POSTGRESQL (RDS)                           │
│  raw.*  →  staging.*  →  intermediate.*  →  marts.*             │
│                  (dbt, orchestrated by Airflow)                  │
└──────────────────────────┬──────────────────────────────────────┘
                           │
          ┌────────────────┼───────────────────┐
          ▼                ▼                   ▼
  ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐
  │   FastAPI    │  │  PySpark     │  │  Debezium CDC        │
  │   REST API   │  │  ML Training │  │                      │
  │              │  │  + MLflow    │  │  Watches             │
  │  · Claims    │  │              │  │  fraud_investigation │
  │  · Fraud     │  │  → New model │  │  _outcomes           │
  │  · Analytics │  │    artefact  │  │                      │
  │  · Verdicts  │  │    → Flink   │  │  → Kafka             │
  └──────────────┘  └──────────────┘  │  investigations.     │
                                      │  closed topic        │
                                      └──────────────────────┘
                                               │
                                               ▼
                                      Airflow weekly
                                      retraining DAG
                                      consumes new labels
```

### 5.2 The Three Processing Tiers

| Tier | Latency | Tools | Purpose |
| --- | --- | --- | --- |
| Real-Time | Under 2 seconds | Kafka, Flink, Schema Registry | Ingest events, score claims, detect patterns |
| Batch | Daily | dbt, Airflow, PostgreSQL | Transform, test, document, build analytics |
| ML Retraining | Weekly | PySpark, MLflow, Airflow | Retrain on new labels, evaluate, promote |

### 5.3 Key Architectural Decisions

**Kafka as the single entry point — not the database.**
Microservices publish to Kafka. They do not write to PostgreSQL directly. This means if the database is temporarily unavailable, no events are lost — Kafka retains them and Kafka Connect resumes when the database recovers. It also means the same event simultaneously feeds Flink for scoring, Kafka Connect for persistence, and the S3 Sink for archival — one publish, three consumers, no duplication of logic.

**Schema Registry before anything else in the streaming layer.**
Multiple microservices publishing to shared Kafka topics without schema enforcement is the most common source of silent data corruption in streaming architectures. Schema Registry enforces Avro schemas at publish time. A producer publishing a non-conforming message receives an immediate error at the source, not a silent corruption in the database hours later.

**dbt reads from PostgreSQL, not directly from Kafka.**
dbt is the batch transformation layer. It reads from the raw schema that Kafka Connect has already populated. This separation means dbt models are always working on persisted, queryable data — not on live stream state that is difficult to test and impossible to time-travel.

**Debezium for the feedback loop, not dual-write.**
When an investigator submits a fraud verdict through the API, it writes to PostgreSQL. The challenge is getting that verdict back into Kafka for the retraining pipeline to consume — without requiring the API to publish to Kafka directly, which creates a dual-write consistency problem. Debezium reads the PostgreSQL Write-Ahead Log and publishes every relevant INSERT to Kafka automatically, with no application code required.

**Read replica for workload isolation.**
dbt runs daily and generates significant read load. FastAPI serves the investigation portal continuously. Both reading from the same PostgreSQL instance that Kafka Connect is writing to creates contention. A read replica isolates analytical workloads from transactional writes — same technology, much lower complexity than separate databases.

**Dead Letter Queue for malformed events.**
Schema Registry catches messages that violate the registered Avro schema at publish time. However, a message can be schema-valid but still fail to write to PostgreSQL — a null value in a non-nullable column, a foreign key that does not exist yet, a JSONB payload that exceeds column limits. Rather than silently dropping these events or blocking the pipeline, each source topic has a corresponding dead letter queue (DLQ) topic following the naming convention `*.dlq` (e.g. `claims.submitted.dlq`). Kafka Connect routes failed sink writes to the DLQ with an error metadata header. The Airflow data quality DAG monitors DLQ consumer lag hourly and triggers a Slack alert if any DLQ topic accumulates more than 10 messages — indicating a systematic issue that needs investigation rather than a one-off transient failure.

---

## 6. Database Schema Design

### 6.1 Schema Organisation

PostgreSQL is organised into four schemas with clearly defined ownership boundaries. Database roles enforce these boundaries — Kafka Connect has write access to `raw` only; dbt has read access to `raw` and write access to `staging`, `intermediate`, and `marts`; FastAPI has read access to `marts` only.

| Schema | Written By | Read By | Purpose |
| --- | --- | --- | --- |
| `raw` | Kafka Connect | dbt | Exact mirror of Kafka message payloads. Never modified after insert. |
| `staging` | dbt | dbt | Views. One model per raw table. Rename, cast, deduplicate. No joins. |
| `intermediate` | dbt | dbt | Views. Business logic and joins. Never accessed by applications. |
| `marts` | dbt | FastAPI, BI tools | Tables. Denormalised, tested, refreshed daily. Production query target. |

### 6.2 Reference and Master Data (raw schema)

These tables change infrequently and are loaded via full refresh each cycle.

| Table | Key Columns | Notes |
| --- | --- | --- |
| `raw.plans` | plan_code, plan_name, plan_tier, annual_limit_kes, inpatient_limit_kes, outpatient_limit_kes, premium_monthly_kes, is_family_plan | Defines the financial parameters of each product |
| `raw.employers` | employer_id, employer_name, industry, member_count, contract_start, contract_end | Corporate clients sponsoring member coverage |
| `raw.members` | member_id, full_name, date_of_birth, gender, plan_code, employer_id, enrol_date, term_date, region | Insured beneficiaries. FK to plans and employers |
| `raw.providers` | provider_id, provider_name, city, tier (1–3), is_accredited, contracted_at, avg_processing_days | Healthcare facilities in the provider network |
| `raw.drug_formulary` | drug_code, drug_name, category, is_essential_medicine, is_controlled_substance | Approved drug list for prescription validation |
| `raw.icd10_codes` | icd10_code, description, clinical_category | Seeded from CSV. Validates and enriches diagnosis codes. |

### 6.3 Transactional Event Tables (raw schema)

These tables grow continuously as Kafka Connect consumes from the event topics.

| Table | Key Columns | Notes |
| --- | --- | --- |
| `raw.claim_events` | event_id, claim_id, event_type, previous_status, new_status, event_at, triggered_by, metadata (JSONB) | Immutable event log. Every state transition. Never updated, only appended. |
| `raw.claims` | claim_id, member_id, provider_id, claim_date, service_date, diagnosis_code, amount_claimed, amount_approved, status, submission_channel, submitted_at, updated_at,_loaded_at | Current claim state. Upserted by Kafka Connect on claim_id. |
| `raw.vitals` | claim_id, weight_kg, height_cm, bp_systolic, bp_diastolic, temperature_c, spo2_pct, pulse_bpm, recorded_at | Clinical measurements. Absence of vitals for a consultation claim is a fraud signal. |
| `raw.prescriptions` | prescription_id, claim_id, drug_code, dosage, frequency, duration_days, dispensed_on_site | Prescribed drugs per claim. Absence for diagnoses that require medication is clinically anomalous. |
| `raw.payments` | payment_id, claim_id, amount_paid, payment_method, reference_no, paid_at | One row per payment disbursement. |
| `raw.stock_levels` | facility_id, drug_code, quantity_on_hand, reorder_level, reported_date | Pharmacy stock. Enables stockout detection and supply chain analytics. |

### 6.4 Fraud Pipeline Tables (raw schema)

| Table | Written By | Key Columns | Notes |
| --- | --- | --- | --- |
| `raw.fraud_predictions` | Flink | prediction_id, claim_id, model_version, fraud_probability, risk_tier, feature_snapshot (JSONB), predicted_at | One row per claim per model version. feature_snapshot records exact feature values for auditability. |
| `raw.fraud_investigations` | FastAPI | investigation_id, claim_id, assigned_to, opened_at, closed_at, status, priority, notes | Investigation lifecycle. Updated as status changes. |
| `raw.fraud_investigation_outcomes` | FastAPI | outcome_id, claim_id, investigation_id, final_label, fraud_type, confirmed_by, confirmed_at, estimated_loss_kes | Final verdict. Debezium watches this table and publishes every INSERT to Kafka automatically. |
| `raw.model_performance_log` | PySpark retraining job | model_version, evaluation_date, auc_roc, precision_at_10pct, recall, f1_score, promoted | Tracks model quality over time. Used by Airflow to decide promotion. |
| `raw.dlq_events` | Kafka Connect (DLQ sink) | dlq_id, source_topic, failed_at, error_code, error_message, raw_payload (JSONB) | Dead letter records. Every event that failed to write to its target raw table lands here with full error context for manual inspection and replay. |

### 6.5 dbt Model Structure

#### Staging Layer — Views

| Model | Key Transformations |
| --- | --- |
| `stg_claims` | Deduplicate on claim_id (latest_loaded_at wins). Cast types. Derive days_to_submit, approval_pct, approval_type. Flag is_same_day_submission. |
| `stg_members` | Derive is_active, age_years, tenure_months. Clean and standardise name and region. |
| `stg_providers` | Derive tier_label. Flag is_accredited for network tier classification. |
| `stg_fraud_predictions` | Join model_performance_log for model context per prediction. |
| `stg_fraud_investigation_outcomes` | Validate investigation timeline. Derive investigation_duration_days. |
| `stg_payments` | Normalise payment_method to uppercase. Derive payment_date from paid_at timestamp. |
| `stg_stock_levels` | Derive is_stockout (quantity_on_hand = 0) and days_of_stock_remaining. |

#### Intermediate Layer — Views

| Model | Purpose |
| --- | --- |
| `int_claim_fraud_features` | The ML feature engineering model. Computes all statistical signals: provider rolling claim frequencies (1h, 6h, 24h, 7d windows), provider amount z-score vs district peers, member multi-provider visit count in 7 days, amount vs approval threshold proximity, has_matching_vitals, has_matching_prescription, member YTD utilisation rate, days_to_submit z-score. |
| `int_claims_enriched` | Full claim context: claim joined to member, provider, plan, employer, diagnosis description, and payment. Base for fct_claims. |
| `int_fraud_model_evaluation` | Links predictions to investigation outcomes. Computes was_prediction_correct, false_positive_rate by risk tier. Feeds model drift monitoring. |
| `int_provider_risk_profile` | Daily provider-level aggregates: historical fraud rate, claim frequency trend, average amount trend, anomaly count in last 30 days. |
| `int_member_plan_history` | Derived from the members_snapshot. Tracks plan changes over time for each member. |
| `int_supply_chain_status` | Current stock levels vs reorder thresholds per facility per drug. Identifies stockout events and duration. |
| `int_disease_patterns` | Diagnosis frequency by ICD category, provider, region, and week. Used for disease surveillance reporting. |

#### Marts Layer — Tables (daily refresh)

| Model | Materialisation | Purpose |
| --- | --- | --- |
| `fct_claims` | Incremental, merge on claim_id | One row per claim with full member, provider, plan, fraud score, and payment context. Primary query target for all downstream consumers. |
| `fct_payments` | Incremental, merge on payment_id | One row per payment with claim and member context. Feeds financial reconciliation reporting. |
| `fct_fraud_scoring` | Incremental, merge on prediction_id | Prediction accuracy over time as investigation outcomes arrive. Used for model drift monitoring. |
| `dim_members` | Full refresh | Current-state member dimension with plan, employer, age segment, and member segment (Retail / Corporate). |
| `dim_providers` | Full refresh | Provider dimension with network tier, accreditation, and daily risk profile score. |
| `dim_plans` | Full refresh | Plan dimension with affordability tier derived from monthly premium. |
| `dim_date` | Full refresh | Date spine 2020–2028 with public holidays and business day flag. |
| `mart_fraud_analytics` | Full refresh | Aggregated fraud KPIs by provider, plan tier, region, and month. |
| `mart_claims_performance` | Full refresh | Monthly approval rates, processing times, and financial summaries by employer, provider, and diagnosis category. |
| `mart_supply_chain_summary` | Full refresh | Stockout frequency and duration by facility and drug category. |

#### Snapshot

| Snapshot | Strategy | Tracked Columns | Purpose |
| --- | --- | --- | --- |
| `members_snapshot` | check | plan_code, employer_id, region, term_date | SCD Type 2 — captures plan upgrades, downgrades, and employer changes over time. Enables temporal fraud analysis: did fraud begin after a plan upgrade? |

---

## 7. Technology Stack and Rationale

Every tool in AfyaBima was chosen because it solves a specific problem that a simpler alternative cannot solve well enough. The following table documents both the choice and the reason the alternative was not sufficient.

| Tool | Role | Why This Tool | Why Not the Alternative |
| --- | --- | --- | --- |
| **Apache Kafka 3.7** (KRaft mode) | Central event bus | Durable, replayable, multi-consumer fan-out. Industry standard for event streaming. | RabbitMQ is a message queue, not an event log. No replay. No long-term retention. |
| **Karapace** (Schema Registry) | Enforce Avro schemas at publish time | Prevents silent schema drift from breaking downstream consumers. Errors at the source, not in the database. | Without it: a renamed field in one microservice corrupts raw tables silently. |
| **Apache Flink 1.19** | Real-time stateful stream processing and ML scoring | True continuous streaming. Native stateful windowing. CEP library for temporal fraud patterns. | PySpark Streaming is micro-batch (30–60s lag). Stateful joins require manual state management. |
| **Kafka Connect** | JDBC Sink (→ PostgreSQL) and S3 Sink (→ AWS S3) | No-code connectors. Schema-Registry-aware. Handles upserts. Scales horizontally. | Custom Flink sinks: more failure modes, same result, more code to maintain. |
| **Debezium 2.6** | CDC from PostgreSQL WAL back to Kafka | Captures investigation outcome writes without dual-write logic in FastAPI. Consistency at the database level. | FastAPI publishing to Kafka directly: transaction and Kafka publish can diverge on failure. |
| **PostgreSQL 15** (AWS RDS) | System of record | JSONB for feature snapshots. Read replicas for workload isolation. Battle-tested. | Snowflake: unnecessary cost and complexity at this data volume. |
| **dbt 1.8** (dbt-postgres) | Batch transformation: raw → staging → intermediate → marts | Declarative SQL, built-in testing, lineage documentation, version controlled. | Custom ETL scripts: no testing, no lineage, no documentation. |
| **PySpark 3.5** | Batch ML feature engineering and model training | Handles full historical dataset efficiently. Rich ML ecosystem. | Flink for batch: Flink is optimised for streaming, not large historical scans. |
| **MLflow 2.13** | Experiment tracking, model registry, champion/challenger | Tracks every run. Manages model versions. ONNX export for Flink. | Without it: no reproducibility, no version comparison, no rollback capability. |
| **Apache Airflow 2.9** (KubernetesExecutor) | Orchestrates dbt, PySpark, quality checks, model promotion | DAG-based pipelines, native Kubernetes integration, industry standard. | Cron jobs: no dependency management, no retries, no observability. |
| **FastAPI 0.111** | REST API and investigator portal backend | High performance async. Pydantic validation. Auto-generated OpenAPI docs. | Django: too heavy for a pure API. Flask: no async, no automatic validation. |
| **AWS S3** | Raw event archive, ML training data, model artefacts | Durable, cheap, integrates natively with Flink, PySpark, and Airflow. | Local storage: not durable, not accessible across services. |
| **AWS EKS** | Kubernetes cluster for all containerised workloads | Managed Kubernetes. Integrates with IAM, ECR, ALB, CloudWatch. | Self-managed K8s: significant operational overhead. |
| **AWS RDS** (PostgreSQL 15, Multi-AZ) | Managed PostgreSQL with automated backups and read replicas | Eliminates database operations overhead. Read replicas isolate analytical workloads. | Containerised PostgreSQL: no managed backups, no easy read replicas. |
| **Docker** | Container runtime | Reproducible environments. Required by Kubernetes. | Bare-metal: environment drift, no portability. |
| **GitHub Actions** | CI/CD: dbt slim CI, pytest, Docker builds to ECR | Native to GitHub. Fast. Free for public repositories. | Jenkins: requires its own infrastructure. |
| **Prometheus + Grafana** | Metrics collection and dashboarding | Kafka consumer group lag, Flink checkpoint duration and failure rate, fraud prediction rate per hour, DLQ depth, FastAPI request latency, Airflow task success/failure rate. Alerts on breach of defined SLOs. | CloudWatch metrics alone: no cross-service dashboard, no custom Flink metrics. |
| **AWS CloudWatch** | Log aggregation on EKS | All container stdout/stderr forwarded via the CloudWatch agent on EKS. Structured JSON logging from FastAPI and Airflow. Log retention per compliance requirements. | Elasticsearch/Kibana: significant operational overhead for log aggregation at this scale. CloudWatch integrates natively with EKS. |
| **OpenTelemetry** | Distributed tracing | Traces a single claim event across Kafka → Flink → Kafka Connect → PostgreSQL → FastAPI. Surfaces latency bottlenecks and failure points across service boundaries. Exporters for both Prometheus and CloudWatch. | No tracing: impossible to diagnose why a specific claim took 4 seconds to score or where in the pipeline a failure occurred. |

### Tools Evaluated and Excluded

The following tools were considered and deliberately excluded. The principle applied consistently: do not add a tool unless it solves a specific problem that the simpler alternative demonstrably cannot handle at the current scale.

- **Apache Iceberg / Delta Lake** — valuable at petabyte scale. At thousands of claims per day against PostgreSQL, adds operational complexity without addressing a real bottleneck. Revisit when PySpark training jobs exceed 30 minutes.
- **Presto / Trino / Apache Druid** — purpose-built for analytics at massive scale. PostgreSQL with a read replica handles all current analytical query requirements.
- **Elasticsearch** — attractive for search and debugging. PostgreSQL full-text search covers all investigator query patterns at this scale.
- **ksqlDB** — overlaps with Flink. Flink's CEP library and state management are superior for the fraud pattern detection use cases here.

---

## 8. Implementation Phases

Each phase produces an independently verifiable, demonstrable artefact. No phase requires the next to be complete before its output can be tested. The platform is coherent and deployable at the end of any phase.

---

### Phase 0 — Design and Specification

**Duration:** 1–2 days before any code is written

**Objective:** Lock down every schema decision, Kafka topic design, fraud pattern specification, and data generation timeline before building anything. Every downstream component depends on these decisions being stable.

**Why first:** Changing the schema after Kafka topics are live means updating Avro schemas in Schema Registry, Kafka Connect configurations, dbt models, Flink jobs, and FastAPI schemas simultaneously. A single session of disciplined upfront design prevents cascading rework across every layer.

**Deliverables:**

- Complete PostgreSQL table specification: every column, type, constraint, index, and foreign key
- Kafka topic specification: every topic name, partition key, Avro schema, retention policy
- Fraud pattern specifications written as precise statistical definitions, not vague descriptions
- Data generation timeline: 24 months, investigation capability maturing over time
- Investigation lifecycle state machine: states, allowed transitions, who triggers each

**Done when:** A specification document exists that two engineers could use to build compatible components independently without synchronising on implementation details.

---

### Phase 1 — Data Foundation: PostgreSQL and Seed Data

**Duration:** 2–3 days

**Objective:** Produce a populated PostgreSQL database with realistic seed data including fraud patterns. Everything built after this reads from or writes to this database.

**Why second:** Every other component depends on data existing in the right shape. Building any streaming or transformation component against an empty or poorly designed database means rebuilding it when the schema is corrected. Get the foundation right and everything built on it remains stable.

**Deliverables:**

- Docker Compose with PostgreSQL running all four schemas (raw, staging, intermediate, marts) with DDL for every table
- Python data generator producing 24 months of realistic claims: ~500 claims, 15 members, 10 providers, 6 employers
- Fraud patterns encoded with statistical precision: phantom billing, threshold manipulation, benefit sharing, duplicate claims
- Temporal realism: investigation labels arriving weeks after claims, not on the same day
- Label distribution: ~30–40 confirmed fraud cases across ~90–110 investigated claims, remainder unlabelled
- SQL assertions validating fraud label distribution, temporal gaps, referential integrity, and fraud signal detectability

**Done when:** `python generate_data.py` populates PostgreSQL with verifiable, realistic data. All SQL assertions pass.

---

### Phase 2 — Batch Transformation: dbt

**Duration:** 3–4 days

**Objective:** Build the complete dbt project transforming raw PostgreSQL data into tested, documented, lineage-tracked analytics assets.

**Why third:** dbt defines the feature engineering logic the ML pipeline will use. Defining these features in SQL now — before writing any PySpark — means all ML features are tested, version-controlled, and documented before the ML work begins. dbt also validates the data model rigorously, surfacing schema issues before streaming infrastructure complicates debugging.

**Deliverables:**

- Full dbt project: `dbt_project.yml`, `profiles.yml`, `packages.yml` (dbt_utils, dbt_expectations, dbt_date)
- All staging models with generic tests on every model (unique, not_null, accepted_values)
- `int_claim_fraud_features`: provider rolling frequencies (1h, 6h, 24h, 7d), amount z-scores, threshold proximity, member clustering, has_matching_vitals, has_matching_prescription, YTD utilisation
- All remaining intermediate and mart models
- `members_snapshot` configured for SCD Type 2 plan change tracking
- Singular dbt tests for business rules: approved amount never exceeds claimed, no future service dates, investigation timeline validity
- `generate_schema_name` macro override for clean schema naming across environments
- Seeds: `diagnosis_code_lookup.csv`, `kenya_public_holidays.csv`

**Done when:** `dbt build` passes with zero test failures. `dbt docs generate` produces a complete lineage graph showing every model from source to mart.

---

### Phase 3 — Event Streaming: Kafka and Schema Registry

**Duration:** 3–4 days

**Objective:** The same data that went directly to PostgreSQL in Phase 1 now flows through Kafka. Validate that the schema survives the streaming path unchanged.

**Why fourth:** By refactoring the data generator to publish to Kafka at this stage — after the schema is verified by dbt — Kafka becomes a transport layer built on a stable foundation rather than a design tool that forces schema rework in every other component.

**Deliverables:**

- Docker Compose additions: Kafka (KRaft, no Zookeeper), Karapace Schema Registry, Kafka Connect, Kafka UI (Redpanda Console)
- Avro schemas registered in Schema Registry for every topic defined in Phase 0
- Data generator refactored to publish Avro-encoded messages to Kafka instead of writing directly to PostgreSQL
- Kafka Connect JDBC Sink: one connector per topic, upsert mode, writing to the correct `raw.*` table
- Kafka Connect S3 Sink: all events archived to S3 partitioned by `topic/year/month/day/`
- Schema enforcement verification: a message with wrong schema is rejected at Schema Registry, not silently downstream
- DLQ topics configured for every source topic (`*.dlq` naming convention). `raw.dlq_events` table created. Airflow quality DAG configured to alert on DLQ consumer lag exceeding 10 messages.

**Done when:** One Kafka publish produces a PostgreSQL row AND an S3 file. `dbt build` still passes with zero failures — the database is identical to Phase 1 output.

---

### Phase 4 — Real-Time Processing: Apache Flink

**Duration:** 4–5 days

**Objective:** Build the Flink processing layer that enriches claims in real time, detects fraud patterns using stateful windows, and writes scoring results back to PostgreSQL.

**Why fifth:** With Kafka and PostgreSQL verified, Flink can consume real events from a stable foundation. Starting with rule-based detection before the ML model exists validates the full streaming pipeline end-to-end. When the model is added in Phase 5, it slots into an already-working pipeline.

**Deliverables:**

- Flink cluster (JobManager + TaskManager) in Docker Compose with S3 checkpointing and RocksDB state backend
- **Flink Job 1 — Claim Enrichment:** consume `claims.submitted`, enrich with member and provider history from PostgreSQL, publish to `claims.enriched`
- **Flink Job 2 — Windowed Anomaly Detection:** stateful windows per provider and per member, detect all four rule-based fraud patterns, publish fraud signals to `claims.fraud.signals`
- Kafka Connect JDBC Sink writing `claims.fraud.signals` to `raw.fraud_predictions`
- Flink checkpoint recovery test: simulate a JobManager failure, confirm correct state recovery from S3

**Done when:** A claim event matching a fraud pattern produces a prediction row in `raw.fraud_predictions` within 5 seconds. Flink recovers correctly from a simulated failure. `dbt build` incorporates predictions into `fct_fraud_scoring`.

---

### Phase 5 — Machine Learning: PySpark and MLflow

**Duration:** 4–5 days

**Objective:** Train a fraud detection model on labelled historical data, deploy it into the Flink scoring job, and establish the automated weekly retraining cycle.

**Why sixth:** The ML model requires everything before it: labelled historical data (Phase 1), tested features (Phase 2), the S3 event archive (Phase 3), and the real-time scoring infrastructure (Phase 4). A model built without this foundation cannot be deployed sustainably or retrained reliably.

**Deliverables:**

- PySpark feature engineering job: reads from S3 event archive and PostgreSQL fraud labels, computes full feature set mirroring `int_claim_fraud_features`
- XGBoost binary classifier with SMOTE oversampling and class-weighted loss. Logged to MLflow: AUC-ROC, precision at 10% recall, feature importance, model artefact.
- Threshold analysis: precision-recall curve, recommended operating threshold for the insurer's false-positive cost tolerance
- ONNX model export to S3. Flink loads the model artefact via a compacted `model.versions` Kafka topic — when Airflow promotes a new model, it publishes `{model_version, s3_artefact_path}` to that topic; Flink's `KeyedBroadcastProcessFunction` picks this up and hot-swaps the loaded model across all parallel instances without a job restart or downtime
- Airflow retraining DAG: weekly, consumes new investigation outcomes, triggers PySpark job, compares to MLflow champion, promotes if AUC-ROC improves by more than 0.01
- MLflow model registry with champion/challenger tracking and rollback capability

**Done when:** Flink scores each claim using the ML model within 500ms. MLflow shows version history. Airflow retraining DAG promotes or rejects correctly based on AUC-ROC comparison.

---

### Phase 6 — Serving Layer, Feedback Loop, and Production Deployment

**Duration:** 4–5 days

**Objective:** Build the FastAPI serving layer, close the investigator feedback loop via Debezium, write the full Airflow DAG suite, and deploy everything to AWS EKS.

**Why last:** FastAPI serves from mart tables that only exist after dbt runs. Debezium captures verdicts that only exist after investigators use FastAPI. Kubernetes deployment is appropriate only after every component is stable.

**Deliverables:**

- **FastAPI application:** claims list (filterable by status, provider, risk tier, date range), single claim detail with fraud score and feature context, investigator verdict submission endpoint, provider analytics, fraud dashboard aggregates, health check
- **Debezium configuration:** watches `raw.fraud_investigation_outcomes`, publishes every INSERT to `investigations.closed` Kafka topic — closing the retraining feedback loop
- **Full Airflow DAG suite:**
  - `dag_01_extract_load`: extract reference data, verify load counts
  - `dag_02_dbt_transform`: staging → intermediate → marts → dbt test → dbt docs
  - `dag_03_data_quality`: volume checks, null rate checks, referential integrity, statistical anomaly detection, Slack alerting
  - `dag_04_ml_retraining`: weekly PySpark feature job, training, MLflow comparison, conditional promotion
  - `dag_05_full_pipeline`: master orchestration DAG chaining all above
- Kubernetes manifests for all services: Flink cluster, Airflow, FastAPI, Kafka Connect, Debezium
- AWS deployment: EKS cluster, RDS PostgreSQL with read replica, S3 buckets, ECR image registry
- GitHub Actions workflows: dbt slim CI on pull requests, pytest for FastAPI, Docker build and push to ECR

**Done when:** The full platform runs on AWS EKS. An investigator verdict submitted via FastAPI results in a new model version appearing in MLflow within the simulated weekly retraining cycle.

---

## 9. Data Generation Strategy

### 9.1 Why This Is the Most Critical Non-Infrastructure Component

The data generator determines whether the entire project is credible. A generator that produces structurally valid but statistically unrealistic data will produce a fraud model that learns nothing meaningful — and an analyst who builds reports on it will notice immediately that the numbers do not behave like real insurance data.

The generator must satisfy three constraints simultaneously:

**Structural validity:** Every row in every table satisfies all constraints, foreign keys, and type expectations. No raw data ever breaks a dbt test.

**Statistical realism:** Claim amounts follow realistic distributions by diagnosis category. Provider claim frequencies have realistic variance. Approval rates by plan tier match published industry ranges. A trained analyst looking at the generated data should not immediately identify it as synthetic.

**Temporal honesty:** Investigation labels arrive weeks after claims, not instantly. Some investigations are still open at the end of the generation period. The proportion of claims that have been investigated is realistic — investigation teams have limited capacity and prioritise suspicious cases, so the investigation rate among high-risk claims is much higher than among low-risk ones.

### 9.2 Data Timeline Design

| Period | Data Characteristics |
| --- | --- |
| Months 1–6 | Claims arrive and are paid normally. No investigation function yet. Zero fraud labels. Provides the unlabelled baseline and simulates the backlog most insurers start with. |
| Months 7–12 | Investigation team formed. Retrospective review of suspicious Months 1–6 claims begins. Labels arrive 2–8 weeks after original claim submission. Some investigations still open at end of this period. |
| Months 13–18 | Investigation function fully operational. Real-time flagging begins. Approximately 60–70% of high-risk flagged claims are investigated. Correlation between statistical fraud signals and confirmed outcomes becomes statistically meaningful. |
| Months 19–24 | Sufficient labelled data exists for a first ML model. This is the evaluation period. New labels continue to arrive weekly. The retraining cycle produces measurably improving model versions. |

### 9.3 Fraud Pattern Encoding

| Pattern | Generator Implementation |
| --- | --- |
| **Phantom Billing** | Three providers designated as fraudulent. Their claims reference real member_ids but produce no corresponding vitals or prescription records. Claim frequency is set to 2.5–4x the district average for the same diagnosis category. Detectable at the provider level but not at the individual claim level — this is the realistic challenge. |
| **Threshold Manipulation** | 8% of all claims have amount_claimed in the range of 92–99% of one of three auto-approval thresholds (5,000 / 10,000 / 25,000 KES). The distribution of amounts just below these thresholds is statistically anomalous compared to the smooth distribution elsewhere. |
| **Benefit Sharing** | Two members appear at 4+ distinct providers within 7-day windows with identical diagnosis codes. Their individual claims look normal — the pattern only appears through temporal and geographic clustering. |
| **Duplicate Claims** | Twelve pairs of claims with the same (member_id, provider_id, diagnosis_code) within 48 hours, amounts varying by less than 5%. Variation is deliberate — exact duplicates are caught by simple deduplication and are not an interesting detection problem. |

### 9.4 Label Distribution

| State | Target Volume | Notes |
| --- | --- | --- |
| Total claims generated | ~500 | Across 24 months |
| Investigated claims | ~90–110 | 18–22% of total — realistic investigation capacity |
| Confirmed fraud (of investigated) | ~28–35 | ~30% of investigated — higher than population rate because investigators prioritise suspicious cases |
| Confirmed legitimate (of investigated) | ~55–75 | Cleared cases — equally important as negative training examples |
| Investigations still open | ~10–15 | No label yet — the model must not treat these as legitimate |
| Unlabelled | ~390–410 | Never investigated — the vast majority of all claims |

The 30% fraud rate among investigated claims is intentionally higher than the ~5–8% population fraud rate. Investigators prioritise suspicious cases. The ML pipeline must account for this selection bias when interpreting training data.

---

## 10. Analytics and Reporting Layer

AfyaBima is not only a fraud platform. The dbt marts layer and FastAPI reporting endpoints cover the full operational analytics need of an insurance operation.

### 10.1 Claims Performance Reporting

Sourced from `fct_claims` and `mart_claims_performance`:

- Monthly claim volumes by status (pending, approved, rejected, partial)
- Approval rate trends by plan tier, employer group, provider, and diagnosis category
- Claims processing time distribution: median and 90th percentile days from submission to decision
- Financial exposure: total amount_claimed vs total amount_approved by month and by employer
- Pending claim queue with age buckets (under 7 days, 7–14 days, over 14 days)

### 10.2 Provider Analytics

Sourced from `dim_providers` and `int_provider_risk_profile`:

- Provider leaderboard by approved claim volume and approval rate
- Provider claim frequency trends: week-over-week and month-over-month
- Provider average claim amount vs district peers for the same diagnosis category
- Network utilisation: which providers are receiving above-average referral volume
- Accreditation compliance: claims submitted through non-accredited providers

### 10.3 Member Utilisation

Sourced from `fct_claims` and `dim_members`:

- Member annual limit utilisation: what percentage of the annual limit has been consumed year-to-date
- Members approaching limit (above 80% utilisation) — proactive case management trigger
- Claims by age segment and gender — actuarial basis for pricing
- Member claim velocity: average days between consecutive claims (input for fraud scoring)
- Retail vs Corporate segment performance comparison

### 10.4 Financial Reconciliation

Sourced from `fct_payments`:

- Total payments disbursed by month and by payment method (EFT, MPESA, Cheque)
- Payments by provider and by employer group
- Payment processing lag: days between claim approval and actual disbursement
- Unpaid approved claims (approved but no corresponding payment record)

### 10.5 Supply Chain and Pharmacy

Sourced from `mart_supply_chain_summary` and `int_supply_chain_status`:

- Current stock levels vs reorder thresholds by facility and drug category
- Stockout events: frequency, duration, and affected prescriptions
- Essential medicine availability rate by facility tier
- Prescriptions dispensed on-site vs referred to external pharmacy

### 10.6 Disease Surveillance

Sourced from `int_disease_patterns`:

- Top 10 diagnosis categories by claim volume, by region, by week
- Seasonal patterns in respiratory, gastrointestinal, and injury diagnoses
- Unusual diagnosis frequency spikes at specific providers (a fraud signal as well as a public health signal)

---

## 11. Machine Learning and MLOps Pipeline

### 11.1 Model Architecture

AfyaBima uses a binary XGBoost classifier for fraud detection. The choice is deliberate:

- Handles class imbalance better than neural networks at this dataset size
- Produces interpretable feature importance scores essential for explaining decisions to investigators and regulators
- Trains quickly, enabling rapid iteration during development
- Exports cleanly to ONNX format for deployment into Flink without requiring a Python runtime inside the cluster

### 11.2 Feature Set

Features are organised into three categories. The full feature set is defined in `int_claim_fraud_features` (dbt) and mirrored in the PySpark feature engineering job.

**Claim-Level Features**

| Feature | Fraud Signal |
| --- | --- |
| days_to_submit | Fraudulent claims often submitted same-day — no real patient experience |
| amount_vs_threshold_proximity | Amounts near approval limits indicate threshold awareness |
| has_matching_vitals | Absence of vitals for a consultation is the primary phantom billing signal |
| has_matching_prescription | Diagnosis without prescription is clinically anomalous |
| is_same_day_submission | Boolean flag derived from days_to_submit |

**Provider-Level Aggregate Features**

| Feature | Fraud Signal |
| --- | --- |
| provider_claim_freq_1h | Burst in 1-hour window far above provider's historical baseline |
| provider_claim_freq_6h | 6-hour window is the most predictive for phantom billing |
| provider_claim_freq_24h | Daily volume vs 30-day daily average |
| provider_amount_zscore_30d | Provider's average amount vs district peers, same diagnosis category |
| provider_fraud_history_rate | Proportion of historical claims confirmed as fraudulent |

**Member-Level Aggregate Features**

| Feature | Fraud Signal |
| --- | --- |
| member_provider_count_7d | Distinct providers in 7 days — primary benefit sharing signal |
| member_ytd_utilisation_pct | Approved YTD as % of annual limit — limit exhaustion fraud |
| member_claim_velocity | Average days between consecutive claims |

### 11.3 Training Pipeline

1. Read all claims from S3 event archive with confirmed labels from PostgreSQL investigation outcomes
2. Join on claim_id to produce a labelled feature dataset, excluding unlabelled and inconclusive cases
3. Apply SMOTE oversampling to the confirmed_fraud minority class
4. Split: 70% train, 15% validation, 15% hold-out test — stratified by label
5. Train XGBoost with class-weighted loss. Hyperparameter tuning on validation set.
6. Evaluate on hold-out: AUC-ROC, precision at 10% recall, F1, confusion matrix, feature importance — all logged to MLflow
7. Compare to current champion model in MLflow Model Registry by AUC-ROC
8. If new model AUC-ROC exceeds champion by more than 0.01: promote to Production stage
9. Airflow signals Flink to reload the new model artefact from S3. Flink reloads without downtime.

### 11.4 Model Monitoring and Drift Detection

Two monitoring mechanisms run as part of the daily Airflow quality DAG:

**Retrospective accuracy tracking:** As new investigation outcomes arrive, `fct_fraud_scoring` is updated with whether each past prediction was correct. A 4-week rolling accuracy metric is computed daily. A declining trend over four consecutive weeks triggers a review alert.

**Prediction distribution monitoring:** The proportion of claims scoring above 0.8 is tracked weekly. A significant change without a corresponding change in confirmed fraud rates flags a potential distribution shift — either fraud patterns have changed or the model has drifted.

### 11.5 Flink Model Update Mechanism

Deploying a new model version into a running Flink job without downtime requires an explicit update mechanism. AfyaBima uses a compacted Kafka topic called `model.versions` for this purpose.

When the Airflow retraining DAG promotes a new model, it publishes a small metadata message to `model.versions`:

```json
{
  "model_version": "2.3",
  "s3_artefact_path": "s3://afyabima-models/xgboost/v2.3/model.onnx",
  "promoted_at": "2025-03-01T02:36:00Z"
}
```

The Flink scoring job maintains a `KeyedBroadcastProcessFunction` that subscribes to `model.versions` as a broadcast stream. When a new message arrives, all parallel scoring instances load the new ONNX artefact from S3 simultaneously and switch to it for all subsequent claims. Claims already in-flight at the moment of the switch complete with the previous model version — the `model_version` field in `raw.fraud_predictions` records which version scored each claim, preserving full auditability.

The `model.versions` topic uses log compaction so Flink always recovers the latest model version correctly after a job restart or checkpoint recovery, without replaying the full topic history

---

## 12. API Serving Layer

The FastAPI application reads exclusively from the `marts` schema and exposes the following endpoints:

### Claims Endpoints

- `GET /claims` — paginated, filterable by status, provider, risk tier, date range, plan tier
- `GET /claims/{claim_id}` — full claim detail with fraud score, feature context, investigation status
- `GET /claims/member/{member_id}` — all claims for a member with utilisation context

### Fraud Endpoints

- `GET /fraud/queue` — high-risk claims ranked by fraud_probability, filterable by risk tier
- `GET /fraud/predictions/{claim_id}` — prediction detail with feature_snapshot for explainability
- `POST /fraud/investigations` — open a new investigation
- `PUT /fraud/investigations/{investigation_id}/verdict` — submit confirmed fraud or legitimate verdict (triggers Debezium → Kafka → retraining loop)

### Analytics Endpoints

- `GET /analytics/monthly-summary` — monthly claim volume, approval rate, financial summary
- `GET /analytics/provider-leaderboard` — providers ranked by approved volume and approval rate
- `GET /analytics/plan-utilisation` — average annual limit utilisation by plan tier
- `GET /analytics/claim-velocity` — members with abnormally high claim frequency (fraud signal)
- `GET /analytics/disease-patterns` — weekly diagnosis frequency by category and region

### Member and Provider Endpoints

- `GET /members/{member_id}` — current member detail with plan and employer context
- `GET /providers/{provider_id}` — provider detail with risk profile and accreditation status

---

## 13. Future Expansion

Each expansion below is designed in but not built in v1.0. Each has a specific trigger condition — a measurable sign that the current approach has reached its limit.

| Expansion | Trigger | Rationale |
| --- | --- | --- |
| **PostgreSQL read replica** | dbt runs begin causing latency on FastAPI responses | Isolates analytical workloads from transactional writes without changing any application code |
| **PgBouncer connection pooling** | Multiple Flink TaskManagers and FastAPI pods exhaust connection limit | Adds pooling with no schema changes |
| **Apache Iceberg on S3** | PySpark training jobs exceed 30 minutes | Partition pruning and time travel reduce scan costs. Replaces raw Parquet on S3. |
| **Feature Store (Feast)** | More than three ML models share overlapping features | Centralises feature computation. Serves features to both training and real-time inference. |
| **Streaming dbt (Materialize or RisingWave)** | Investigators demand predictions reflecting the last hour, not yesterday | Continuous SQL streaming supplements or replaces the daily batch layer |
| **Graph fraud detection (Neo4j)** | Provider-member-employer collusion rings become a dominant pattern | Graph algorithms detect network relationships that tabular ML cannot |
| **Multi-region Kafka (MirrorMaker 2)** | Expansion to a second country with data residency requirements | Replicates topics across regions while keeping data physically co-located |
| **Multi-tenant architecture** | A second insurer is onboarded | Per-tenant schemas, Kafka topic namespacing, per-tenant MLflow model versioning |
| **Real-time BI (Apache Superset + Flink)** | Finance team requests intraday dashboards | Flink writes aggregates to a dedicated Superset data source with sub-minute refresh |

### Regulatory Considerations

Operating in regulated insurance markets introduces requirements that are designed in but not fully configured in v1.0:

- **Model explainability:** The `feature_snapshot` JSONB column in `raw.fraud_predictions` records the exact feature values at scoring time. Every fraud decision can be explained to investigators and regulators.
- **Audit trail:** The Debezium event log provides a complete, immutable record of all investigation outcome changes.
- **Data retention:** S3 lifecycle policies and PostgreSQL partition expiry must be configured per jurisdiction.
- **Data localisation:** Markets with strict localisation requirements may need per-country EKS cluster deployments rather than a shared cluster.

---

## 14. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
| --- | --- | --- | --- |
| Schema drift silently breaks Kafka Connect sink | High without Schema Registry | High | Schema Registry with BACKWARD compatibility enforced. Incompatible messages fail at publish time, not silently in the database. |
| Flink job fails — fraud signals stop flowing | Medium | High | Prometheus metrics on prediction rate per hour. Airflow quality DAG alerts if `raw.fraud_predictions` has no new rows within threshold. S3 checkpointing enables fast restart. |
| ML model degrades as fraud patterns evolve | Medium over time | Medium | Weekly retraining cycle. 4-week rolling accuracy tracked in `fct_fraud_scoring`. Alert on >0.05 AUC-ROC drop from promoted baseline. |
| Class imbalance produces high accuracy, zero usefulness | High if not addressed | High | SMOTE oversampling. Class-weighted XGBoost. Evaluation on precision-recall only. Human review of confusion matrix before every promotion decision. |
| Data generator produces unrealistic patterns | Medium | High | Generator reviewed against published health insurance fraud literature. Feature distributions inspected before ML training. If the model learns no meaningful signal, the generator is the first thing reviewed. |
| Debezium CDC misses investigation outcome writes | Low | High | WAL-level CDC is highly reliable. Weekly reconciliation task cross-references PostgreSQL investigation outcomes against Kafka consumer offset to detect gaps. |
| PostgreSQL write bottleneck under increased load | Low at current scale | Medium | Read replica pre-planned. PgBouncer for connection pooling. Kafka Connect batch.size and poll.interval tuned to avoid write spikes. |
| dbt test failure blocks downstream mart refresh | Medium | Medium | dbt tests configured with warn severity for non-critical assertions. Error severity only for mart-level data correctness. Airflow alerts on dbt test failures. mart tables not dropped on failure — stale data is surfaced, not deleted. |
| Kafka Connect write fails silently on valid-schema but bad-data events | Medium | Medium | DLQ topic per source topic captures all failed writes with full error context. Airflow monitors DLQ lag hourly. Slack alert on accumulation above threshold. `raw.dlq_events` enables manual inspection and replay. |

---

## 15. Appendix — Skill Coverage Map

AfyaBima is designed as a portfolio project demonstrating the full range of a data generalist's capabilities. The following table maps each discipline to the specific components of the platform that demonstrate it.

| Discipline | Where Demonstrated |
| --- | --- |
| **Analytics Engineering** | dbt project: staging, intermediate, and mart models. Source freshness. Schema tests. Singular tests. dbt docs lineage graph. SCD Type 2 snapshot. |
| **Data Pipeline Design** | Kafka event bus. Kafka Connect sinks (JDBC and S3). Schema Registry. Multi-consumer fan-out. Idempotent load patterns. Incremental dbt models. |
| **Streaming Data Engineering** | Apache Flink: stateful windowed aggregations, claim enrichment job, anomaly detection job, ML scoring job, S3 checkpoint recovery. |
| **Batch Data Engineering** | PySpark feature engineering job. Airflow DAG suite with cross-DAG dependencies, ExternalTaskSensor, TriggerDagRunOperator. |
| **Machine Learning** | XGBoost fraud classifier. Class imbalance handling via SMOTE. Feature importance analysis. Threshold optimisation. ONNX model export. |
| **MLOps** | MLflow experiment tracking and model registry. Champion/challenger promotion logic. Model drift monitoring. Automated weekly retraining DAG. Flink model reload without downtime. |
| **Data Modelling** | Star schema: fact tables, dimension tables, and aggregate marts. SCD Type 2 via dbt snapshot. Incremental vs full refresh strategy decisions. |
| **API Development** | FastAPI application with Pydantic validation, dependency injection, pagination, filtering, and auto-generated OpenAPI documentation. |
| **Cloud and Infrastructure** | AWS EKS, RDS, S3, ECR. Docker containerisation. Kubernetes manifests for all services. GitHub Actions CI/CD. |
| **Data Quality** | dbt generic and singular tests. Airflow quality DAG: volume checks, null rate checks, referential integrity checks, statistical anomaly detection, Slack alerting. |
| **Change Data Capture** | Debezium CDC from PostgreSQL WAL back to Kafka for the investigator feedback loop. |
| **Database Design** | PostgreSQL schema organisation with role-based access control. JSONB for semi-structured feature snapshots. Indexes for query performance. Read replica for workload isolation. |
| **Business Intelligence** | Mart models designed for BI consumption: claims performance, provider analytics, member utilisation, financial reconciliation, disease surveillance, supply chain. |
| **Documentation** | This document. dbt docs with model descriptions and column-level documentation. OpenAPI docs generated by FastAPI. |
| **Observability and Monitoring** | Prometheus + Grafana for metrics and SLO alerting. CloudWatch for EKS log aggregation. OpenTelemetry for distributed tracing across Kafka, Flink, and FastAPI. DLQ monitoring via Airflow quality DAG. |

---

*AfyaBima — End-to-End Health Insurance Data Platform*
*Version 1.0 · 2026 · Confidential*
