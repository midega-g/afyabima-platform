"""
AfyaBima — Scenario B: Kafka Streaming Producer
================================================

PURPOSE
-------
Extends the existing AfyaBimaKenyaGenerator with a streaming mode that
emits each day's data to Kafka topics as it is generated, instead of
accumulating everything in memory and writing CSVs at the end.

WHY THE EXISTING GENERATOR CANNOT STREAM AS-IS
-----------------------------------------------
The generator currently has a hard batch dependency: after the 24-month
daily loop completes, it runs two post-processing passes that require
the *entire* claim history to exist in memory:

  1. inject_benefit_sharing_clusters()
     Scans all_claims to find existing member activity windows, then
     injects additional claims at different providers within those windows.
     This requires seeing 24 months of history before deciding where to
     inject — impossible to do day-by-day.

  2. inject_duplicate_pairs()
     Randomly selects legitimate claims from the full history and creates
     near-duplicate records. Again needs the full population to draw from.

STREAMING APPROACH
------------------
The approach here separates the two concerns:

  Phase 1 — Batch pre-generation (runs offline, once):
    Generate and write the full timeline including injected fraud patterns.
    This is what `main.py` already does. The CSVs are the output.

  Phase 2 — Streaming replay (this module):
    Read the CSVs in chronological order and publish each record to the
    correct Kafka topic at a configurable playback speed. Kafka Connect
    then sinks from those topics into PostgreSQL, exactly as it would in
    production.

This cleanly separates "synthetic data generation" from "pipeline simulation."
The DB never sees a CSV — it only sees Kafka-sourced records, validating
the full Kafka → Kafka Connect → PostgreSQL path.

KAFKA TOPICS (must be created before running)
---------------------------------------------
  afyabima.raw.plans
  afyabima.raw.employers
  afyabima.raw.icd10_codes
  afyabima.raw.drug_formulary
  afyabima.raw.providers
  afyabima.raw.members
  afyabima.raw.claims
  afyabima.raw.claim_events
  afyabima.raw.vitals
  afyabima.raw.prescriptions
  afyabima.raw.payments
  afyabima.raw.stock_levels
  afyabima.raw.fraud_investigations
  afyabima.raw.fraud_investigation_outcomes

USAGE
-----
  # Install kafka-python (add to pyproject.toml)
  uv add kafka-python

  # Replay at 1× real-time (1 simulated day = 1 real second)
  uv run python -m data_generation.stream_producer \
      --data-dir ./afyabima_data \
      --bootstrap-servers localhost:9092 \
      --speed 1.0

  # Replay as fast as possible (no delay between days)
  uv run python -m data_generation.stream_producer \
      --data-dir ./afyabima_data \
      --bootstrap-servers localhost:9092 \
      --speed 0

  # Resume from a specific date (skips earlier records)
  uv run python -m data_generation.stream_producer \
      --data-dir ./afyabima_data \
      --bootstrap-servers localhost:9092 \
      --speed 1.0 \
      --from-date 2024-06-01
"""

import argparse
import csv
import datetime
import json
import logging
import sys
import time
from collections import defaultdict
from pathlib import Path
from typing import Any

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Topic routing
# Each CSV file maps to a Kafka topic.
# Reference tables are published once at startup (no date filtering).
# Transactional tables are published day-by-day in chronological order.
# ---------------------------------------------------------------------------

TOPIC_PREFIX = "afyabima.raw"

# Reference tables — published once at startup, keyed by their natural PK
REFERENCE_FILES: dict[str, str] = {
    "plans.csv": f"{TOPIC_PREFIX}.plans",
    "employers.csv": f"{TOPIC_PREFIX}.employers",
    "drug_formulary.csv": f"{TOPIC_PREFIX}.drug_formulary",
    "providers.csv": f"{TOPIC_PREFIX}.providers",
    "members.csv": f"{TOPIC_PREFIX}.members",
    "seeds/diagnosis_code_lookup.csv": f"{TOPIC_PREFIX}.icd10_codes",
}

# Column that maps to the icd10_codes PK (seed file uses "diagnosis_code")
ICD10_REMAP = {"diagnosis_code": "icd10_code"}

# Transactional files — streamed chronologically, keyed by their date column
TRANSACTIONAL_FILES: dict[str, tuple[str, str]] = {
    # filename          : (kafka_topic,                          date_column)
    "claims.csv": (f"{TOPIC_PREFIX}.claims", "date_of_service"),
    "claim_events.csv": (f"{TOPIC_PREFIX}.claim_events", "event_at"),
    "vitals.csv": (f"{TOPIC_PREFIX}.vitals", "date_taken"),
    "prescriptions.csv": (f"{TOPIC_PREFIX}.prescriptions", "prescribed_at"),
    "payments.csv": (f"{TOPIC_PREFIX}.payments", "paid_at"),
    "stock_levels.csv": (f"{TOPIC_PREFIX}.stock_levels", "reported_date"),
    "fraud_investigations.csv": (f"{TOPIC_PREFIX}.fraud_investigations", "opened_at"),
    "fraud_investigation_outcomes.csv": (
        f"{TOPIC_PREFIX}.fraud_investigation_outcomes",
        "confirmed_at",
    ),
}


def _load_csv(path: Path, column_remap: dict[str, str] | None = None) -> list[dict[str, str | None]]:
    """Read a CSV file, remapping column names if needed. Empty strings → None."""
    rows = []
    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for raw_row in reader:
            row: dict[str, str | None] = {}
            for k, v in raw_row.items():
                key = (column_remap or {}).get(k, k)
                row[key] = None if v == "" else v
            rows.append(row)
    return rows


def _date_key(value: str | None) -> datetime.date:
    """Parse a date or datetime string to a date for bucketing."""
    if not value:
        return datetime.date.min
    # Handle both "2024-01-15" and "2024-01-15T09:00:00"
    return datetime.date.fromisoformat(value[:10])


def publish_reference_tables(producer: Any, data_dir: Path) -> None:
    """Publish all reference/master data tables to Kafka before streaming events."""
    logger.info("Publishing reference tables...")
    for filename, topic in REFERENCE_FILES.items():
        path = data_dir / filename
        if not path.exists():
            logger.warning("  Skipping %s — file not found", filename)
            continue

        remap = ICD10_REMAP if "diagnosis_code_lookup" in filename else None
        rows = _load_csv(path, column_remap=remap)

        for row in rows:
            # Use the first column value as the Kafka message key (natural PK)
            pk = next(iter(row.values()))
            producer.send(
                topic,
                key=str(pk).encode("utf-8"),
                value=json.dumps(row, default=str).encode("utf-8"),
            )

        producer.flush()
        logger.info("  ✓ %s → %s  (%d records)", filename, topic, len(rows))


def stream_transactional_data(
    producer: Any,
    data_dir: Path,
    speed: float,
    from_date: datetime.date | None,
) -> None:
    """
    Stream transactional records to Kafka in chronological order.

    speed=0  → as fast as possible (no sleep between days)
    speed=1  → 1 simulated day per real second
    speed=N  → N simulated days per real second

    Buckets all transactional records by date, then emits one day at a time
    so a Kafka consumer sees a realistic arrival pattern.
    """
    logger.info("Indexing transactional records by date...")

    # { date: { topic: [rows] } }
    daily_buckets: dict[datetime.date, dict[str, list[dict[str, str | None]]]] = defaultdict(
        lambda: defaultdict(list)
    )

    for filename, (topic, date_col) in TRANSACTIONAL_FILES.items():
        path = data_dir / filename
        if not path.exists():
            logger.warning("  Skipping %s — file not found", filename)
            continue

        rows = _load_csv(path)
        for row in rows:
            d = _date_key(row.get(date_col))
            daily_buckets[d][topic].append(row)

    all_dates = sorted(daily_buckets.keys())
    if from_date:
        all_dates = [d for d in all_dates if d >= from_date]

    logger.info(
        "Streaming %d days of transactional data (%s → %s)...",
        len(all_dates),
        all_dates[0] if all_dates else "?",
        all_dates[-1] if all_dates else "?",
    )

    for i, day in enumerate(all_dates):
        day_records = daily_buckets[day]
        total_records = sum(len(v) for v in day_records.values())

        for topic, rows in day_records.items():
            for row in rows:
                # Use the first column value as the Kafka message key
                pk = next(iter(row.values()))
                producer.send(
                    topic,
                    key=str(pk).encode("utf-8") if pk else None,
                    value=json.dumps(row, default=str).encode("utf-8"),
                )

        producer.flush()

        if i % 30 == 0:
            logger.info(
                "  Day %d/%d  (%s)  records emitted today: %d",
                i + 1,
                len(all_dates),
                day,
                total_records,
            )

        if speed and speed > 0:
            time.sleep(1.0 / speed)

    logger.info("Streaming complete.")


def run(
    data_dir: str | Path,
    bootstrap_servers: str,
    speed: float = 1.0,
    from_date: datetime.date | None = None,
) -> None:
    """
    Entry point for the streaming producer.
    Imports kafka-python lazily so the rest of the module is importable
    without kafka-python installed (useful for unit tests).
    """
    try:
        from kafka import KafkaProducer
    except ImportError:
        logger.error("kafka-python is not installed. Run: uv add kafka-python")
        sys.exit(1)

    data_dir = Path(data_dir)

    producer = KafkaProducer(
        bootstrap_servers=bootstrap_servers,
        # Batch settings for throughput
        batch_size=65536,  # 64 KB batches
        linger_ms=10,  # wait up to 10ms to fill a batch
        # Reliability
        acks="all",  # wait for all in-sync replicas
        retries=5,
        max_in_flight_requests_per_connection=1,  # preserve ordering
        # Compression
        compression_type="snappy",
    )

    try:
        publish_reference_tables(producer, data_dir)
        stream_transactional_data(producer, data_dir, speed, from_date)
    finally:
        producer.close()


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        prog="stream_producer",
        description="AfyaBima Kafka Streaming Producer — replays generated CSVs as Kafka events",
    )
    parser.add_argument(
        "--data-dir",
        type=str,
        default="./afyabima_data",
        metavar="PATH",
        help="Directory containing generator CSV output (default: ./afyabima_data)",
    )
    parser.add_argument(
        "--bootstrap-servers",
        type=str,
        default="localhost:9092",
        metavar="HOST:PORT",
        help="Kafka bootstrap servers (default: localhost:9092)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        metavar="N",
        help="Playback speed: simulated days per real second. 0 = as fast as possible (default: 1.0)",
    )
    parser.add_argument(
        "--from-date",
        type=str,
        default=None,
        metavar="YYYY-MM-DD",
        help="Start streaming from this date (skip earlier records). Default: stream all dates.",
    )
    return parser.parse_args(argv)


if __name__ == "__main__":
    args = _parse_args()
    from_date = datetime.date.fromisoformat(args.from_date) if args.from_date else None
    run(
        data_dir=args.data_dir,
        bootstrap_servers=args.bootstrap_servers,
        speed=args.speed,
        from_date=from_date,
    )
