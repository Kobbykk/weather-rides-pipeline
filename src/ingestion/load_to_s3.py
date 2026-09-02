"""Publish cleaned trip data to S3 as Hive-partitioned Parquet."""

import argparse
import logging
import os
import sys
from pathlib import Path

import boto3
import pandas as pd
from dotenv import load_dotenv

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_to_s3")

DATA_DIR = Path(__file__).resolve().parents[2] / "data"
RAW_DIR = DATA_DIR / "raw"
STAGE_DIR = DATA_DIR / "staged"
PREFIX = "trips"

COLUMNS = [
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "tip_amount",
    "total_amount",
]


def transform(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Apply the same cleaning rules as the Postgres path."""
    before = len(df)
    df = df[COLUMNS].copy()
    df.columns = [c.lower() for c in df.columns]

    df = df[
        (df["trip_distance"] > 0)
        & (df["total_amount"] > 0)
        & (df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"])
    ]

   

    log.info("Kept %s of %s rows", f"{len(df):,}", f"{before:,}")
    return df


def write_partition(df: pd.DataFrame, month: str) -> Path:
    """Write one Parquet file into the local partition path."""
    year, mon = month.split("-")
    part_dir = STAGE_DIR / PREFIX / f"year={year}" / f"month={mon}"
    part_dir.mkdir(parents=True, exist_ok=True)

    target = part_dir / "trips.parquet"
    df.to_parquet(target, engine="pyarrow", compression="snappy", index=False)

    size_mb = target.stat().st_size / 1_000_000
    log.info("Wrote %s (%.1f MB, snappy)", target.name, size_mb)
    return target


def upload(local_path: Path, bucket: str, month: str) -> str:
    """Upload to the matching S3 key. Overwrites, so reruns are idempotent."""
    year, mon = month.split("-")
    key = f"{PREFIX}/year={year}/month={mon}/trips.parquet"

    s3 = boto3.client("s3")
    s3.upload_file(str(local_path), bucket, key)

    uri = f"s3://{bucket}/{key}"
    log.info("Uploaded to %s", uri)
    return uri


def main() -> int:
    parser = argparse.ArgumentParser(description="Publish trips to S3.")
    parser.add_argument("--month", required=True, help="Month to publish, e.g. 2024-01")
    parser.add_argument("--bucket", help="Target bucket (or set S3_BUCKET).")
    args = parser.parse_args()

    load_dotenv()
    bucket = args.bucket or os.environ.get("S3_BUCKET")
    if not bucket:
        log.error("No bucket given. Pass --bucket or set S3_BUCKET.")
        return 1

    source = RAW_DIR / f"yellow_tripdata_{args.month}.parquet"
    if not source.exists():
        log.error("Missing %s. Run load_trips.py first to download it.", source)
        return 1

    df = transform(pd.read_parquet(source), args.month)
    local = write_partition(df, args.month)
    upload(local, bucket, args.month)
    return 0


if __name__ == "__main__":
    sys.exit(main())