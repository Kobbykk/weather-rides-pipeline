"""Extract NYC TLC yellow taxi trips and load them into Postgres."""

import argparse
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_trips")

BASE_URL = "https://d37ci6vzurychx.cloudfront.net/trip-data"
DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "raw"
TABLE = "raw_yellow_trips"
CHUNK_SIZE = 50_000

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


def get_engine():
    load_dotenv()
    user = os.environ["POSTGRES_USER"]
    password = os.environ["POSTGRES_PASSWORD"]
    host = os.environ["POSTGRES_HOST"]
    port = os.environ["POSTGRES_PORT"]
    db = os.environ["POSTGRES_DB"]
    return create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}")


def download(month: str) -> Path:
    """Download one month of trip data, skipping if already on disk."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    target = DATA_DIR / f"yellow_tripdata_{month}.parquet"

    if target.exists():
        log.info("Already downloaded: %s", target.name)
        return target

    url = f"{BASE_URL}/yellow_tripdata_{month}.parquet"
    log.info("Downloading %s", url)
    response = requests.get(url, stream=True, timeout=120)
    response.raise_for_status()

    with target.open("wb") as f:
        for block in response.iter_content(chunk_size=1024 * 1024):
            f.write(block)

    log.info("Saved %s (%.1f MB)", target.name, target.stat().st_size / 1_000_000)
    return target


def transform(df: pd.DataFrame, month: str) -> pd.DataFrame:
    """Trim to needed columns, drop junk rows, tag with the source month."""
    before = len(df)
    df = df[COLUMNS].copy()
    df.columns = [c.lower() for c in df.columns]

    df = df[
        (df["trip_distance"] > 0)
        & (df["total_amount"] > 0)
        & (df["tpep_pickup_datetime"] < df["tpep_dropoff_datetime"])
    ]
    df["source_month"] = month

    log.info("Kept %s of %s rows (%.1f%% dropped)",
             f"{len(df):,}", f"{before:,}", 100 * (1 - len(df) / before))
    return df


def load(df: pd.DataFrame, engine, month: str) -> None:
    """Idempotent load: clear this month's rows, then insert."""
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT to_regclass(:t)"), {"t": TABLE}
        ).scalar()
        if exists:
            deleted = conn.execute(
                text(f"DELETE FROM {TABLE} WHERE source_month = :m"), {"m": month}
            ).rowcount
            log.info("Removed %s existing rows for %s", f"{deleted:,}", month)

    df.to_sql(TABLE, engine, if_exists="append", index=False, chunksize=CHUNK_SIZE)
    log.info("Loaded %s rows into %s", f"{len(df):,}", TABLE)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load NYC taxi trips into Postgres.")
    parser.add_argument("--month", required=True, help="Month to load, e.g. 2024-01")
    args = parser.parse_args()

    engine = get_engine()
    path = download(args.month)
    df = pd.read_parquet(path)
    df = transform(df, args.month)
    load(df, engine, args.month)

    with engine.connect() as conn:
        total = conn.execute(text(f"SELECT count(*) FROM {TABLE}")).scalar()
    log.info("Done. Table now holds %s rows.", f"{total:,}")
    return 0


if __name__ == "__main__":
    sys.exit(main())