"""Fetch daily NYC weather from Open-Meteo and load it into Postgres."""

import argparse
import calendar
import logging
import os
import sys
from datetime import date

import pandas as pd
import requests
from dotenv import load_dotenv
from sqlalchemy import create_engine, text

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s | %(levelname)-8s | %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("load_weather")

API_URL = "https://archive-api.open-meteo.com/v1/archive"
NYC_LAT, NYC_LON = 40.7128, -74.0060
TABLE = "raw_daily_weather"

METRICS = [
    "temperature_2m_max",
    "temperature_2m_min",
    "precipitation_sum",
    "snowfall_sum",
    "wind_speed_10m_max",
]


def get_engine():
    load_dotenv()
    return create_engine(
        "postgresql+psycopg2://{u}:{p}@{h}:{port}/{db}".format(
            u=os.environ["POSTGRES_USER"],
            p=os.environ["POSTGRES_PASSWORD"],
            h=os.environ["POSTGRES_HOST"],
            port=os.environ["POSTGRES_PORT"],
            db=os.environ["POSTGRES_DB"],
        )
    )


def month_bounds(month: str) -> tuple[date, date]:
    year, mon = (int(part) for part in month.split("-"))
    last_day = calendar.monthrange(year, mon)[1]
    return date(year, mon, 1), date(year, mon, last_day)


def extract(month: str) -> pd.DataFrame:
    start, end = month_bounds(month)
    params = {
        "latitude": NYC_LAT,
        "longitude": NYC_LON,
        "start_date": start.isoformat(),
        "end_date": end.isoformat(),
        "daily": ",".join(METRICS),
        "timezone": "America/New_York",
    }

    log.info("Requesting weather for %s", month)
    response = requests.get(API_URL, params=params, timeout=60)
    response.raise_for_status()

    daily = response.json()["daily"]
    df = pd.DataFrame(daily).rename(columns={"time": "weather_date"})
    df["weather_date"] = pd.to_datetime(df["weather_date"]).dt.date
    df["source_month"] = month

    log.info("Received %d days of weather", len(df))
    return df


def load(df: pd.DataFrame, engine, month: str) -> None:
    with engine.begin() as conn:
        if conn.execute(text("SELECT to_regclass(:t)"), {"t": TABLE}).scalar():
            deleted = conn.execute(
                text(f"DELETE FROM {TABLE} WHERE source_month = :m"), {"m": month}
            ).rowcount
            log.info("Removed %d existing rows for %s", deleted, month)

    df.to_sql(TABLE, engine, if_exists="append", index=False)
    log.info("Loaded %d rows into %s", len(df), TABLE)


def main() -> int:
    parser = argparse.ArgumentParser(description="Load NYC daily weather.")
    parser.add_argument("--month", required=True, help="Month to load, e.g. 2024-01")
    args = parser.parse_args()

    engine = get_engine()
    df = extract(args.month)
    load(df, engine, args.month)
    return 0


if __name__ == "__main__":
    sys.exit(main())