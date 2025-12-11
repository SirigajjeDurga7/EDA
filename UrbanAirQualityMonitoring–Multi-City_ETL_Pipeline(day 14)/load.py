import os
import math
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from supabase import create_client
from time import sleep


# Load environment variables
load_dotenv()

BASE_DIR = Path(__file__).resolve().parents[0]
STAGED_DIR = BASE_DIR / "data" / "staged"

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")  # service role key required

if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("❌ Missing SUPABASE_URL or SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)

TABLE_NAME = "air_quality_data"


# Create Table SQL
CREATE_TABLE_SQL = f"""
CREATE TABLE IF NOT EXISTS public.{TABLE_NAME} (
    id BIGSERIAL PRIMARY KEY,
    city TEXT,
    time TIMESTAMP,
    pm10 DOUBLE PRECISION,
    pm2_5 DOUBLE PRECISION,
    carbon_monoxide DOUBLE PRECISION,
    nitrogen_dioxide DOUBLE PRECISION,
    sulphur_dioxide DOUBLE PRECISION,
    ozone DOUBLE PRECISION,
    uv_index DOUBLE PRECISION,
    aqi_category TEXT,
    severity_score DOUBLE PRECISION,
    risk_flag TEXT,
    hour INTEGER
);
"""


def create_table_if_not_exists():
    """
    Attempt RPC create.
    If not supported, print SQL for manual execution.
    """
    print("🔧 Trying to create table via RPC...")
    try:
        supabase.rpc("execute_sql", {"query": CREATE_TABLE_SQL}).execute()
        print("✅ Table create RPC executed (or table already exists).")
    except Exception as e:
        print(f"⚠️ RPC create failed: {e}")
        print("\n➡️ Run this SQL manually in Supabase SQL Editor:\n")
        print(CREATE_TABLE_SQL)


# Read CSV + Fix Column Names
def _read_staged_csv(staged_path: str) -> pd.DataFrame:
    df = pd.read_csv(staged_path, parse_dates=["time"])

    # FIX COLUMN NAMES TO MATCH SUPABASE EXACTLY
    rename_map = {
        "AQI_Category": "aqi_category",
        "Pollution_Severity": "severity_score",
        "Risk_Level": "risk_flag",
        "pm2.5": "pm2_5",
        "PM2.5": "pm2_5",
        "PM10": "pm10",
        "Carbon_Monoxide": "carbon_monoxide",
        "Nitrogen_Dioxide": "nitrogen_dioxide",
        "Sulphur_Dioxide": "sulphur_dioxide",
        "Ozone": "ozone",
        "UV_Index": "uv_index",
        "Hour": "hour",
        "City": "city"
    }

    df.rename(columns=rename_map, inplace=True)

    # Convert datetime to ISO string
    df["time"] = df["time"].astype(str)

    # Convert NaN → None
    df = df.where(pd.notnull(df), None)
    return df


# Upload In Batches to Supabase
def load_to_supabase(staged_csv_path: str, batch_size: int = 200):
    df = _read_staged_csv(staged_csv_path)
    total = len(df)
    print(f"📦 Loading {total} rows → {TABLE_NAME} (batch size = {batch_size})")

    records = df.to_dict(orient="records")

    for i in range(0, total, batch_size):
        batch = records[i:i + batch_size]

        try:
            res = supabase.table(TABLE_NAME).insert(batch).execute()

            if hasattr(res, "error") and res.error:
                print(f"⚠️ Insert failed in batch {i//batch_size + 1}: {res.error}")
                print("⏳ Retrying in 3 seconds...")
                sleep(3)
                supabase.table(TABLE_NAME).insert(batch).execute()
                print("✅ Retry success")
            else:
                print(f"✅ Inserted batch {i//batch_size + 1}")

        except Exception as e:
            print(f"❌ Retry failed: {e}")
            continue

    print("\n🎯 Load complete.")


if __name__ == "__main__":
    create_table_if_not_exists()

    staged_files = sorted(list(STAGED_DIR.glob("air_quality_transformed.csv")))
    if not staged_files:
        raise SystemExit("❌ No staged file found. Run transform.py first.")

    load_to_supabase(str(staged_files[-1]), batch_size=200)
