# etl_analysis.py
import os
from pathlib import Path
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from supabase import create_client
import matplotlib.pyplot as plt

# Load env
load_dotenv()
BASE_DIR = Path(__file__).resolve().parents[0]
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

SUPABASE_URL = os.getenv("SUPABASE_URL")
SUPABASE_KEY = os.getenv("SUPABASE_KEY")
if not SUPABASE_URL or not SUPABASE_KEY:
    raise SystemExit("Please set SUPABASE_URL and SUPABASE_KEY in .env")

supabase = create_client(SUPABASE_URL, SUPABASE_KEY)
TABLE_NAME = "air_quality_data"

def fetch_all_from_supabase() -> pd.DataFrame:
    """Fetch all rows from Supabase table and return DataFrame."""
    print("📡 Fetching data from Supabase...")
    # fetch (simple approach)
    res = supabase.table(TABLE_NAME).select("*").execute()
    if hasattr(res, "error") and res.error:
        raise RuntimeError(f"Supabase error: {res.error}")
    data = res.data if hasattr(res, "data") else res
    df = pd.DataFrame(data)
    if df.empty:
        print("⚠️ No data found in Supabase table.")
        return df
    # Parse time column to datetime
    if "time" in df.columns:
        df["time"] = pd.to_datetime(df["time"], errors="coerce")
    return df

def compute_kpis(df: pd.DataFrame) -> pd.DataFrame:
    """Compute KPI metrics and return a single-row summary (as DataFrame)."""
    metrics = {}
    # City with highest average PM2.5
    pm25_by_city = df.groupby("city")["pm2_5"].mean().dropna()
    if not pm25_by_city.empty:
        metrics["city_highest_avg_pm2_5"] = pm25_by_city.idxmax()
        metrics["highest_avg_pm2_5_value"] = pm25_by_city.max()
    else:
        metrics["city_highest_avg_pm2_5"] = None
        metrics["highest_avg_pm2_5_value"] = None

    # City with highest average severity score
    sev_by_city = df.groupby("city")["severity_score"].mean().dropna()
    if not sev_by_city.empty:
        metrics["city_highest_severity"] = sev_by_city.idxmax()
        metrics["highest_severity_value"] = sev_by_city.max()
    else:
        metrics["city_highest_severity"] = None
        metrics["highest_severity_value"] = None

    # Percentage of High/Moderate/Low risk hours (overall)
    risk_counts = df["risk_flag"].value_counts(dropna=True)
    total = risk_counts.sum() if not risk_counts.empty else 0
    for flag in ["High Risk", "Moderate Risk", "Low Risk"]:
        metrics[f"pct_{flag.replace(' ', '_').lower()}"] = (risk_counts.get(flag, 0) / total * 100) if total > 0 else 0.0

    # Hour of day with worst AQI (highest average pm2_5)
    if "hour" in df.columns:
        hour_pm25 = df.groupby("hour")["pm2_5"].mean().dropna()
        metrics["worst_aqi_hour"] = int(hour_pm25.idxmax()) if not hour_pm25.empty else None
        metrics["worst_aqi_hour_avg_pm2_5"] = float(hour_pm25.max()) if not hour_pm25.empty else None
    else:
        metrics["worst_aqi_hour"] = None
        metrics["worst_aqi_hour_avg_pm2_5"] = None

    return pd.DataFrame([metrics])

def city_risk_distribution(df: pd.DataFrame) -> pd.DataFrame:
    """Return a DataFrame with risk distribution % per city."""
    # Count per city per risk_flag
    counts = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    totals = counts.sum(axis=1)
    pct = counts.div(totals, axis=0) * 100
    pct = pct.reset_index().fillna(0)
    return pct

def pollution_trends(df: pd.DataFrame) -> pd.DataFrame:
    """
    Build long-form trends for each city with columns:
    city, time, pm2_5, pm10, ozone
    """
    cols = ["city", "time", "pm2_5", "pm10", "ozone"]
    out = df[cols].copy()
    out = out.sort_values(["city", "time"])
    return out

def save_csvs(summary_df: pd.DataFrame, risk_df: pd.DataFrame, trends_df: pd.DataFrame):
    summary_df.to_csv(PROCESSED_DIR / "summary_metrics.csv", index=False)
    risk_df.to_csv(PROCESSED_DIR / "city_risk_distribution.csv", index=False)
    trends_df.to_csv(PROCESSED_DIR / "pollution_trends.csv", index=False)
    print("💾 Saved CSV outputs to data/processed/")

def make_plots(df: pd.DataFrame, trends_df: pd.DataFrame):
    # Histogram of PM2.5
    plt.figure()
    plt.hist(df["pm2_5"].dropna(), bins=30)
    plt.title("Histogram of PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Frequency")
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "hist_pm2_5.png")
    plt.close()

    # Bar chart of risk flags per city (counts)
    risk_counts = df.groupby(["city", "risk_flag"]).size().unstack(fill_value=0)
    plt.figure()
    risk_counts.plot(kind="bar", stacked=False)
    plt.title("Risk Flags per City")
    plt.xlabel("City")
    plt.ylabel("Count")
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "bar_risk_per_city.png")
    plt.close()

    # Line chart of hourly PM2.5 trends (aggregate hourly average by city)
    plt.figure()
    for city, g in df.groupby("city"):
        hourly_avg = g.groupby(g["time"].dt.hour)["pm2_5"].mean()
        plt.plot(hourly_avg.index, hourly_avg.values, label=city)
    plt.title("Hourly Average PM2.5 by City (Hour of Day)")
    plt.xlabel("Hour of day")
    plt.ylabel("Avg PM2.5")
    plt.legend()
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "line_hourly_pm2_5_trends.png")
    plt.close()

    # Scatter: severity_score vs pm2_5
    plt.figure()
    plt.scatter(df["pm2_5"].dropna(), df["severity_score"].dropna())
    plt.title("Severity Score vs PM2.5")
    plt.xlabel("PM2.5")
    plt.ylabel("Severity Score")
    plt.tight_layout()
    plt.savefig(PROCESSED_DIR / "scatter_severity_vs_pm2_5.png")
    plt.close()

    print("🖼️ Saved plots to data/processed/")

def etl_analysis_pipeline():
    df = fetch_all_from_supabase()
    if df.empty:
        print("No data to analyze. Exiting.")
        return

    summary = compute_kpis(df)
    risk_pct = city_risk_distribution(df)
    trends = pollution_trends(df)

    save_csvs(summary, risk_pct, trends)
    make_plots(df, trends)

    print("\n📊 Analysis complete. Files written to data/processed/")

if __name__ == "__main__":
    etl_analysis_pipeline()
