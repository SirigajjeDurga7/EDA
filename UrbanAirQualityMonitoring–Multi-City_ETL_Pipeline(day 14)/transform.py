import pandas as pd
import json
from pathlib import Path
from datetime import datetime


# Folder setup
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR / "data" / "raw"
STAGED_DIR = BASE_DIR / "data" / "staged"
STAGED_DIR.mkdir(parents=True, exist_ok=True)


# AQI Category Function
def aqi_pm25_category(pm25):
    if pm25 <= 50:
        return "Good"
    elif pm25 <= 100:
        return "Moderate"
    elif pm25 <= 200:
        return "Unhealthy"
    elif pm25 <= 300:
        return "Very Unhealthy"
    else:
        return "Hazardous"


# Pollution Severity Score
def compute_severity(row):
    return (
        row.get("pm2_5", 0) * 5 +
        row.get("pm10", 0) * 3 +
        row.get("nitrogen_dioxide", 0) * 4 +
        row.get("sulphur_dioxide", 0) * 4 +
        row.get("carbon_monoxide", 0) * 2 +
        row.get("ozone", 0) * 3
    )


# Risk Classification
def risk_classification(severity):
    if severity > 400:
        return "High Risk"
    elif severity > 200:
        return "Moderate Risk"
    else:
        return "Low Risk"


# Main Transform Function
def transform_pipeline():
    all_records = []

    # Iterate over all raw JSON files
    for json_file in RAW_DIR.glob("*_raw_*.json"):
        city_name = json_file.stem.split("_raw_")[0].capitalize()

        with open(json_file, "r", encoding="utf-8") as f:
            data = json.load(f)

        hourly_data = data.get("hourly", {})
        if not hourly_data:
            continue

        # Get list of hours
        times = hourly_data.get("time", [])
        for i, t in enumerate(times):
            record = {"city": city_name, "time": pd.to_datetime(t)}

            # Extract pollutant values safely
            for pollutant in ["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone","uv_index"]:
                values = hourly_data.get(pollutant, [])
                record[pollutant] = pd.to_numeric(values[i]) if i < len(values) else None

            all_records.append(record)

    # Create DataFrame
    df = pd.DataFrame(all_records)

    # Remove rows where all pollutant readings are missing
    pollutant_cols = ["pm10","pm2_5","carbon_monoxide","nitrogen_dioxide","sulphur_dioxide","ozone","uv_index"]
    df = df.dropna(subset=pollutant_cols, how="all")

    # Feature Engineering
    df["AQI_Category"] = df["pm2_5"].apply(aqi_pm25_category)
    df["Pollution_Severity"] = df.apply(compute_severity, axis=1)
    df["Risk_Level"] = df["Pollution_Severity"].apply(risk_classification)
    df["hour"] = df["time"].dt.hour

    # Save transformed data
    output_file = STAGED_DIR / f"air_quality_transformed.csv"
    df.to_csv(output_file, index=False)
    print(f"💾 Transformed data saved → {output_file}")
    return df


# Run transformation
if __name__ == "__main__":
    transform_pipeline()
