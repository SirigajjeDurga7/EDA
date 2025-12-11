import requests
import json
import logging
import time
from pathlib import Path
from datetime import datetime


# Folder setup
BASE_DIR = Path(__file__).resolve().parents[0]
RAW_DIR = BASE_DIR /"data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# Logging setup
LOG_DIR = BASE_DIR / "logs"
LOG_DIR.mkdir(parents=True, exist_ok=True)

logging.basicConfig(
    filename=LOG_DIR / "extract.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s"
)


# Cities with coordinates
CITIES = {
    "Delhi": {"lat": 28.7041, "lon": 77.1025},
    "Mumbai": {"lat": 19.0760, "lon": 72.8777},
    "Bengaluru": {"lat": 12.9716, "lon": 77.5946},
    "Hyderabad": {"lat": 17.3850, "lon": 78.4867},
    "Kolkata": {"lat": 22.5726, "lon": 88.3639}
}

# Pollutants to fetch
POLLUTANTS = "pm10,pm2_5,carbon_monoxide,nitrogen_dioxide,ozone,sulphur_dioxide,uv_index"


# Helper: API request with retry
def fetch_city_data(city_name, lat, lon, retries=3, wait=2):
    url = f"https://air-quality-api.open-meteo.com/v1/air-quality?latitude={lat}&longitude={lon}&hourly={POLLUTANTS}"
    
    for attempt in range(1, retries + 1):
        try:
            print(f"⏳ Requesting {city_name} AQI (Attempt {attempt}/{retries})")
            response = requests.get(url, timeout=20)
            response.raise_for_status()
            data = response.json()

            if not data.get("hourly"):
                logging.warning(f"Empty response for {city_name}")
                print(f"⚠️ Warning: Empty response for {city_name}")

            logging.info(f"SUCCESS - Fetched data for {city_name}")
            return data

        except Exception as e:
            logging.error(f"ERROR - Failed to fetch {city_name} | Attempt {attempt} | Error: {e}")
            print(f"⚠️ Attempt {attempt} failed for {city_name}. Retrying in {wait}s...")
            time.sleep(wait)

    print(f"❌ Failed to fetch {city_name} data after {retries} attempts.")
    logging.error(f"FAILED - {city_name} data not fetched after {retries} attempts.")
    return None


# Main extraction
def extract_all():
    print("🚀 Starting Open-Meteo AQI Extraction Pipeline...")
    saved_files = []

    for city, coords in CITIES.items():
        data = fetch_city_data(city, coords["lat"], coords["lon"])
        if data:
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            filename = RAW_DIR / f"{city.lower()}_raw_{timestamp}.json"
            with open(filename, "w", encoding="utf-8") as f:
                json.dump(data, f, indent=2)
            print(f"💾 Saved {city} AQI data → {filename}")
            logging.info(f"Saved file: {filename}")
            saved_files.append(str(filename))
    
    print("\n✅ Extraction Completed Successfully!")
    return saved_files


# Run extraction if script is executed
if __name__ == "__main__":
    extract_all()
