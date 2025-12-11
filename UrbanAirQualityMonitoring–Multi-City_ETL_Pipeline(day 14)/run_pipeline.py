# run_pipeline.py

from extract import extract_all
from transform import transform_pipeline
from load import load_to_supabase, create_table_if_not_exists
from etl_analysis import etl_analysis_pipeline
from pathlib import Path


def run_pipeline():
    print("\n🚀 Starting FULL ETL + Analysis Pipeline...\n")

    # 1️⃣ Extract
    print("📥 STEP 1: Extracting data...")
    extract_all()

    # 2️⃣ Transform
    print("\n🔄 STEP 2: Transforming data...")
    df = transform_pipeline()

    # 3️⃣ Load
    print("\n🗄️ STEP 3: Loading data into Supabase...")

    # Ensure table exists in Supabase
    create_table_if_not_exists()

    staged_file = Path("data/staged/air_quality_transformed.csv")
    load_to_supabase(str(staged_file))

    # 4️⃣ Analysis
    print("\n📊 STEP 4: Running Analysis...")
    etl_analysis_pipeline()

    print("\n🎉 ETL + Analysis Pipeline Completed Successfully!\n")


if __name__ == "__main__":
    run_pipeline()
