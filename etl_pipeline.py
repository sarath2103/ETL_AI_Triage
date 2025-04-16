# etl_pipeline.py

import pandas as pd
from datetime import datetime
from config import SOURCE_CSV_PATH, TARGET_CSV_PATH, INPUT_COLUMNS, OUTPUT_COLUMNS
from gpt_error_handler import ask_gpt

def extract_data():
    print("📥 Extracting source data...")
    return pd.read_csv(SOURCE_CSV_PATH)

def save_gpt_output_to_file(content, filename="gpt_etl_analysis.md"):
    print("🧠 Saving GPT analysis to markdown...")
    with open(filename, "w", encoding="utf-8") as f:
        f.write("# 📝 Issue Analysis & Fix Suggestions\n\n")
        f.write(content)
        print(f"📄 GPT output saved to: {filename}")

def transform(df):
    print("🔧 Starting data transformation...")
    actual_columns = list(df.columns)
    errors = []

    # Validate schema (based only on input)
    if set(actual_columns) != set(INPUT_COLUMNS):
        print("⚠️ Schema mismatch detected!")
        schema_context = {
            "expected_columns": INPUT_COLUMNS,
            "actual_columns": actual_columns
        }
        explanation = ask_gpt(["Schema mismatch detected."], schema_context)
        save_gpt_output_to_file(explanation)
        raise Exception("Schema mismatch. See GPT report.")

    try:
        print("🔹 Transforming salary...")
        df['salary'] = pd.to_numeric(df['salary']).round(2)
    except Exception as e:
        errors.append(f"Error converting 'salary': {str(e)}")

    try:
        print("🔹 Checking for missing start_date values...")
        if df['start_date'].isnull().any() or (df['start_date'].astype(str).str.strip() == "").any():
            raise ValueError("start_date contains missing or empty values.")

        print("🔹 Parsing start_date with strict validation...")
        df['start_date'] = pd.to_datetime(df['start_date'], errors='raise')
    except Exception as e:
        errors.append(f"Error converting 'start_date': {str(e)}")

    try:
        print("🔹 Calculating experience_years...")
        if df['start_date'].isnull().any():
            raise ValueError("Cannot calculate experience_years — start_date has missing values.")
        df['experience_years'] = (datetime.now() - df['start_date']).dt.days // 365
    except Exception as e:
        errors.append(f"Error calculating 'experience_years': {str(e)}")

    try:
        print("🔹 Creating tenure_band...")
        if df['experience_years'].isnull().any():
            raise ValueError("Cannot create tenure_band — experience_years has missing values.")
        df['tenure_band'] = pd.cut(
            df['experience_years'],
            bins=[0, 2, 5, 10, 20, 100],
            labels=['<2 yrs', '2-5 yrs', '5-10 yrs', '10-20 yrs', '20+ yrs']
        )
    except Exception as e:
        errors.append(f"Error creating 'tenure_band': {str(e)}")

    try:
        print("🔹 Formatting job_title...")
        df['job_title'] = df['job_title'].str.strip().str.title()
    except Exception as e:
        errors.append(f"Error formatting 'job_title': {str(e)}")

    try:
        print("🔹 Formatting department...")
        df['department'] = df['department'].str.strip().str.upper()
    except Exception as e:
        errors.append(f"Error formatting 'department': {str(e)}")

    try:
        print("🔹 Formatting location...")
        df['location'] = df['location'].str.strip().str.title()
    except Exception as e:
        errors.append(f"Error formatting 'location': {str(e)}")

    try:
        print("🔹 Formatting email...")
        df['email'] = df['email'].str.strip().str.lower()
    except Exception as e:
        errors.append(f"Error formatting 'email': {str(e)}")

    try:
        print("✅ Selecting output columns...")
        df = df[OUTPUT_COLUMNS]
    except Exception as e:
        errors.append(f"Error selecting output columns: {str(e)}")

    if errors:
        print("❌ One or more transformation errors found. Sending to GPT...")
        explanation = ask_gpt(errors, actual_columns)
        save_gpt_output_to_file(explanation)
        raise Exception("Transformation failed. See GPT report.")

    print("✅ Transformation completed successfully.")
    return df

def load_data(df):
    print("💾 Saving transformed data to target CSV...")
    df.to_csv(TARGET_CSV_PATH, index=False)
    print(f"🎉 Transformed data saved to: {TARGET_CSV_PATH}")

def main():
    print("🚀 Starting ETL pipeline...\n")
    try:
        df = extract_data()
        transformed_df = transform(df)
        load_data(transformed_df)
        print("\n✅ ETL pipeline completed successfully.")
    except Exception as e:
        print("\n❌ ETL pipeline failed.")
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
