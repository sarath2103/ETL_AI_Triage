# config.py

SOURCE_CSV_PATH = "employees_source_with_email.csv"
TARGET_CSV_PATH = "employees_target_output.csv"  # Output file path

INPUT_COLUMNS = [
    'first_name',
    'last_name',
    'email',
    'department',
    'location',
    'job_title',
    'salary',
    'start_date'
]

# ✅ Output schema: input + derived columns
OUTPUT_COLUMNS = INPUT_COLUMNS + [
    'experience_years',
    'tenure_band'
]

