# gpt_error_handler.py

import openai
import os
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY")


def ask_gpt(error_list, schema_info=None):
    joined_errors = "\n".join([f"{i+1}. {err}" for i, err in enumerate(error_list)])

    schema_prompt = ""
    if isinstance(schema_info, dict):
        schema_prompt = f"""
Expected Columns: {schema_info.get('expected_columns')}
Actual Columns in Source File: {schema_info.get('actual_columns')}
"""
    elif isinstance(schema_info, list):
        schema_prompt = f"Columns in Source File: {schema_info}"

    prompt = f"""
You are a data engineering expert helping to analyze errors in an ETL pipeline.

Below are a set of issues that occurred while processing the data. These may be due to:
- 🆕 **New columns added** (e.g., `'age_new'` instead of `'age'`)
- ❌ **Missing or renamed columns**
- ⚠️ **Unexpected columns**

Your job is to provide a response in **markdown** with these clear sections:

### 🔍 **Root Cause**
Briefly describe what likely caused the issue.

### ✅ **Suggested Fix**
Give a short and clear fix (e.g., rename column, remap, drop unused).

### 🧩 **Code Snippets**
Provide **Python (Pandas)** where applicable.

Use column name similarities (e.g., `'age_new'` ≈ `'age'`) to inform your suggestions.

---

**Schema Details**:
{schema_prompt}

**Issues Detected**:
{joined_errors}

Format cleanly for presentation: use **bold**, appropriate emojis, and concise markdown.
"""


    response = openai.ChatCompletion.create(
        model="gpt-4",
        messages=[{"role": "user", "content": prompt}]
    )
    
    return response.choices[0].message.content