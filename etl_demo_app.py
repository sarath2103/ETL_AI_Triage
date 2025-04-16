# etl_demo_app.py

import streamlit as st
import pandas as pd
from config import SOURCE_CSV_PATH

st.set_page_config(page_title="ETL Error Triage", layout="wide")

# Header
st.markdown("""
# 🧠 ETL Error Triage System
""")

# Source CSV Preview
st.markdown("## 📂 Source CSV Preview")
try:
    df = pd.read_csv(SOURCE_CSV_PATH)
    st.dataframe(df.head(), use_container_width=True)
except Exception as e:
    st.error(f"⚠️ Could not load source file: {e}")

# GPT Output Section
st.markdown("## 📋 ETL Issue Breakdown & Action Plan")

try:
    with open("gpt_etl_analysis.md", "r", encoding="utf-8") as f:
        gpt_output = f.read()
        st.markdown(gpt_output, unsafe_allow_html=True)
except FileNotFoundError:
    st.warning("⚠️ GPT analysis not found. Run the ETL pipeline to generate it.")