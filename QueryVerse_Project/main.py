import streamlit as st
import random
from modules.query_optimizer import streamlit_page as query_optimizer  # assuming file renamed to streamlit_page.py

# --- MUST BE FIRST Streamlit call ---
st.set_page_config(page_title="QueryVerse Home", layout="wide")

# --- Title ---
st.markdown("<h1 style='text-align: center; color: cyan;'>🚀 QueryVerse</h1>", unsafe_allow_html=True)
st.markdown("Start your day with a quick view of query health, cost, and system anomalies.", unsafe_allow_html=True)

# --- KPI Dashboard ---
st.markdown("## 📊 System Snapshot")
col1, col2, col3 = st.columns(3)
with col1:
    st.metric("Total Queries (Today)", f"{random.randint(1000, 1500)}")
with col2:
    st.metric("Estimated Cost (₹)", f"₹{random.uniform(3000, 5000):,.2f}")
with col3:
    st.metric("Anomalies Detected", random.randint(5, 15))

# --- Module Navigator ---
st.markdown("---")
st.markdown("## 🧭 Explore Modules")

selected = st.selectbox(
    "Choose a module to open",
    ["Home", "🧠 Query Optimizer", "🔍 Anomaly Detection (Coming Soon)", "📉 Cost Forecasting (Coming Soon)"]
)

# --- Module Routing ---
if selected == "🧠 Query Optimizer":
    query_optimizer.render()  # Make sure streamlit_page.py defines a `render()` function
elif selected == "🔍 Anomaly Detection (Coming Soon)":
    st.info("🚧 Anomaly Detection module is under development.")
elif selected == "📉 Cost Forecasting (Coming Soon)":
    st.info("📊 Cost Forecasting module is under development.")
else:
    st.success("Welcome to QueryVerse! Select a module to begin.")

# --- Footer ---
st.markdown("---")
st.caption("QueryVerse MVP v0.1 • Spark & Himanshu | Powered by Krishnity")

