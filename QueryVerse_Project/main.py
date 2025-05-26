import streamlit as st
import random
import snowflake.connector
from modules.query_optimizer import streamlit_page as query_optimizer

# --- Page Config ---
st.set_page_config(page_title="QueryVerse", layout="wide")

# --- Sidebar (like dbt style) ---
with st.sidebar:
    st.markdown("<h2 style='color:#4CAF50;'>⚡ QueryVerse</h2>", unsafe_allow_html=True)
    
    st.markdown("#### 🧭 Navigation")
    selected_tab = st.radio(
        "Navigate",
        [
            "🏠 Home",
            "🧩 Connection",
            "🧠 Query Optimizer",
            "🔍 Anomaly Detection",
            "📉 Cost Forecasting"
        ],
        label_visibility="collapsed"
    )

    st.markdown("---")
    st.markdown("#### ⚙️ Settings")
    st.markdown("- Profile")
    st.markdown("- API Tokens")
    st.markdown("- Help & Support")

    st.markdown("---")
    st.markdown("👤 **Himanshu Gaikwad**", unsafe_allow_html=True)

# --- MAIN PANEL ---
st.markdown(f"<h1 style='text-align: center; color: #1976D2;'>🚀 {selected_tab}</h1>", unsafe_allow_html=True)

# --- HOME TAB ---
if selected_tab == "🏠 Home":
    st.markdown("Welcome to **QueryVerse**! Begin by configuring your connection or selecting a module.")

    st.markdown("## 📊 System Snapshot")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Queries (Today)", f"{random.randint(1000, 1500)}")
    with col2:
        st.metric("Estimated Cost (₹)", f"₹{random.uniform(3000, 5000):,.2f}")
    with col3:
        st.metric("Anomalies Detected", random.randint(5, 15))

# --- CONNECTION TAB ---
elif selected_tab == "🧩 Connection":
    st.subheader("🧩 Snowflake Connection Settings")

    config = st.session_state.get("snowflake_config", {})

    sf_account = st.text_input("Account", value=config.get("account", ""))
    sf_user = st.text_input("Username", value=config.get("user", ""))
    sf_password = st.text_input("Password", type="password", value=config.get("password", ""))
    sf_warehouse = st.text_input("Warehouse", value=config.get("warehouse", ""))
    sf_database = st.text_input("Database", value=config.get("database", ""))
    sf_schema = st.text_input("Schema", value=config.get("schema", ""))
    sf_role = st.text_input("Role (Optional)", value=config.get("role", ""))

    if st.button("Save Connection"):
        st.session_state["snowflake_config"] = {
            "account": sf_account,
            "user": sf_user,
            "password": sf_password,
            "warehouse": sf_warehouse,
            "database": sf_database,
            "schema": sf_schema,
            "role": sf_role
        }
        st.success("✅ Connection saved in session")

    if st.button("🔗 Test Connection"):
        try:
            conn = snowflake.connector.connect(
                user=sf_user,
                password=sf_password,
                account=sf_account,
                warehouse=sf_warehouse,
                database=sf_database,
                schema=sf_schema,
                role=sf_role if sf_role else None
            )
            conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP();")
            st.success("✅ Connection successful!")
            conn.close()
        except Exception as e:
            st.error(f"❌ Connection failed: {e}")

# --- QUERY OPTIMIZER TAB ---
elif selected_tab == "🧠 Query Optimizer":
    query_optimizer.render()

# --- ANOMALY DETECTION (COMING SOON) ---
elif selected_tab == "🔍 Anomaly Detection":
    st.info("🚧 Anomaly Detection module is under development.")

# --- COST FORECASTING (COMING SOON) ---
elif selected_tab == "📉 Cost Forecasting":
    st.info("📊 Cost Forecasting module is under development.")

# --- FOOTER ---
st.markdown("---")
st.caption("QueryVerse MVP v0.1 • Spark & Himanshu | Powered by Krishnity")
