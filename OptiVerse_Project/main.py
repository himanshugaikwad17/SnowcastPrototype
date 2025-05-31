import streamlit as st
from datetime import datetime
from modules.api_config.config_manager import get_snowflake_connections, get_api_credentials
from shared.snowflake_connector import connect_to_snowflake

# Page setup
st.set_page_config(page_title="OptiVerse", layout="wide")

# Import modules
from modules.query_optimizer import streamlit_page as query_optimizer
from modules.api_config import streamlit_page as api_config
from modules.stale_tables import stale_tables_page
from modules.anomaly_detection import anomaly_detection

# Credentials
llm_creds = get_api_credentials()
available_providers = list(llm_creds.keys())

# App title
st.markdown(
    "<div style='position: absolute; top: 10px; left: 15px; font-size: 14px; font-weight: bold; color: #6c757d;'>"
    "OptiVerse"
    "</div>",
    unsafe_allow_html=True
)

# --- Session Initialization ---
if "snowflake_connections" not in st.session_state:
    st.session_state.snowflake_connections = get_snowflake_connections()
if "active_connection_name" not in st.session_state:
    st.session_state.active_connection_name = None
if "selected_tab" not in st.session_state:
    st.session_state.selected_tab = "Home"

# --- CSS Styling ---
st.markdown("""
    <style>
        html, body, [class*="css"] {
            font-family: 'Segoe UI', sans-serif;
        }
        .stApp { background-color: #f4f7fb; }
        .stMetric label, .stMetric div { color: #1f2937; }
        .stSubheader, .stMarkdown h2, .stMarkdown h3 { color: #0f172a; }
        .stButton>button {
            background-color: #1f2937; color: white; border-radius: 8px; padding: 0.4rem 1rem; width: 100%; text-align: left;
        }
        .stButton>button:hover {
            background-color: #374151; color: white;
        }
        .block-container { padding: 2rem; }
    </style>
""", unsafe_allow_html=True)

# --- Sidebar Section ---
with st.sidebar:
    st.markdown("<h2 style='color: #4B5563; font-size: 18px;'>⚙️ LLM Settings</h2>", unsafe_allow_html=True)

    st.selectbox(
        "Provider",
        available_providers,
        index=available_providers.index(st.session_state.get("llm_provider", available_providers[0])),
        key="llm_provider"
    )

    selected_model = llm_creds[st.session_state.llm_provider]["model"]

    st.selectbox(
        "Model",
        [selected_model],
        index=0,
        key="llm_model"
    )

    st.markdown("<hr style='margin-top:20px;margin-bottom:10px;'>", unsafe_allow_html=True)

    st.markdown("<h2 style='color: #4B5563; font-size: 18px;'>🧭 Navigation</h2>", unsafe_allow_html=True)

    nav_options = {
        "Home": "🏠 Home",
        "Connections": "🔗 Connections",
        "Query Optimizer": "🧠 Query Optimizer",
        "API Configuration": "🔧 API Config",
        "Anomaly Detection": "📊 Anomaly Detection",
        "Cost Forecasting": "📈 Cost Forecasting",
        "Stale table detection": "🧹 Stale Table Cleanup"
    }

    for key, label in nav_options.items():
        if st.button(label, key=key):
            st.session_state.selected_tab = key

# --- Page Header ---
selected_tab = st.session_state.selected_tab
st.markdown(f"<h2 style='text-align: center; color: #1F2937;'>{selected_tab}</h2>", unsafe_allow_html=True)

# --- Home Page ---
if selected_tab == "Home":
    active_conn_name = st.session_state.active_connection_name
    all_connections = st.session_state.snowflake_connections
    active_conn = all_connections.get(active_conn_name)

    try:
        if active_conn:
            conn = connect_to_snowflake(active_conn)
            cur = conn.cursor()

            today_str = datetime.utcnow().strftime('%Y-%m-%d')

            cur.execute(f"""SELECT COUNT(*) FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME::DATE = '{today_str}'""")
            total_queries = cur.fetchone()[0]

            cur.execute(f"""SELECT SUM(CREDITS_USED) FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY WHERE START_TIME::DATE = '{today_str}'""")
            total_credits = cur.fetchone()[0] or 0
            estimated_cost = round(total_credits * 1, 2)

            cur.execute("SHOW WAREHOUSES")
            warehouses = cur.fetchall()
            active_wh = sum(1 for w in warehouses if "RUNNING" in str(w[5]))

            cur.execute("""SELECT EXECUTION_STATUS FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())""")
            statuses = [row[0] for row in cur.fetchall()]
            running_q = statuses.count("RUNNING")
            queued_q = statuses.count("QUEUED")

            cur.close()
            conn.close()
        else:
            raise ValueError("No active connection.")
    except Exception as e:
        st.warning(f"Could not load live metrics: {e}")
        total_queries = 0
        estimated_cost = 0
        active_wh = 0
        running_q, queued_q = 0, 0

    st.subheader("System Snapshot")
    col1, col2, col3 = st.columns(3)
    col1.metric("Total Queries (Today)", total_queries)
    col2.metric("Estimated Cost ($)", f"${estimated_cost:,.2f}")
    col3.metric("Active Warehouses", active_wh)

    st.markdown("---")
    st.subheader("Live Query Status")
    col4, col5 = st.columns(2)
    col4.metric("Running Queries", running_q)
    col5.metric("Queued Queries", queued_q)

    st.markdown("---")
    st.subheader("System Alerts")
    st.info("✅ No critical alerts at the moment.")

    st.markdown("---")
    st.subheader("Quick Actions")
    col6, col7, col8 = st.columns(3)
    col6.button("Open Query Optimizer", on_click=lambda: st.session_state.update({"selected_tab": "Query Optimizer"}))
    col7.button("Manage Connections", on_click=lambda: st.session_state.update({"selected_tab": "Connections"}))
    col8.button("API Configuration", on_click=lambda: st.session_state.update({"selected_tab": "API Configuration"}))

# --- Module Tabs ---
elif selected_tab == "Connections":
    from modules.connections import streamlit_page as connections_page
    connections_page.render()

elif selected_tab == "Query Optimizer":
    conn_name = st.session_state.active_connection_name
    conn_dict = st.session_state.snowflake_connections.get(conn_name)
    if conn_dict:
        query_optimizer.render(conn_dict)
    else:
        st.warning("Please set an active Snowflake connection.")

elif selected_tab == "API Configuration":
    api_config.render()

elif selected_tab == "Anomaly Detection":
    conn_name = st.session_state.active_connection_name
    conn_dict = st.session_state.snowflake_connections.get(conn_name)
    if conn_dict:
        anomaly_detection.render(conn_dict)
    else:
        st.error("❌ No active Snowflake connection. Please connect from the 'Connections' tab.")

elif selected_tab == "Cost Forecasting":
    st.info("Cost Forecasting module is under development.")

elif selected_tab == "Stale table detection":
    conn_name = st.session_state.active_connection_name
    conn_dict = st.session_state.snowflake_connections.get(conn_name)
    if conn_dict:
        stale_tables_page.render(conn_dict)
    else:
        st.error("❌ No active Snowflake connection. Please connect from the 'Connections' tab.")

# --- Footer ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; font-size: 13px; color: gray;'>"
    "OptiVerse MVP v0.1 · Built by Himanshu & Spark · © 2025 Krishnity"
    "</div>",
    unsafe_allow_html=True
)
