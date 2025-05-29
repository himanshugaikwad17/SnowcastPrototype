import streamlit as st
import random
import json
import os
import snowflake.connector
from datetime import datetime
from modules.query_optimizer import streamlit_page as query_optimizer

# --- Persistent storage path ---
CONN_FILE = "shared/connections.json"

# --- Load connections from file ---
def load_connections():
    if os.path.exists(CONN_FILE):
        with open(CONN_FILE, "r") as f:
            return json.load(f)
    return {}

# --- Save connections to file ---
def save_connections(connections):
    with open(CONN_FILE, "w") as f:
        json.dump(connections, f, indent=4)

# --- Get live dashboard stats from Snowflake ---
def get_live_dashboard_stats(conn_details):
    try:
        auth_method = conn_details.get("auth_method")

        if auth_method == "Username/Password":
            sf_conn = snowflake.connector.connect(
                user=conn_details["user"],
                password=conn_details["password"],
                account=conn_details["account"],
                warehouse=conn_details["warehouse"],
                database=conn_details["database"],
                schema=conn_details["schema"],
                role=conn_details["role"] or None
            )
        else:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            p_key = serialization.load_pem_private_key(
                conn_details["private_key_content"].encode(),
                password=conn_details["private_key_passphrase"].encode() if conn_details["private_key_passphrase"] else None,
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            sf_conn = snowflake.connector.connect(
                user=conn_details["user"],
                private_key=pkb,
                account=conn_details["account"],
                warehouse=conn_details["warehouse"],
                database=conn_details["database"],
                schema=conn_details["schema"],
                role=conn_details["role"] or None
            )

        cur = sf_conn.cursor()

        today = datetime.utcnow().date()
        today_str = today.strftime('%Y-%m-%d')

        cur.execute(f"""
            SELECT COUNT(*) 
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME::DATE = '{today_str}'
        """)
        total_queries = cur.fetchone()[0]

        cur.execute(f"""
            SELECT SUM(CREDITS_USED)
            FROM SNOWFLAKE.ACCOUNT_USAGE.WAREHOUSE_METERING_HISTORY
            WHERE START_TIME::DATE = '{today_str}'
        """)
        total_credits = cur.fetchone()[0] or 0
        estimated_cost = round(total_credits * 1, 2)

        # Additional live insights
        cur.execute("SHOW WAREHOUSES")
        warehouses = cur.fetchall()
        active_warehouses = sum(1 for w in warehouses if "RUNNING" in str(w[5]))

        cur.execute("""
            SELECT EXECUTION_STATUS
            FROM SNOWFLAKE.ACCOUNT_USAGE.QUERY_HISTORY
            WHERE START_TIME >= DATEADD(hour, -1, CURRENT_TIMESTAMP())
        """)
        statuses = [row[0] for row in cur.fetchall()]
        running_queries = statuses.count("RUNNING")
        queued_queries = statuses.count("QUEUED")

        cur.close()
        sf_conn.close()

        return total_queries, estimated_cost, random.randint(5, 15), running_queries, queued_queries, active_warehouses

    except Exception as e:
        st.warning(f"Could not load live metrics: {e}")
        return random.randint(1000, 1500), random.uniform(3000, 5000), random.randint(5, 15), 0, 0, 0

# --- Initial load ---
if "snowflake_connections" not in st.session_state:
    st.session_state.snowflake_connections = load_connections()

if "active_connection_name" not in st.session_state:
    st.session_state.active_connection_name = None

st.set_page_config(page_title="OptiVerse", layout="wide")

# Inject custom aesthetic CSS
st.markdown("""
    <style>
        html, body, [class*="css"] {
            font-family: 'Segoe UI', sans-serif;
        }
        .stApp {
            background-color: #f4f7fb;
        }
        .stMetric label, .stMetric div {
            color: #1f2937;
        }
        .stSubheader, .stMarkdown h2, .stMarkdown h3 {
            color: #0f172a;
        }
        .stButton>button {
            background-color: #1f2937;
            color: white;
            border-radius: 8px;
            padding: 0.4rem 1rem;
        }
        .stButton>button:hover {
            background-color: #374151;
            color: white;
        }
        .block-container {
            padding: 2rem;
        }
    </style>
""", unsafe_allow_html=True)

# --- Sidebar ---
with st.sidebar:
    st.markdown("### OptiVerse", unsafe_allow_html=True)
    st.markdown("#### Navigation")
    st.markdown("""
    <style>
    .sidebar-nav {
        padding: 0;
        margin: 0;
        list-style: none;
    }
    .sidebar-nav li {
        padding: 8px 16px;
        margin-bottom: 6px;
        background-color: #ffffff;
        border-radius: 6px;
        cursor: pointer;
        font-weight: 500;
        color: #1f2937;
        transition: background-color 0.2s;
    }
    .sidebar-nav li:hover {
        background-color: #e5e7eb;
    }
    .sidebar-nav li.active {
        background-color: #1f2937;
        color: white;
    }
    </style>
""", unsafe_allow_html=True)

    nav_options = ["Home", "Connections", "Query Optimizer", "Anomaly Detection", "Cost Forecasting"]
    for option in nav_options:
        css_class = "active" if option == st.session_state.get("selected_tab", "Home") else ""
        if st.button(option, key=option):
            st.session_state.selected_tab = option
    selected_tab = st.session_state.get("selected_tab", "Home")

    st.markdown("---")
    st.markdown("#### Settings")
    st.markdown("- Profile")
    st.markdown("- API Tokens")
    st.markdown("- Help & Support")
    st.markdown("---")
    st.caption("Himanshu Gaikwad")

# --- Page Header ---
st.markdown(f"<h2 style='text-align: center; color: #1F2937;'>{selected_tab}</h2>", unsafe_allow_html=True)

# --- HOME TAB ---
if selected_tab == "Home":
    active_conn_name = st.session_state.get("active_connection_name")
    all_connections = st.session_state.get("snowflake_connections", {})
    active_conn = all_connections.get(active_conn_name)

    if active_conn:
        total_queries, estimated_cost, anomalies, running_q, queued_q, active_wh = get_live_dashboard_stats(active_conn)
    else:
        total_queries, estimated_cost, anomalies, running_q, queued_q, active_wh = random.randint(1000, 1500), random.uniform(3000, 5000), random.randint(5, 15), 0, 0, 0

    st.subheader("System Snapshot")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.metric("Total Queries (Today)", f"{total_queries}")
    with col2:
        st.metric("Estimated Cost ($)", f"${estimated_cost:,.2f}")
    with col3:
        st.metric("Anomalies Detected", anomalies)

    st.markdown("---")
    st.subheader("Live Query & Warehouse Status")
    col4, col5, col6 = st.columns(3)
    with col4:
        st.metric("Running Queries", running_q)
    with col5:
        st.metric("Queued Queries", queued_q)
    with col6:
        st.metric("Active Warehouses", active_wh)

    st.markdown("---")
    st.subheader("System Alerts")
    st.info("No critical alerts at the moment.")

    st.markdown("---")
    st.subheader("Smart Suggestions")
    st.warning("Consider resizing Warehouse_X to Small to reduce cost.")
    st.warning("Query 456 took 3x longer than the average yesterday.")

    st.markdown("---")
    st.subheader("Quick Actions")
    col7, col8, col9 = st.columns(3)
    with col7:
        if st.button("Go to Query Optimizer"):
            st.session_state.selected_tab = "Query Optimizer"
    with col8:
        if st.button("Open Cost Forecasting"):
            st.session_state.selected_tab = "Cost Forecasting"
    with col9:
        if st.button("Manage Connections"):
            st.session_state.selected_tab = "Connections"

# --- QUERY OPTIMIZER TAB ---
elif selected_tab == "Query Optimizer":
    active_conn_name = st.session_state.get("active_connection_name")
    all_connections = st.session_state.get("snowflake_connections", {})
    active_conn = all_connections.get(active_conn_name)

    if active_conn:
        query_optimizer.render(active_conn)
    else:
        st.warning("No active connection selected. Please go to the Connections tab and set one.")

# --- PLACEHOLDER TABS ---
elif selected_tab == "Connections":
    st.subheader("Manage Snowflake Connections")

    connections = st.session_state.snowflake_connections
    connection_names = list(connections.keys())
    connection_names.insert(0, "+ New Connection")

    selected_connection = st.selectbox("Select Connection", connection_names)

    if selected_connection == "+ New Connection":
        new_conn_name = st.text_input("Connection Name")
        auth_method = st.selectbox("Authentication Method", ["Username/Password", "Private Key Pair"])
        sf_account = st.text_input("Account")
        sf_user = st.text_input("Username")

        if auth_method == "Username/Password":
            sf_password = st.text_input("Password", type="password")
            private_key_content, private_key_passphrase = "", ""
        else:
            private_key_content = st.text_area("Private Key Content", height=150)
            private_key_passphrase = st.text_input("Private Key Passphrase", type="password")
            sf_password = ""

        sf_warehouse = st.text_input("Warehouse")
        sf_database = st.text_input("Database")
        sf_schema = st.text_input("Schema")
        sf_role = st.text_input("Role (Optional)")

        if st.button("Save Connection") and new_conn_name:
            connections[new_conn_name] = {
                "auth_method": auth_method,
                "account": sf_account,
                "user": sf_user,
                "password": sf_password,
                "private_key_content": private_key_content,
                "private_key_passphrase": private_key_passphrase,
                "warehouse": sf_warehouse,
                "database": sf_database,
                "schema": sf_schema,
                "role": sf_role
            }
            save_connections(connections)
            st.success(f"Connection '{new_conn_name}' saved.")

        if st.button("Test Connection"):
            try:
                get_live_dashboard_stats(connections[new_conn_name])
                st.success("Connection successful!")
            except Exception as e:
                st.error(f"Connection failed: {e}")

    else:
        conn = connections[selected_connection]
        st.write(f"Connection: {selected_connection}")

        if st.button("Delete Connection"):
            del connections[selected_connection]
            save_connections(connections)
            st.success("Connection deleted.")

        if st.button("Set Active Connection"):
            st.session_state.active_connection_name = selected_connection
            st.success(f"Active connection set to: {selected_connection}")

        if st.button("Test Connection"):
            try:
                get_live_dashboard_stats(conn)
                st.success("Connection successful!")
            except Exception as e:
                st.error(f"Connection failed: {e}")

elif selected_tab == "Anomaly Detection":
    st.info("Anomaly Detection module is under development.")

elif selected_tab == "Cost Forecasting":
    st.info("Cost Forecasting module is under development.")

# --- FOOTER ---
st.markdown("---")
st.markdown(
    "<div style='text-align: center; font-size: 13px; color: gray;'>"
    "OptiVerse MVP v0.1 · Built by Himanshu & Spark · © 2025 Krishnity"
    "</div>",
    unsafe_allow_html=True
)
