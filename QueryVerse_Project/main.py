import streamlit as st
import random
import json
import os
import snowflake.connector
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

# --- Initial load ---
if "snowflake_connections" not in st.session_state:
    st.session_state.snowflake_connections = load_connections()

if "active_connection_name" not in st.session_state:
    st.session_state.active_connection_name = None

# --- Page Config ---
st.set_page_config(page_title="QueryVerse", layout="wide")

# --- Sidebar ---
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
    st.subheader("🧩 Manage Snowflake Connections")

    connections = st.session_state.snowflake_connections
    connection_names = list(connections.keys())
    connection_names.insert(0, "+ New Connection")

    selected_connection = st.selectbox("Select Connection", connection_names)

    if selected_connection == "+ New Connection":
        new_conn_name = st.text_input("🔐 Connection Name")
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
            st.success(f"✅ Connection '{new_conn_name}' saved.")

        if st.button("🔗 Test Connection"):
            try:
                if auth_method == "Username/Password":
                    sf_conn = snowflake.connector.connect(
                        user=sf_user,
                        password=sf_password,
                        account=sf_account,
                        warehouse=sf_warehouse,
                        database=sf_database,
                        schema=sf_schema,
                        role=sf_role or None
                    )
                else:
                    from cryptography.hazmat.primitives import serialization
                    from cryptography.hazmat.backends import default_backend

                    p_key = serialization.load_pem_private_key(
                        private_key_content.encode(),
                        password=private_key_passphrase.encode() if private_key_passphrase else None,
                        backend=default_backend()
                    )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )

                    sf_conn = snowflake.connector.connect(
                        user=sf_user,
                        private_key=pkb,
                        account=sf_account,
                        warehouse=sf_warehouse,
                        database=sf_database,
                        schema=sf_schema,
                        role=sf_role or None
                    )

                sf_conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP();")
                st.success("✅ Connection successful!")
                sf_conn.close()
            except Exception as e:
                st.error(f"❌ Connection failed: {e}")

    else:
        conn = connections[selected_connection]
        st.write(f"🔐 Connection: {selected_connection}")

        if st.button("Delete Connection"):
            del connections[selected_connection]
            save_connections(connections)
            st.success("🗑️ Connection deleted.")

        if st.button("Set Active Connection"):
            st.session_state.active_connection_name = selected_connection
            st.success(f"🌐 Active connection set to: {selected_connection}")

        if st.button("🔗 Test Connection"):
            try:
                if conn["auth_method"] == "Username/Password":
                    sf_conn = snowflake.connector.connect(
                        user=conn["user"],
                        password=conn["password"],
                        account=conn["account"],
                        warehouse=conn["warehouse"],
                        database=conn["database"],
                        schema=conn["schema"],
                        role=conn["role"] or None
                    )
                else:
                    from cryptography.hazmat.primitives import serialization
                    from cryptography.hazmat.backends import default_backend

                    p_key = serialization.load_pem_private_key(
                        conn["private_key_content"].encode(),
                        password=conn["private_key_passphrase"].encode() if conn["private_key_passphrase"] else None,
                        backend=default_backend()
                    )

                    pkb = p_key.private_bytes(
                        encoding=serialization.Encoding.DER,
                        format=serialization.PrivateFormat.PKCS8,
                        encryption_algorithm=serialization.NoEncryption()
                    )

                    sf_conn = snowflake.connector.connect(
                        user=conn["user"],
                        private_key=pkb,
                        account=conn["account"],
                        warehouse=conn["warehouse"],
                        database=conn["database"],
                        schema=conn["schema"],
                        role=conn["role"] or None
                    )

                sf_conn.cursor().execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP();")
                st.success("✅ Connection successful!")
                sf_conn.close()
            except Exception as e:
                st.error(f"❌ Connection failed: {e}")

# --- QUERY OPTIMIZER TAB ---
elif selected_tab == "🧠 Query Optimizer":
    active_conn_name = st.session_state.get("active_connection_name")
    all_connections = st.session_state.get("snowflake_connections", {})
    active_conn = all_connections.get(active_conn_name)

    if active_conn:
        query_optimizer.render(active_conn)
    else:
        st.warning("⚠️ No active connection selected. Please go to the Connection tab and set one.")

# --- ANOMALY DETECTION (COMING SOON) ---
elif selected_tab == "🔍 Anomaly Detection":
    st.info("🚧 Anomaly Detection module is under development.")

# --- COST FORECASTING (COMING SOON) ---
elif selected_tab == "📉 Cost Forecasting":
    st.info("📊 Cost Forecasting module is under development.")

# --- FOOTER ---
st.markdown("---")
st.caption("QueryVerse MVP v0.1 • Spark & Himanshu | Powered by Krishnity")
