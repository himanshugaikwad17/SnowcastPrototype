import streamlit as st
import snowflake.connector

def get_connection():
    config = st.session_state.get("snowflake_config")
    if not config:
        st.error("❌ Snowflake connection not configured. Please set it from the 'Connection' tab.")
        st.stop()

    try:
        conn = snowflake.connector.connect(
            user=config["user"],
            password=config["password"],
            account=config["account"],
            warehouse=config["warehouse"],
            database=config["database"],
            schema=config["schema"],
            role=config["role"] if config.get("role") else None
        )
        return conn
    except Exception as e:
        st.error(f"❌ Failed to connect to Snowflake: {e}")
        st.stop()
