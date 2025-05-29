import streamlit as st
import snowflake.connector
from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.backends import default_backend

def get_connection():
    conn_details = st.session_state.get("_active_conn")
    if not conn_details:
        raise ValueError("Snowflake connection not found in session.")

    if conn_details["auth_method"] == "Username/Password":
        return snowflake.connector.connect(
            user=conn_details["user"],
            password=conn_details["password"],
            account=conn_details["account"],
            warehouse=conn_details["warehouse"],
            database=conn_details["database"],
            schema=conn_details["schema"],
            role=conn_details.get("role") or None
        )
    else:
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

        return snowflake.connector.connect(
            user=conn_details["user"],
            private_key=pkb,
            account=conn_details["account"],
            warehouse=conn_details["warehouse"],
            database=conn_details["database"],
            schema=conn_details["schema"],
            role=conn_details.get("role") or None
        )
