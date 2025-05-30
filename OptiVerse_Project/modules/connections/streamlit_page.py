import streamlit as st
from modules.api_config.config_manager import (
    get_snowflake_connections,
    update_snowflake_connection,
    save_all_config
)
from shared.snowflake_connector import connect_to_snowflake

def render():
    st.subheader("🔗 Manage Snowflake Connections")

    # Show current active connection (if set)
    active = st.session_state.get("active_connection_name")
    if active:
        st.info(f"🔌 Active Connection: **{active}**")

    # Load connections
    connections = get_snowflake_connections()
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

        new_conn = {
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

        if st.button("Test Connection"):
            try:
                test_conn = connect_to_snowflake(new_conn)
                test_conn.cursor().execute("SELECT 1")
                test_conn.close()
                st.success("✅ Connection test successful!")
            except Exception as e:
                st.error(f"❌ Test failed: {e}")

        if st.button("Save Connection") and new_conn_name:
            update_snowflake_connection(new_conn_name, new_conn)
            st.session_state.snowflake_connections = get_snowflake_connections()
            st.session_state.active_connection_name = new_conn_name
            st.success(f"✅ Connection '{new_conn_name}' saved and set as active.")
            st.rerun()

    else:
        # Editing an existing connection
        conn = connections[selected_connection]
        st.write(f"Connection: **{selected_connection}**")
        st.markdown("### ✏️ Edit Connection Details")

        auth_method = st.selectbox("Authentication Method", ["Username/Password", "Private Key Pair"],
                                   index=0 if conn["auth_method"] == "Username/Password" else 1)
        sf_account = st.text_input("Account", value=conn["account"])
        sf_user = st.text_input("Username", value=conn["user"])

        if auth_method == "Username/Password":
            sf_password = st.text_input("Password", value=conn.get("password", ""), type="password")
            private_key_content, private_key_passphrase = "", ""
        else:
            private_key_content = st.text_area("Private Key Content", value=conn.get("private_key_content", ""), height=150)
            private_key_passphrase = st.text_input("Private Key Passphrase", value=conn.get("private_key_passphrase", ""), type="password")
            sf_password = ""

        sf_warehouse = st.text_input("Warehouse", value=conn["warehouse"])
        sf_database = st.text_input("Database", value=conn["database"])
        sf_schema = st.text_input("Schema", value=conn["schema"])
        sf_role = st.text_input("Role (Optional)", value=conn.get("role", ""))

        updated_conn = {
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

        if st.button("Test Connection"):
            try:
                test_conn = connect_to_snowflake(updated_conn)
                test_conn.cursor().execute("SELECT 1")
                test_conn.close()
                st.success("✅ Connection test successful!")
            except Exception as e:
                st.error(f"❌ Test failed: {e}")

        if st.button("Save Changes"):
            update_snowflake_connection(selected_connection, updated_conn)
            st.session_state.snowflake_connections = get_snowflake_connections()
            st.success("✅ Connection updated.")
            st.rerun()

        if st.button("Set Active Connection"):
            st.session_state.active_connection_name = selected_connection
            st.success(f"✅ Active connection set to: {selected_connection}")
            st.rerun()

        if st.button("Delete Connection"):
            all_config = get_snowflake_connections()
            del all_config[selected_connection]
            save_all_config({"snowflake": all_config})
            st.session_state.snowflake_connections = get_snowflake_connections()
            st.success("❌ Connection deleted.")
            st.rerun()
