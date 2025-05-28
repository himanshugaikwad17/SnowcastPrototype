import streamlit as st
import re
from shared.snowflake_connector import get_connection
from shared.llm_client import call_ollama, compare_explain_plans

# --- HELPER FUNCTIONS ---

def get_explain_plan(query):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN USING TEXT {query}")
        result = cursor.fetchall()
        return "\n".join([row[0] for row in result])
    except Exception as e:
        return f"Error: {e}"
    finally:
        cursor.close()

import re
import streamlit as st
from shared.snowflake_connector import get_connection

def get_table_columns(query: str):
    """
    Attempts to extract the correct table name from the SQL query and then runs DESC TABLE on it.
    """
    conn = get_connection()
    cursor = conn.cursor()
    try:
        # Attempt to extract the full table name from the SQL query
        match = re.search(r"from\s+([a-zA-Z0-9_\.]+)", query, re.IGNORECASE)
        table_name = match.group(1) if match else None

        if not table_name:
            return []

        # If the table name is already fully qualified
        if table_name.count(".") == 2:
            desc_target = table_name
        else:
            # Fallback to session context
            conn_details = st.session_state.get("_active_conn")
            desc_target = f"{conn_details['database']}.{conn_details['schema']}.{table_name}"

        cursor.execute(f"DESC TABLE {desc_target}")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error during DESC TABLE: {e}")
        return []
    finally:
        cursor.close()

def clean_optimized_query(sql: str) -> str:
    """
    Removes invalid ORDER BY clauses that reference non-existent or escaped pseudo-columns like \_row\_key\_\_
    """
    # Strip invalid ORDER BY clauses with \_row\_key\_\_ or similar
    sql = re.sub(r"ORDER BY\s+\\?_row\\?_key\\?_+.*?(FETCH|LIMIT|OFFSET|;)", r"\1", sql, flags=re.IGNORECASE)
    return sql.strip()


def extract_sql_only(text):
    text = re.sub(r'\\([.*])', r'\1', text.strip())
    match = re.search(r"(?is)(select .*?from .*?;)(?=\s|$)", text)
    return match.group(1).strip() if match else text.strip()

def optimize_sql_with_ollama(query, _):
    columns = get_table_columns(query)
    schema_hint = f"Available columns: {', '.join(columns)}\n" if columns else ""
    prompt = f"""
You are a Snowflake SQL performance expert.

Given the following SQL query, optimize it for:
- Better performance
- Lower warehouse credit cost
- Query plan simplification

DO NOT return any explanation, comments, or annotations.

Only respond with the **complete optimized SELECT SQL query**.

{schema_hint}
Original query:
{query.strip()}
"""
    raw = call_ollama(prompt, model="mistral")
    optimized = extract_sql_only(raw)
    return clean_optimized_query(optimized)


# --- MAIN MODULE FUNCTION ---

def render(connection):
    if not connection:
        st.error("❌ Snowflake connection not configured. Please set it from the 'Connection' tab.")
        return

    st.session_state["_active_conn"] = connection  # lightweight reference only

    required_keys = ["account", "user", "warehouse", "database", "schema"]
    if any(k not in connection or not connection[k] for k in required_keys):
        st.error("❌ Missing required connection fields. Please check your active connection.")
        return

    try:
        import snowflake.connector
        if connection["auth_method"] == "Username/Password":
            conn = snowflake.connector.connect(
                user=connection["user"],
                password=connection["password"],
                account=connection["account"],
                warehouse=connection["warehouse"],
                database=connection["database"],
                schema=connection["schema"],
                role=connection.get("role") or None
            )
        else:
            from cryptography.hazmat.primitives import serialization
            from cryptography.hazmat.backends import default_backend

            p_key = serialization.load_pem_private_key(
                connection["private_key_content"].encode(),
                password=connection["private_key_passphrase"].encode() if connection["private_key_passphrase"] else None,
                backend=default_backend()
            )

            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption()
            )

            conn = snowflake.connector.connect(
                user=connection["user"],
                private_key=pkb,
                account=connection["account"],
                warehouse=connection["warehouse"],
                database=connection["database"],
                schema=connection["schema"],
                role=connection.get("role") or None
            )

        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP()")
        result = cur.fetchone()
        st.success(f"🔌 Connected as **{result[0]}** at **{result[1]}**")
        cur.close()
        conn.close()

    except Exception as e:
        st.error(f"❌ Connection failed in Query Optimizer: {e}")

    st.header("🧠 Query Optimizer")
    st.markdown("### Enter your Snowflake SQL query:")

    if "user_query" not in st.session_state:
        st.session_state["user_query"] = ""
    user_query = st.text_area("SQL Query", height=200, value=st.session_state["user_query"])

    if "table_name" not in st.session_state:
        st.session_state["table_name"] = "DEMO_SALES"
    table_name = st.text_input("Target Table Name", value=st.session_state["table_name"])

    if st.button("Clear"):
        for key in [
            "user_query", "table_name",
            "original_plan", "optimized_query",
            "optimized_plan", "comparison_summary"
        ]:
            st.session_state.pop(key, None)
        st.success("Reset complete. Refreshing...")
        st.stop()

    if st.button("Analyze and Optimize"):
        st.session_state["user_query"] = user_query
        st.session_state["table_name"] = table_name

        if user_query.strip().lower().startswith("select"):
            with st.spinner("Running EXPLAIN plan on original query..."):
                original_plan = get_explain_plan(user_query)
                st.session_state["original_plan"] = original_plan

            with st.spinner("Contacting Mistral via Ollama to optimize query..."):
                optimized_query = optimize_sql_with_ollama(user_query, table_name)
                st.session_state["optimized_query"] = optimized_query

            if optimized_query.lower().startswith("select"):
                with st.spinner("Running EXPLAIN plan on optimized query..."):
                    optimized_plan = get_explain_plan(optimized_query)
                    st.session_state["optimized_plan"] = optimized_plan

                with st.spinner("Comparing EXPLAIN plans using Mistral..."):
                    comparison_summary = compare_explain_plans(original_plan, optimized_plan)
                    st.session_state["comparison_summary"] = comparison_summary
            else:
                st.warning("Optimized query is not a valid SELECT statement.")
        else:
            st.error("Only SELECT queries are allowed for optimization and EXPLAIN analysis.")

    if "original_plan" in st.session_state and "optimized_plan" in st.session_state:
        st.markdown("### 🔍 EXPLAIN Plan Comparison")
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("#### 📝 Original Query")
            st.code(st.session_state["user_query"], language="sql")
            st.markdown("#### 🧮 EXPLAIN Plan (Original)")
            st.code(st.session_state["original_plan"], language="sql")

        with col2:
            st.markdown("#### ✨ Optimized Query")
            st.code(st.session_state["optimized_query"], language="sql")
            st.markdown("#### ⚙️ EXPLAIN Plan (Optimized)")
            st.code(st.session_state["optimized_plan"], language="sql")

    if "comparison_summary" in st.session_state:
        st.markdown("### 🤖 LLM-Based Summary")
        st.markdown(st.session_state["comparison_summary"])
