import streamlit as st
import re
import html
from shared.snowflake_connector import connect_to_snowflake
from llm.ollama_helpers import call_llm
from shared.llm_client import compare_explain_plans
from modules.api_config.config_manager import get_api_credentials

# --- Helper Functions ---

def get_explain_plan(query):
    conn = connect_to_snowflake(st.session_state.get("_active_conn"))
    cursor = conn.cursor()
    try:
        cursor.execute(f"EXPLAIN USING TEXT {query}")
        result = cursor.fetchall()
        return "\n".join([row[0] for row in result])
    except Exception as e:
        return f"Error: {e}"
    finally:
        cursor.close()
        conn.close()

def get_table_columns(query: str):
    conn = connect_to_snowflake(st.session_state.get("_active_conn"))
    cursor = conn.cursor()
    try:
        match = re.search(r"from\s+([a-zA-Z0-9_\.]+)", query, re.IGNORECASE)
        table_name = match.group(1) if match else None
        if not table_name:
            return []
        conn_details = st.session_state.get("_active_conn")
        desc_target = table_name if table_name.count(".") == 2 else f"{conn_details['database']}.{conn_details['schema']}.{table_name}"
        cursor.execute(f"DESC TABLE {desc_target}")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        print(f"Error during DESC TABLE: {e}")
        return []
    finally:
        cursor.close()
        conn.close()

def clean_optimized_query(sql: str) -> str:
    sql = sql.strip()
    if "TOP" in sql.upper() or "LIMIT" in sql.upper():
        sql = re.sub(r"\bTOP\s+\d+\b", "", sql, flags=re.IGNORECASE)
        sql = re.sub(r"\bLIMIT\s+\d+\b", "", sql, flags=re.IGNORECASE)
    return sql.replace(";;", ";").strip()

def extract_sql_only(text: str) -> str:
    text = re.sub(r"```(?:sql)?", "", text, flags=re.IGNORECASE).replace("```", "").strip()
    match = re.search(r"(?is)\b(select|with)\b[\s\S]+", text)
    return match.group(0).strip() if match else text

def build_optimization_prompt(query: str, schema_hint: str = "") -> str:
    return f"""
You are an expert Snowflake SQL performance engineer.

Your task is to optimize the following SQL query for performance using only **valid and executable Snowflake SQL syntax**. Ensure correctness, improve execution speed, and follow all Snowflake best practices.

Output:
- Must return only a complete, executable SQL query (including any WITH/CTE clauses if needed)
- Must not include explanations, comments, markdown, or surrounding text

Rules:
1. ❌ DO NOT use `TOP N` — Snowflake does not support this syntax. Never include `TOP` in any part of the query.
2. ❌ DO NOT use `LIMIT N` at the end or inside subqueries — instead use `ROW_NUMBER()`, `QUALIFY`, or `FILTERS` to reduce result size.
3. ✅ Prefer using `QUALIFY ROW_NUMBER() OVER (...) <= N` to limit rows efficiently.
4. ✅ Ensure deterministic ordering by including a unique key (e.g., `UNIQUE_SURROGATE_KEY`) in `ORDER BY` or `ROW_NUMBER()` functions.

{schema_hint}
Original SQL Query:
{query.strip()}
""".strip()

def optimize_sql_with_ollama(query: str, _) -> str:
    schema_hint = ""
    if not query.strip().lower().startswith("with"):
        columns = get_table_columns(query)
        schema_hint = f"Available columns: {', '.join(columns)}\n" if columns else ""

    provider = st.session_state.get("llm_provider", "together")
    model = st.session_state.get("llm_model", "meta-llama/llama-4-scout-17b-16e-instruct")
    prompt = build_optimization_prompt(query, schema_hint)

    raw = call_llm(prompt, model=model, provider=provider)
    st.session_state["raw_llm_output"] = raw
    print("\n🔍 Raw LLM Response:\n", raw, "\n")

    return clean_optimized_query(extract_sql_only(raw))

# --- UI Helper for Wide SQL Blocks ---

def render_sql_block(title: str, sql_text: str):
    st.markdown(f"#### {title}")
    dark_mode = st.get_option("theme.base") == "dark"
    bg = "#1e1e1e" if dark_mode else "#f9f9f9"
    fg = "#f1f1f1" if dark_mode else "#1a1a1a"
    border = "#444" if dark_mode else "#ccc"

    escaped_sql = html.escape(sql_text, quote=False)  # preserve single quotes

    st.markdown(f"""
<div style="
    background-color: {bg};
    color: {fg};
    border: 1px solid {border};
    border-radius: 6px;
    padding: 12px;
    margin-bottom: 16px;
    overflow-x: auto;
    max-height: 400px;
    max-width: 100%;
    width: 100%;
    margin-left: auto;
    margin-right: auto;
    white-space: pre;
    font-family: monospace;
    font-size: 14px;
">
{escaped_sql}
</div>
""", unsafe_allow_html=True)

# --- Main App UI ---

def render(connection):
    if not connection:
        st.error("❌ Snowflake connection not configured. Please set it from the 'Connection' tab.")
        return

    st.session_state["_active_conn"] = connection

    required_keys = ["account", "user", "warehouse", "database", "schema"]
    if any(k not in connection or not connection[k] for k in required_keys):
        st.error("❌ Missing required connection fields.")
        return

    try:
        conn = connect_to_snowflake(connection)
        cur = conn.cursor()
        cur.execute("SELECT CURRENT_USER(), CURRENT_TIMESTAMP()")
        result = cur.fetchone()
        st.success(f"🔌 Connected as **{result[0]}** at **{result[1]}**")
        cur.close()
        conn.close()
    except Exception as e:
        st.error(f"❌ Connection failed: {e}")

    st.header("🧠 Query Optimizer")

    user_query = st.text_area("SQL Query", height=200, value=st.session_state.get("user_query", ""))
    table_name = st.text_input("Target Table Name", value=st.session_state.get("table_name", "DEMO_SALES"))

    if st.button("Clear"):
        for key in ["user_query", "table_name", "original_plan", "optimized_query", "optimized_plan", "comparison_summary", "raw_llm_output"]:
            st.session_state.pop(key, None)
        st.success("Reset complete.")
        st.stop()

    if st.button("Analyze and Optimize"):
        st.session_state["user_query"] = user_query
        st.session_state["table_name"] = table_name

        if re.match(r"^(select|with)\s", user_query.strip().lower()):
            original_plan = get_explain_plan(user_query)
            st.session_state["original_plan"] = original_plan

            optimized_query = optimize_sql_with_ollama(user_query, table_name)
            st.session_state["optimized_query"] = optimized_query

            if optimized_query.lower().startswith(("select", "with")):
                optimized_plan = get_explain_plan(optimized_query)
                st.session_state["optimized_plan"] = optimized_plan

                comparison_summary = compare_explain_plans(original_plan, optimized_plan)
                st.session_state["comparison_summary"] = comparison_summary
            else:
                st.warning("Optimized output is not a valid SELECT/WITH query.")
        else:
            st.error("Only SELECT or WITH queries are supported.")

    if "original_plan" in st.session_state and "optimized_plan" in st.session_state:
        col1, col2 = st.columns(2)

        with col1:
            render_sql_block("Original Query", st.session_state["user_query"])
            render_sql_block("EXPLAIN Plan (Original)", st.session_state["original_plan"])

        with col2:
            render_sql_block("Optimized Query", st.session_state["optimized_query"])
            render_sql_block("EXPLAIN Plan (Optimized)", st.session_state["optimized_plan"])

    if "comparison_summary" in st.session_state:
        st.markdown("### 🤖 LLM-Based Summary")
        st.markdown(st.session_state["comparison_summary"])

    if "raw_llm_output" in st.session_state and st.checkbox("Show Raw LLM Output"):
        st.text_area("Raw LLM Output", value=st.session_state["raw_llm_output"], height=300, key="llm_raw")
