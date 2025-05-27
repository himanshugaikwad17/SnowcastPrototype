import streamlit as st
import re
from shared.snowflake_connector import get_connection
from shared.llm_client import generate_sql_optimization, compare_explain_plans

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

def get_table_columns(table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESC TABLE {table_name}")
        return [row[0] for row in cursor.fetchall()]
    except Exception:
        return []
    finally:
        cursor.close()

def extract_sql_only(text):
    text = re.sub(r'\\([.*])', r'\1', text.strip())
    match = re.search(r"(?is)(select .*?from .*?;)(?=\s|$)", text)
    return match.group(1).strip() if match else text.strip()

def optimize_sql_with_ollama(query, table):
    columns = get_table_columns(table)
    schema_hint = f"Available columns in table {table}: {', '.join(columns)}.\n" if columns else ""
    prompt = (
        f"{schema_hint}Optimize this Snowflake SQL query for cost and performance. "
        "Return only the optimized SQL query, no explanation, and end it with a semicolon.\n"
        f"{query}"
    )
    raw = generate_sql_optimization(prompt)
    return extract_sql_only(raw)

# --- MAIN MODULE FUNCTION ---

def render():
    st.header("🧠 Query Optimizer")
    st.markdown("### Enter your Snowflake SQL query:")

    # --- Persistent Input Fields ---
    user_query = st.text_area(
        "SQL Query",
        height=200,
        value=st.session_state.get("user_query", "")
    )
    st.session_state["user_query"] = user_query

    table_name = st.text_input(
        "Target Table Name",
        value=st.session_state.get("table_name", "DEMO_SALES")
    )
    st.session_state["table_name"] = table_name

    # --- Clear Button ---
    if st.button("Clear"):
        for key in [
            "user_query", "table_name",
            "original_plan", "optimized_query",
            "optimized_plan", "comparison_summary"
        ]:
            st.session_state.pop(key, None)
        st.success("Reset complete. Refreshing...")
        st.stop()


    # --- Analyze and Optimize ---
    if st.button("Analyze and Optimize"):
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

    # --- Side-by-Side View ---
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

    # --- LLM Summary ---
    if "comparison_summary" in st.session_state:
        st.markdown("### 🤖 LLM-Based Summary")
        st.markdown(st.session_state["comparison_summary"])

