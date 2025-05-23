import streamlit as st
import re
from shared.snowflake_connector import get_connection
from shared.llm_client import generate_sql_optimization, compare_explain_plans

# --- Function to get EXPLAIN plan ---
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

# --- Get table column names ---
def get_table_columns(table_name: str):
    conn = get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESC TABLE {table_name}")
        return [row[0] for row in cursor.fetchall()]
    except Exception as e:
        return []
    finally:
        cursor.close()

# --- Function to clean LLM response ---
def extract_sql_only(text):
    text = re.sub(r'\\([.*])', r'\1', text.strip())
    match = re.search(r"(?is)(select .*?from .*?;)(?=\s|$)", text)
    return match.group(1).strip() if match else text.strip()

# --- Function to optimize query ---
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

# --- Streamlit App ---
st.title("PolarMonster: Snowflake Query Optimizer")

st.markdown("### Enter your Snowflake SQL query:")
user_query = st.text_area("SQL Query", height=200)
table_name = st.text_input("Target Table Name", value="DEMO_SALES")

if st.button("Analyze and Optimize"):
    if user_query.strip().lower().startswith("select"):
        with st.spinner("Running EXPLAIN plan on original query..."):
            original_plan = get_explain_plan(user_query)
            st.markdown("### EXPLAIN Plan (Original Query)")
            st.code(original_plan, language="sql")

        with st.spinner("Contacting Mistral via Ollama to optimize query..."):
            optimized_query = optimize_sql_with_ollama(user_query, table_name)
            st.markdown("### Optimized SQL Query")
            st.code(optimized_query, language="sql")

        if optimized_query.lower().startswith("select"):
            with st.spinner("Running EXPLAIN plan on optimized query..."):
                optimized_plan = get_explain_plan(optimized_query)
                st.markdown("### EXPLAIN Plan (Optimized Query)")
                st.code(optimized_plan, language="sql")

            with st.spinner("Comparing EXPLAIN plans using Mistral..."):
                comparison_summary = compare_explain_plans(original_plan, optimized_plan)
                st.markdown("### LLM-Based EXPLAIN Plan Comparison Summary")
                st.markdown(comparison_summary)
        else:
            st.warning("Optimized query is not a valid SELECT statement.")
    else:
        st.error("Only SELECT queries are allowed for optimization and EXPLAIN analysis.")
