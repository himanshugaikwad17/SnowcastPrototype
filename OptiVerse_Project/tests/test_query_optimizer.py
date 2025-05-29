import streamlit as st
import snowflake.connector
import requests
import re

# --- CONFIGURATION (Update with your Snowflake credentials) ---
SNOWFLAKE_ACCOUNT = "TPBJBVN-YZ63291"  # e.g., xy12345.us-east-1
SNOWFLAKE_USER = "himanshugaikwadsnowflake052025"
SNOWFLAKE_PASSWORD = "Two1704*states"
SNOWFLAKE_WAREHOUSE = "COMOUTE_WH"
SNOWFLAKE_DATABASE = "QUERYVERSE_DEMO"
SNOWFLAKE_SCHEMA = "STAGING"

# --- Connect to Snowflake ---
@st.cache_resource
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=SNOWFLAKE_USER,
        password=SNOWFLAKE_PASSWORD,
        account=SNOWFLAKE_ACCOUNT,
        warehouse=SNOWFLAKE_WAREHOUSE,
        database=SNOWFLAKE_DATABASE,
        schema=SNOWFLAKE_SCHEMA
    )
    return conn

# --- Function to get EXPLAIN plan ---
def get_explain_plan(query):
    conn = get_snowflake_connection()
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
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(f"DESC TABLE {table_name}")
        columns = [row[0] for row in cursor.fetchall()]
        return columns
    except Exception as e:
        return []
    finally:
        cursor.close()

# --- Function to clean LLM response ---
def extract_sql_only(text):
    # Remove backslashes before *, . or other special characters
    text = re.sub(r'\\([.*])', r'\1', text.strip())
    match = re.search(r"(?is)(select .*?from .*?;)(?=\s|$)", text)
    return match.group(1).strip() if match else text.strip()


# --- Function to call Ollama locally (Mistral model) ---
def optimize_sql_with_ollama(prompt_sql: str, table_name: str) -> str:
    columns = get_table_columns(table_name)
    schema_hint = f"Available columns in table {table_name}: {', '.join(columns)}.\n" if columns else ""
    prompt = (
        f"{schema_hint}Optimize this Snowflake SQL query for cost and performance. "
        "Return only the optimized SQL query, no explanation, and end it with a semicolon.\n"
        f"{prompt_sql}"
    )
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "mistral",
                "prompt": prompt,
                "stream": False
            }
        )
        if response.status_code == 200:
            raw_response = response.json().get("response", "No response generated. ").strip()
            return extract_sql_only(raw_response)
        else:
            return f"Error {response.status_code}: {response.text}"
    except Exception as e:
        return f"Exception while calling Ollama: {e}"

# --- Function to compare two EXPLAIN plans using Ollama ---
def compare_explain_plans(original_plan: str, optimized_plan: str) -> str:
    prompt = (
        "Compare the following two Snowflake EXPLAIN plans. "
        "Summarize key improvements or regressions in cost, pruning, joins, scans, or performance.\n\n"
        f"Original Plan:\n{original_plan}\n\nOptimized Plan:\n{optimized_plan}\n"
    )
    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": "mistral",
                "prompt": prompt,
                "stream": False
            }
        )
        if response.status_code == 200:
            return response.json().get("response", "No response generated.").strip()
        else:
            return f"Error {response.status_code}: {response.text}"
    except Exception as e:
        return f"Exception while calling Ollama for comparison: {e}"

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