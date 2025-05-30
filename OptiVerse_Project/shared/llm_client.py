import streamlit as st
from llm.ollama_helpers import call_llm
from modules.api_config.config_manager import get_api_credentials

def generate_sql_optimization(prompt):
    return call_llm(prompt)

def compare_explain_plans(original: str, optimized: str) -> str:
    prompt = f"""
You are a Snowflake SQL optimization expert.

Compare these two EXPLAIN plans and give a layman summary:
1️⃣ Original Plan:
{original}

2️⃣ Optimized Plan:
{optimized}

Clearly explain if performance improved, worsened, or stayed the same.
"""
    creds = get_api_credentials()
    # Use selected provider and model from session
    provider = st.session_state.get("llm_provider", "together")
    model = creds.get(provider, {}).get("model", "meta-llama/llama-4-scout-17b-16e-instruct")

    return call_llm(prompt, model=model, provider=provider)
