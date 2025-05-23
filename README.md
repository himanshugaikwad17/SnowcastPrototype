# QueryVerse - Query Optimizer Module

This module provides an AI-powered SQL query optimizer for Snowflake.
It uses LLMs (via Ollama + Mistral) to suggest improvements and
compares EXPLAIN plans to show potential gains in performance and cost.

## Features
- EXPLAIN plan comparison for original and optimized SQL
- Snowflake-compatible optimization suggestions
- Schema-aware LLM prompting

## How to Run
```
cd modules/query-optimizer
streamlit run streamlit_app.py
```

## Requirements
- Python 3.8+
- Snowflake Connector
- Streamlit
- Ollama (local LLM runner)
