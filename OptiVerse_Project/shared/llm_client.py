from llm.ollama_helpers import call_llm

def generate_sql_optimization(prompt):
    return call_llm(prompt)

def compare_explain_plans(original_plan, optimized_plan):
    prompt = f"Compare these Snowflake EXPLAIN plans:\nOriginal:\n{original_plan}\n\nOptimized:\n{optimized_plan}"
    return call_llm(prompt)
