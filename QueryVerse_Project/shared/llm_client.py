from modules.query-optimizer.ollama_helpers import call_ollama

def generate_sql_optimization(prompt):
    return call_ollama(prompt)

def compare_explain_plans(original_plan, optimized_plan):
    prompt = f"Compare these Snowflake EXPLAIN plans:\nOriginal:\n{original_plan}\n\nOptimized:\n{optimized_plan}"
    return call_ollama(prompt)
