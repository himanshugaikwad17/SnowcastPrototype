import os
import json

CONFIG_FILE = "shared/connections.json"

def load_all_config():
    if os.path.exists(CONFIG_FILE):
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    return {}

def save_all_config(data):
    with open(CONFIG_FILE, "w") as f:
        json.dump(data, f, indent=4)

def get_snowflake_connections():
    return load_all_config().get("snowflake", {})

def update_snowflake_connection(name, conn_details):
    config = load_all_config()
    config.setdefault("snowflake", {})[name] = conn_details
    save_all_config(config)

def get_api_credentials():
    config = load_all_config()
    return {
        "groq": config.get("groq", {"api_key": "", "model": "llama-4-8b"})
    }

def update_api_credentials(provider_key, api_key, model):
    config = load_all_config()
    config[provider_key] = {"api_key": api_key, "model": model}
    save_all_config(config)
