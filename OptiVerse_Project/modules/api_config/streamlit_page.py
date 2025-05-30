import streamlit as st
from modules.api_config.config_manager import get_api_credentials, update_api_credentials

def render():
    st.subheader("🔐 API Configuration")

    credentials = get_api_credentials()
    provider_display_to_key = {
        "Groq": "groq"
    }

    provider_display = st.selectbox("Select Model Provider", list(provider_display_to_key.keys()))
    provider_key = provider_display_to_key[provider_display]

    api_key = st.text_input(f"{provider_display} API Key", value=credentials[provider_key]["api_key"], type="password")
    model = st.text_input(f"{provider_display} Model Name", value=credentials[provider_key]["model"])

    if st.button("Save API Configuration"):
        update_api_credentials(provider_key, api_key, model)
        st.success(f"{provider_display} API credentials updated.")
