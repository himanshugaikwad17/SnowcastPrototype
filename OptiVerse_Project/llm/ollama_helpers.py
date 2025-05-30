import requests
from together import Together
from modules.api_config.config_manager import get_api_credentials

OLLAMA_URL = "http://localhost:11434"
TIMEOUT_MS = 10000

# Unified LLM call
def call_llm(prompt: str, model: str, provider: str = "together") -> str:
    provider = provider.lower()
    creds = get_api_credentials().get(provider, {})
    api_key = creds.get("api_key", "")

    if provider == "ollama":
        if not is_ollama_up():
            return "❌ Ollama is not running. Please start it with: `ollama run model-name`"
        try:
            response = requests.post(
                f"{OLLAMA_URL}/api/generate",
                json={"model": model, "prompt": prompt, "stream": False},
                timeout=TIMEOUT_MS / 1000
            )
            if response.status_code == 200:
                result = response.json().get("response", "").strip()
                return result.replace("```sql", "").replace("```", "").strip() or "⚠️ No output from the model."
            else:
                return f"❌ Ollama error {response.status_code}: {response.text}"
        except requests.exceptions.RequestException as e:
            return f"❌ Ollama request failed: {e}"

    elif provider == "together":
        try:
            client = Together(api_key=api_key)
            response = client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"❌ Together API error: {e}"

    elif provider == "groq":
        try:
            headers = {"Authorization": f"Bearer {api_key}"}
            payload = {
                "model": model,
                "messages": [
                    {"role": "system", "content": "You are a helpful assistant."},
                    {"role": "user", "content": prompt}
                ]
            }
            response = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=payload)
            if response.status_code == 200:
                return response.json()["choices"][0]["message"]["content"].strip()
            else:
                return f"❌ Groq error {response.status_code}: {response.text}"
        except Exception as e:
            return f"❌ Groq request failed: {e}"

    return "⚠️ Unknown provider specified. Use 'together', 'groq', or 'ollama'."

def is_ollama_up(timeout=TIMEOUT_MS) -> bool:
    try:
        response = requests.get(OLLAMA_URL, timeout=timeout / 1000)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False
