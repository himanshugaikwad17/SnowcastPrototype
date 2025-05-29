import os
import requests
from together import Together
from dotenv import load_dotenv

load_dotenv()

TOGETHER_API_KEY = os.getenv("TOGETHER_API_KEY")
GROQ_API_KEY = os.getenv("GROQ_API_KEY")
OLLAMA_URL = "http://localhost:11434"
TIMEOUT_MS = 10000

# Load Together client
together_client = Together(api_key=TOGETHER_API_KEY)

def is_ollama_up(timeout=TIMEOUT_MS) -> bool:
    try:
        response = requests.get(OLLAMA_URL, timeout=timeout / 1000)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def call_llm(prompt: str, model: str = "mistralai/Mistral-7B-Instruct-v0.1", provider: str = "together") -> str:
    """
    Unified LLM calling function.
    Providers: 'ollama', 'together', 'groq'
    """
    provider = provider.lower()

    if provider == "ollama":
        if not is_ollama_up():
            return "❌ Ollama is not running. Please start it with: `ollama run mistral`"
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
            response = together_client.chat.completions.create(
                model=model,
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"❌ Together API error: {e}"

    elif provider == "groq":
        try:
            headers = {"Authorization": f"Bearer {GROQ_API_KEY}"}
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
