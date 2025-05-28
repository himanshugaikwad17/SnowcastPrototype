import os
import requests
from together import Together
from dotenv import load_dotenv
load_dotenv()

print("TOGETHER_API_KEY:", os.getenv("TOGETHER_API_KEY"))


OLLAMA_URL = "http://localhost:11434"
TIMEOUT_MS = 10000

# Load Together API key once
together_client = Together(api_key=os.getenv("TOGETHER_API_KEY"))

def is_ollama_up(timeout=TIMEOUT_MS) -> bool:
    try:
        response = requests.get(OLLAMA_URL, timeout=timeout / 1000)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def call_llm(prompt: str, model: str = "deepseek-ai/DeepSeek-R1", provider: str = "together") -> str:
    """
    Unified interface to call LLMs via Ollama (local) or Together AI (cloud).
    Set `provider` to either 'ollama' or 'together'.
    """
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
                return f"❌ Error {response.status_code}: {response.text}"
        except requests.exceptions.RequestException as e:
            return f"❌ Failed to connect to Ollama: {e}"

    elif provider == "together":
        try:
            together_model = model if "/" in model else f"{model}"
            response = together_client.chat.completions.create(
                model=together_model,
                messages=[{"role": "user", "content": prompt}],
                stream=False
            )
            return response.choices[0].message.content.strip()
        except Exception as e:
            return f"❌ Failed to call Together AI:\n{e}"

    else:
        return f"⚠️ Unknown provider: {provider}"
