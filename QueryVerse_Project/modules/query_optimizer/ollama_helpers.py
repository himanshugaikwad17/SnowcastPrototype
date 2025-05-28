import requests

def is_ollama_up(timeout=10000) -> bool:
    """Check if Ollama server is running."""
    try:
        response = requests.get("http://localhost:11434", timeout=timeout)
        return response.status_code == 200
    except requests.exceptions.RequestException:
        return False

def call_ollama(prompt: str, model="mistral") -> str:
    """Call Ollama's local API using the specified model (default: mistral)."""
    if not is_ollama_up():
        return "❌ Ollama is not running. Please start it with: `ollama run mistral`"

    try:
        response = requests.post(
            "http://localhost:11434/api/generate",
            json={
                "model": model,
                "prompt": prompt,
                "stream": False
            },
            timeout=10000
        )

        if response.status_code == 200:
            result = response.json().get("response", "").strip()
            # Optional: remove markdown code blocks or prefix/suffix junk
            result = result.replace("```sql", "").replace("```", "").strip()
            return result or "⚠️ No output from the model."
        else:
            return f"❌ Error {response.status_code}: {response.text}"

    except requests.exceptions.RequestException as e:
        return f"❌ Failed to connect to Ollama: {e}"
