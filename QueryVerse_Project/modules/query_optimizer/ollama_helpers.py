import requests

def call_ollama(prompt: str, model="mistral") -> str:
    response = requests.post(
        "http://localhost:11434/api/generate",
        json={"model": model, "prompt": prompt, "stream": False}
    )
    if response.status_code == 200:
        return response.json().get("response", "No response returned.").strip()
    return f"Error {response.status_code}: {response.text}"
