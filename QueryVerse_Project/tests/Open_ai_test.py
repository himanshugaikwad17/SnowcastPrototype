from together import Together
import os

# Optional: set API key in code
os.environ["TOGETHER_API_KEY"] = "24af77987abf2e4583ab93949c1c88b77f697d5fe444ba47d32e6251a160a404"

client = Together()

response = client.chat.completions.create(
    model="mistralai/Mistral-7B-Instruct-v0.1",
    messages=[{"role": "user", "content": "Explain quantum physics simply."}],
    stream=True
)

for token in response:
    if hasattr(token, 'choices'):
        print(token.choices[0].delta.content, end='', flush=True)
