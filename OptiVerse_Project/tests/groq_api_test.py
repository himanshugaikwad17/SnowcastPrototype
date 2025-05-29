import requests

headers = {
    "Authorization": "Bearer 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"
}

data = {
    "model": "llama3-70b-8192",  # or llama3-8b-8192
    "messages": [
        {"role": "system", "content": "You are a helpful assistant."},
        {"role": "user", "content": "Explain how Snowflake caching works."}
    ]
}

res = requests.post("https://api.groq.com/openai/v1/chat/completions", headers=headers, json=data)
print(res.json()['choices'][0]['message']['content'])
