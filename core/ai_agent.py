import requests

OLLAMA_URL = "http://localhost:11434/api/generate"


def explain_mismatch(mismatch):
    prompt = f"""
    You are a data engineering assistant.

    Explain this mismatch clearly.

    Path: {mismatch.get('path')}
    SQL Value: {mismatch.get('sql_value')}
    Mongo Value: {mismatch.get('mongo_value')}
    Status: {mismatch.get('status')}

    Provide:
    - Issue
    - Cause
    - Fix
    """

    try:
        response = requests.post(
            OLLAMA_URL,
            json={
                "model": "llama3",
                "prompt": prompt,
                "stream": False
            }
        )

        data = response.json()
        return data.get("response", "[No response]")

    except Exception as e:
        return f"[Fallback] Error: {str(e)}"