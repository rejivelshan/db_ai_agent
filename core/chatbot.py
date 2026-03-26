import requests
import json

OLLAMA_URL = "http://localhost:11434/api/generate"


def format_mismatches(mismatches):
    """
    Convert mismatches into readable structured text
    """
    if not mismatches:
        return "No mismatches detected."

    formatted = []

    for i, m in enumerate(mismatches, 1):
        formatted.append(f"""
Mismatch {i}:
- Path: {m.get('path')}
- Status: {m.get('status')}
- SQL Value: {m.get('sql_value')}
- Mongo Value: {m.get('mongo_value')}
- Severity: {m.get('severity')}
- Category: {m.get('category')}
""")

    return "\n".join(formatted)


def build_prompt(question, mismatches):
    if not mismatches:
        return f"""
You are REN, an AI database assistant.

Context:
- PostgreSQL and MongoDB data have been compared
- No mismatches were found
- Data is fully consistent

Your role:
- Still act as a database expert
- Answer questions about data, structure, or validation
- Do NOT say "I don’t see anything"
- Always stay in database context

User Question:
{question}

Answer in a helpful and technical way:
"""

    # 🔥 mismatch mode
    context = format_mismatches(mismatches)

    return f"""
You are an expert data engineer and database validation assistant.

Your job:
- Analyze mismatches between SQL and MongoDB
- Answer user questions clearly
- Be precise and technical

DATA MISMATCHES:
{context}

USER QUESTION:
{question}

INSTRUCTIONS:
- Answer ONLY using the mismatch data above
- If answer not found → say "Not enough data"
- Be concise
- If asked for fix → give exact correction

ANSWER:
"""


def ask_agent(question, mismatches):
    prompt = build_prompt(question, mismatches)

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

        if "response" in data:
            return data["response"].strip()
        else:
            return "[Error] No response from model"

    except Exception as e:
        return f"[Error] {str(e)}"