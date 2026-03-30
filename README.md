# 🤖 DB AI Agent

AI-powered agent for cross-database comparison, validation, and migration between SQL (DB2/Postgres) and MongoDB.

## 🚀 Features
- Cross-database data comparison
- Schema normalization (SQL → NoSQL)
- Field-level mismatch detection
- Future: Agentic AI with RAG

## 🧰 Tech Stack
- Python
- MongoDB
- IBM DB2
- PostgreSQL
- LangChain (planned)
- OpenAI API (planned)

## 🚀 Phase 2 Completed

### Features Implemented
- Cross-database comparison (PostgreSQL vs MongoDB)
- Schema normalization (relational → nested)
- Deep recursive comparison
- Fuzzy matching for string keys
- Duplicate detection (SQL & MongoDB)
- Missing and extra record detection
- Severity & category classification
- CSV report generation

### Output Example
- VALUE_MISMATCH
- MISSING_IN_MONGO
- MISSING_IN_SQL
- DUPLICATE_IN_SQL
- DUPLICATE_IN_MONGO

# 🤖 DB AI Agent — Phase 3

## 🚀 Overview

DB AI Agent is an intelligent system that compares PostgreSQL and MongoDB data, detects inconsistencies, and explains them using a local AI model.

---

## ✅ Features (Phase 3)

### 🔍 Data Comparison Engine

* Nested data normalization
* Deep comparison (SQL vs MongoDB)
* Missing and duplicate detection
* Fuzzy matching for real-world data errors
* Severity and category classification

---

### 🤖 AI Explanation Engine

* Local AI using Ollama + LLaMA
* Explains mismatches clearly
* Works offline (no API required)

---

### 💬 Chatbot Interface

* Ask questions about your data
* Context-aware responses
* Dual mode:

  * No mismatches → assistant mode
  * With mismatches → debugging mode

---

## 🧠 Example Queries

* Why is record_key=2 mismatched?
* Summarize all issues
* Which records have errors?
* How to fix mismatches?

---
# Phase 4 — Automated Schema & Data Comparison Engine

## 📌 Overview

Phase 4 introduces a fully automated system to:
- Understand relational and NoSQL database structures
- Infer relationships without manual input
- Generate SQL queries dynamically
- Transform flat relational data into nested JSON
- Compare SQL and MongoDB data intelligently

---
## ⚙️ Tech Stack

* Python
* PostgreSQL
* MongoDB
* Ollama (LLaMA 3)

---

## 🚀 How to Run

1. Start Ollama:

```
ollama serve
```

2. Run project:

```
python main.py
```

---

## 🔮 Upcoming Phases

### Phase 4 — Schema Intelligence

* Automatic schema detection
* Structural validation
* Partial data detection

### Phase 5 — Auto Resolution

* AI-based fixes
* Auto sync between databases

---

## 💡 Vision

Build an autonomous AI agent that understands, validates, and corrects database systems.

## 📦 Setup

```bash
pip install -r requirements.txt
