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

## 📦 Setup

```bash
pip install -r requirements.txt
