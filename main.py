from connectors.postgres_connector import PostgresConnector
from connectors.mongo_connector import MongoConnector
from core.normalizer import normalize_sql_data
from core.comparator import compare_data
from core.reporter import export_to_csv
from core.ai_agent import explain_mismatch
from core.chatbot import ask_agent
from core.schema_extractor import extract_postgres_schema
import json
from core.mongo_schema_infer import infer_mongo_schema
from core.schema_mapper import (
    build_relationship_graph,
    find_root_table,
    build_mapping_tree,
    generate_join_query
)
# PostgreSQL
pg = PostgresConnector()
pg.connect()
schema = extract_postgres_schema(pg.conn)

print("\n📊 Extracted Schema:\n")
for table, details in schema.items():
    print(f"\n🔹 Table: {table}")
    print("Columns:", details["columns"])
    print("PK:", details["primary_key"])
    print("FK:", details["foreign_keys"])
print("\n")



graph = build_relationship_graph(schema)
root = find_root_table(graph)
mapping_tree = build_mapping_tree(graph, root)

print("\n🧠 Mapping Tree:")
print(mapping_tree)

auto_query = generate_join_query(mapping_tree, schema)

print("\n⚡ Auto Generated SQL:\n", auto_query)

sql_data = pg.fetch_data(auto_query)

# Normalize SQL
normalized_sql = normalize_sql_data(sql_data, mapping_tree)

print("\n✅ Normalized SQL:")
print(normalized_sql)
print("\n")
# Mongo
mongo = MongoConnector()
mongo.connect()

collection_name = "orders_embedded"

mongo_collection = mongo.db[collection_name]

mongo_schema = infer_mongo_schema(mongo_collection)

print("\n📦 Mongo Schema (Advanced):\n")
print(json.dumps(mongo_schema, indent=4))
print("\n")
mongo_data = mongo.fetch_data("orders_embedded")

print("\n✅ Mongo Data:")
print(mongo_data)

mismatches = compare_data(normalized_sql, mongo_data)

print("\n🔍 Comparison Result:")

if not mismatches:
    print("✅ PERFECT MATCH — Data is identical")
else:
    print("❌ Mismatches found:")
    for m in mismatches:
        print(m)

print("\n📊 Summary:")

total = len(mismatches)
print(f"Total mismatches: {total}")

if total > 0:
    affected_users = set()
    for m in mismatches:
        if "user_id=" in m["path"]:
            uid = m["path"].split("=")[1].split(".")[0]
            affected_users.add(uid)

    print(f"Affected users: {list(affected_users)}")

export_to_csv(mismatches, "reports/report.csv")

print("\n"*2)

print("\n🤖 AI Explanations:\n")

if mismatches:
    for m in mismatches[:10]:
        print(explain_mismatch(m))
        print("-" * 50)
else:
    print("No mismatches to explain")

print("\n💬 Chat with your data (type 'exit' to quit)\n")

while True:
    question = input("You: ")

    if question.lower() == "exit":
        break

    answer = ask_agent(question, mismatches)

    print("\n🤖:", answer)
    print("-" * 60)