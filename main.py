import argparse
import json
import os
import sys

from connectors.mongo_connector import MongoConnector
from connectors.postgres_connector import PostgresConnector
from core.ai_agent import explain_mismatch
from core.auto_schema_mapper import auto_map_fields
from core.chatbot import ask_agent
from core.comparator import compare_data
from core.mongo_schema_infer import infer_mongo_schema
from core.normalizer import normalize_sql_data
from core.reporter import export_to_csv
from core.schema_extractor import extract_postgres_schema
from core.schema_mapper import (
    build_mapping_tree,
    build_relationship_graph,
    find_root_table,
    generate_join_query,
)
from core.schema_mapper_runtime import apply_schema_mapping, set_schema
from dotenv import load_dotenv

load_dotenv(override=True)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--pg-host", default=os.getenv("POSTGRES_HOST"))
    parser.add_argument("--pg-db", default=os.getenv("POSTGRES_DB"))
    parser.add_argument("--pg-user", default=os.getenv("POSTGRES_USER"))
    parser.add_argument("--pg-password", default=os.getenv("POSTGRES_PASSWORD"))
    parser.add_argument("--pg-port", default=os.getenv("POSTGRES_PORT"))
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017"))
    parser.add_argument("--mongo-db", default=os.getenv("MONGO_DB", "testdb"))
    parser.add_argument("--mongo-collection", default=os.getenv("MONGO_COLLECTION", "lottery"))
    return parser.parse_args()


def align_structure(data):
    if isinstance(data, dict):
        return {key: align_structure(value) for key, value in data.items()}
    if isinstance(data, list):
        return [align_structure(item) for item in data]
    return data


def harmonize_to_schema(data, schema_node):
    if schema_node == {}:
        return data

    if isinstance(schema_node, dict) and "type" in schema_node:
        schema_type = schema_node["type"]

        if schema_type == "array":
            item_schema = schema_node.get("items", {})
            if isinstance(data, list):
                return [harmonize_to_schema(item, item_schema) for item in data]
            if isinstance(data, dict):
                return [harmonize_to_schema(data, item_schema)]
            return data

        if schema_type == "object":
            schema_node = schema_node.get("schema", {})
            if isinstance(data, list):
                if not data:
                    return {}
                return harmonize_to_schema(data[0], schema_node)

    if isinstance(data, list):
        return [harmonize_to_schema(item, schema_node) for item in data]

    if isinstance(data, dict) and isinstance(schema_node, dict):
        harmonized = {}
        matched_keys = 0
        for key, value in data.items():
            if key not in schema_node:
                continue
            matched_keys += 1
            harmonized[key] = harmonize_to_schema(value, schema_node[key])
        if data and schema_node and matched_keys == 0:
            return data
        return harmonized

    return data


args = parse_args()

pg = PostgresConnector(
    host=args.pg_host,
    database=args.pg_db,
    user=args.pg_user,
    password=args.pg_password,
    port=args.pg_port,
)
pg.connect()
schema = extract_postgres_schema(pg.conn)
set_schema(schema)

mongo = MongoConnector(
    mongo_uri=args.mongo_uri,
    mongo_db=args.mongo_db,
)
mongo.connect()

collection_name = args.mongo_collection
mongo_collection = mongo.db[collection_name]
mongo_schema = infer_mongo_schema(mongo_collection)
mongo_data = mongo.fetch_data(collection_name)

print("\nExtracted Schema:\n")
for table, details in schema.items():
    print(f"\nTable: {table}")
    print("Columns:", details["columns"])
    print("PK:", details["primary_key"])
    print("FK:", details["foreign_keys"])
print()

graph = build_relationship_graph(schema)
root = find_root_table(graph, schema, mongo_schema)
mapping_tree = build_mapping_tree(graph, root, schema, mongo_schema)

print("\nMapping Tree:")
print(mapping_tree)

auto_query = generate_join_query(mapping_tree, schema)
print("\nAuto Generated SQL:\n", auto_query)

sql_data = pg.fetch_data(auto_query)
normalized_sql = normalize_sql_data(sql_data, mapping_tree)

print("\nNormalized SQL:")
print(normalized_sql)
print()

print("\nMongo Schema:\n")
print(json.dumps(mongo_schema, indent=4))
print()

print("\nMongo Data:")
print(mongo_data)
print(f"Mongo collection used: {collection_name}")

normalized_sql = align_structure(normalized_sql)
mongo_data = align_structure(mongo_data)

print("\nAfter Structure Alignment:")
print(normalized_sql)

field_map = auto_map_fields(normalized_sql, mongo_data)

print("\nAuto Field Mapping Detected:")
print(field_map)

normalized_sql = apply_schema_mapping(
    normalized_sql,
    field_map,
    ignore_fields=[],
)
normalized_sql = harmonize_to_schema(normalized_sql, mongo_schema)

print("\nNormalized SQL AFTER Mapping:")
print(normalized_sql)

root_key = schema.get(root, {}).get("primary_key", [None])[0]
mismatches = compare_data(normalized_sql, mongo_data, root_key=root_key)

print("\nComparison Result:")

if not mismatches:
    print("PERFECT MATCH - Data is identical")
else:
    print("Mismatches found:")
    for mismatch in mismatches:
        print(mismatch)

print("\nSummary:")
print(f"Total mismatches: {len(mismatches)}")

if mismatches:
    affected_entities = set()
    affected_ids = set()

    for mismatch in mismatches:
        path = mismatch.get("path", "")

        if "=" in path:
            root_part = path.split(".")[0]
            affected_entities.add(root_part)
            affected_ids.add(root_part.split("=")[-1])

    print(f"Affected entities: {list(affected_entities)}")
    print(f"Affected IDs: {list(affected_ids)}")

export_to_csv(mismatches, "reports/report.csv")

print("\nAI Explanations:\n")
if mismatches:
    for mismatch in mismatches[:10]:
        print(explain_mismatch(mismatch))
        print("-" * 50)
else:
    print("No mismatches to explain")

if sys.stdin.isatty():
    print("\nChat with your data (type 'exit' to quit)\n")

    while True:
        try:
            question = input("You: ")
        except EOFError:
            break

        if question.lower() == "exit":
            break

        answer = ask_agent(question, mismatches)
        print("\nAssistant:", answer)
        print("-" * 60)
