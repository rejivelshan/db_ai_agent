from collections import defaultdict

IGNORE_FIELDS = {"_id"}

def get_type(value):
    if isinstance(value, dict):
        return "object"
    elif isinstance(value, list):
        return "array"
    elif value is None:
        return "null"
    else:
        return type(value).__name__


def merge_types(type1, type2):
    if type1 == type2:
        return type1
    return f"{type1}|{type2}"


def merge_schema(schema1, schema2):
    for key, value in schema2.items():
        if key not in schema1:
            schema1[key] = value
        else:
            existing = schema1[key]

            # merge types
            existing["type"] = merge_types(existing["type"], value["type"])

            # merge object schema
            if existing["type"].startswith("object") and "schema" in value:
                if "schema" not in existing:
                    existing["schema"] = {}
                merge_schema(existing["schema"], value["schema"])

            # merge array items
            if existing["type"].startswith("array") and "items" in value:
                if isinstance(existing.get("items"), dict) and isinstance(value["items"], dict):
                    merge_schema(existing["items"], value["items"])

    return schema1


def infer_schema_from_doc(doc):
    schema = {}

    for key, value in doc.items():
        if key in IGNORE_FIELDS:
            continue
        value_type = get_type(value)

        if value_type == "object":
            schema[key] = {
                "type": "object",
                "schema": infer_schema_from_doc(value)
            }

        elif value_type == "array":
            item_schemas = []

            for item in value:
                if isinstance(item, dict):
                    item_schemas.append(infer_schema_from_doc(item))
                else:
                    item_schemas.append({"type": get_type(item)})

            merged_item_schema = {}
            for item_schema in item_schemas:
                merged_item_schema = merge_schema(merged_item_schema, item_schema)

            schema[key] = {
                "type": "array",
                "items": merged_item_schema if merged_item_schema else "unknown"
            }

        else:
            schema[key] = {
                "type": value_type
            }

    return schema


def infer_mongo_schema(collection, sample_size=20):
    docs = list(collection.find().limit(sample_size))

    if not docs:
        return {}

    final_schema = {}

    field_presence = defaultdict(int)

    for doc in docs:
        doc_schema = infer_schema_from_doc(doc)

        for key in doc.keys():
            field_presence[key] += 1

        final_schema = merge_schema(final_schema, doc_schema)

    # 🔥 mark optional fields
    total_docs = len(docs)

    for key in final_schema:
        if field_presence[key] < total_docs:
            final_schema[key]["optional"] = True
        else:
            final_schema[key]["optional"] = False

    return final_schema