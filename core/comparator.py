from collections import defaultdict
from rapidfuzz import fuzz

def is_similar(a, b):
    return fuzz.ratio(str(a), str(b)) > 80

def classify_mismatch(status):
    if status == "DUPLICATE_IN_MONGO":
        return {
            "category": "DATA",
            "severity": "MEDIUM"
        }
    if status == "VALUE_MISMATCH":
        return {
            "category": "DATA",
            "severity": "MEDIUM"
        }

    if status in ["MISSING_IN_MONGO", "MISSING_IN_SQL"]:
        return {
            "category": "DATA",
            "severity": "HIGH"
        }

    return {
        "category": "UNKNOWN",
        "severity": "LOW"
    }


def normalize_value(val):
    # handle numeric comparison (500 vs 500.0)
    if isinstance(val, float) and val.is_integer():
        return int(val)
    return val


def compare_data(sql_data, mongo_data):
    mismatches = []

    # 🔥 Case 1: SQL empty, Mongo has data
    if not sql_data and mongo_data:
        for doc in mongo_data:
            info = classify_mismatch("MISSING_IN_SQL")

            mismatches.append({
                "path": f"user_id={doc.get('user_id')}",
                "sql_value": None,
                "mongo_value": doc,
                "error": "Missing in SQL",
                "status": "MISSING_IN_SQL",
                **info
            })

        return mismatches

    # 🔥 Case 2: Mongo empty, SQL has data
    if sql_data and not mongo_data:
        for doc in sql_data:
            info = classify_mismatch("MISSING_IN_MONGO")

            mismatches.append({
                "path": f"user_id={doc.get('user_id')}",
                "sql_value": doc,
                "mongo_value": None,
                "error": "Missing in Mongo",
                "status": "MISSING_IN_MONGO",
                **info
            })

        return mismatches

    from collections import defaultdict

    # 🔥 build SQL index
    sql_index = defaultdict(list)
    for doc in sql_data:
        sql_index[doc["user_id"]].append(doc)

    # 🔥 build Mongo index
    mongo_index = defaultdict(list)
    for doc in mongo_data:
        mongo_index[doc["user_id"]].append(doc)

    # 🔥 main loop
    for user_id, sql_docs in sql_index.items():
        sql_doc = sql_docs[0]  # use first for comparison

        # 🔥 SQL duplicate detection
        if len(sql_docs) > 1:
            info = classify_mismatch("DUPLICATE_IN_SQL")

            mismatches.append({
                "path": f"user_id={user_id}",
                "sql_value": sql_docs,
                "mongo_value": None,
                "error": "Duplicate records in SQL",
                "status": "DUPLICATE_IN_SQL",
                **info
            })

        mongo_docs = mongo_index.get(user_id)

        # 🔥 Mongo duplicate detection
        if mongo_docs and len(mongo_docs) > 1:
            info = classify_mismatch("DUPLICATE_IN_MONGO")

            mismatches.append({
                "path": f"user_id={user_id}",
                "sql_value": None,
                "mongo_value": mongo_docs,
                "error": "Duplicate records in Mongo",
                "status": "DUPLICATE_IN_MONGO",
                **info
            })

        # ❌ Missing in Mongo
        if not mongo_docs:
            info = classify_mismatch("MISSING_IN_MONGO")

            mismatches.append({
                "path": f"user_id={user_id}",
                "sql_value": sql_doc,
                "mongo_value": None,
                "error": "Missing in Mongo",
                "status": "MISSING_IN_MONGO",
                **info
            })
            continue

        # 🔥 compare against all Mongo duplicates
        matched = False

        for mongo_doc in mongo_docs:
            temp_mismatches = []
            compare_dict(sql_doc, mongo_doc, temp_mismatches, path=f"user_id={user_id}")

            if not temp_mismatches:
                matched = True
                break

        # ❌ if none matched perfectly → record mismatches
        if not matched:
            mismatches.extend(temp_mismatches)

    # 🔥 detect extra records in Mongo (not in SQL)
    for user_id, mongo_docs in mongo_index.items():
        if user_id not in sql_index:
            info = classify_mismatch("MISSING_IN_SQL")

            for mongo_doc in mongo_docs:
                mismatches.append({
                    "path": f"user_id={user_id}",
                    "sql_value": None,
                    "mongo_value": mongo_doc,
                    "error": "Extra record in Mongo (missing in SQL)",
                    "status": "MISSING_IN_SQL",
                    **info
                })
    return mismatches


def compare_dict(d1, d2, mismatches, path=""):
    keys = set(d1.keys()).union(set(d2.keys()))


    for key in keys:
        new_path = f"{path}.{key}" if path else key

        v1 = d1.get(key)
        v2 = d2.get(key)

        if isinstance(v1, dict) and isinstance(v2, dict):
            compare_dict(v1, v2, mismatches, new_path)

        elif isinstance(v1, list) and isinstance(v2, list):
            compare_list(v1, v2, mismatches, new_path)

        else:
            v1 = normalize_value(v1)
            v2 = normalize_value(v2)

            if v1 != v2:
                info = classify_mismatch("VALUE_MISMATCH")

                mismatches.append({
                    "path": new_path,
                    "sql_value": v1,
                    "mongo_value": v2,
                    "error": None,
                    "status": "VALUE_MISMATCH",
                    **info
                })

def detect_key(l1, l2):
    possible_keys = ["id", "user_id", "order_id", "item_id", "product_name"]

    for key in possible_keys:
        if all(isinstance(item, dict) and key in item for item in l1 + l2):
            return key

    return None

def compare_list(l1, l2, mismatches, path):
    # try to detect key automatically
    key = detect_key(l1, l2)

    if key:
        dict1 = {item[key]: item for item in l1 if key in item}
        dict2 = {item[key]: item for item in l2 if key in item}

        used_sql = set()
        used_mongo = set()

        for k1 in dict1:
            found_match = False

            for k2 in dict2:
                if k2 in used_mongo:
                    continue

                # ✅ exact match
                if k1 == k2:
                    compare_dict(dict1[k1], dict2[k2], mismatches, f"{path}[{key}={k1}]")
                    used_sql.add(k1)
                    used_mongo.add(k2)
                    found_match = True
                    break

                # 🔥 fuzzy match (REAL FIX)
                if is_similar(k1, k2):
                    compare_dict(dict1[k1], dict2[k2], mismatches, f"{path}[{key}≈{k1}]")
                    used_sql.add(k1)
                    used_mongo.add(k2)
                    found_match = True
                    break

            # ❌ only if truly not matched
            if not found_match:
                info = classify_mismatch("MISSING_IN_MONGO")

                mismatches.append({
                    "path": f"{path}[{key}={k1}]",
                    "sql_value": dict1[k1],
                    "mongo_value": None,
                    "error": "Missing in Mongo",
                    "status": "MISSING_IN_MONGO",
                    **info
                })

        # 🔥 remaining mongo items (FIXED)
        for k2 in dict2:
            if k2 not in used_mongo:
                info = classify_mismatch("MISSING_IN_SQL")

                mismatches.append({
                    "path": f"{path}[{key}={k2}]",
                    "sql_value": None,
                    "mongo_value": dict2[k2],
                    "error": "Missing in SQL",
                    "status": "MISSING_IN_SQL",
                    **info
                })

    else:
        # fallback to index comparison
        max_len = max(len(l1), len(l2))

        for i in range(max_len):
            new_path = f"{path}[{i}]"

            if i >= len(l1):
                mismatches.append({
                    "path": new_path,
                    "error": "Missing in SQL"
                })
                continue

            if i >= len(l2):
                mismatches.append({
                    "path": new_path,
                    "error": "Missing in Mongo"
                })
                continue

            v1 = l1[i]
            v2 = l2[i]

            if isinstance(v1, dict) and isinstance(v2, dict):
                compare_dict(v1, v2, mismatches, new_path)
            else:
                if normalize_value(v1) != normalize_value(v2):
                    mismatches.append({
                        "path": new_path,
                        "sql_value": v1,
                        "mongo_value": v2
                    })