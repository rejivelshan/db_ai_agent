from collections import defaultdict
import datetime

from rapidfuzz import fuzz

IDENTIFIER_TOKENS = {"id", "number", "code", "ref", "key"}
ENTITY_KEY_TOKENS = {"name", "email", "phone", "mobile", "username", "login"}
ATTRIBUTE_TOKENS = {
    "amount",
    "balance",
    "city",
    "country",
    "date",
    "duration",
    "fare",
    "grade",
    "price",
    "rating",
    "state",
    "status",
    "time",
    "type",
}


def is_similar(a, b):
    return fuzz.ratio(str(a), str(b)) > 80


def tokenize_name(name):
    return [token for token in str(name).lower().split("_") if token]


def is_identifier_like(name):
    return any(
        token in IDENTIFIER_TOKENS
        for token in tokenize_name(name)
    )


def is_entity_key_like(name):
    return any(
        token in ENTITY_KEY_TOKENS
        for token in tokenize_name(name)
    )


def comparison_key_score(key, distinct_count=0, candidate_count=0):
    tokens = tokenize_name(key)
    attribute_count = sum(token in ATTRIBUTE_TOKENS for token in tokens)
    specificity = sum(token not in ATTRIBUTE_TOKENS for token in tokens)
    return (
        0 if is_identifier_like(key) else 1 if is_entity_key_like(key) else 2,
        attribute_count,
        -specificity,
        -distinct_count,
        -candidate_count,
        key,
    )


def classify_mismatch(status):
    if status in ["DUPLICATE_IN_MONGO", "VALUE_MISMATCH", "DUPLICATE_IN_SQL"]:
        return {
            "category": "DATA",
            "severity": "MEDIUM",
        }

    if status in ["MISSING_IN_MONGO", "MISSING_IN_SQL"]:
        return {
            "category": "DATA",
            "severity": "HIGH",
        }

    return {
        "category": "UNKNOWN",
        "severity": "LOW",
    }


def normalize_value(val):
    if isinstance(val, float) and val.is_integer():
        return int(val)
    if isinstance(val, datetime.datetime):
        return val.replace(tzinfo=None).isoformat(sep=" ", timespec="seconds")
    if isinstance(val, datetime.date):
        return val.isoformat()
    if isinstance(val, str):
        normalized = val.strip()
        looks_like_datetime = any(token in normalized for token in ("-", ":", "T", " "))
        if not looks_like_datetime:
            return val
        try:
            parsed = datetime.datetime.fromisoformat(normalized.replace("Z", "+00:00"))
            if parsed.tzinfo is not None:
                parsed = parsed.astimezone(datetime.timezone.utc).replace(tzinfo=None)
            return parsed.isoformat(sep=" ", timespec="seconds")
        except ValueError:
            try:
                return datetime.date.fromisoformat(normalized).isoformat()
            except ValueError:
                return val
    return val


def find_identifier_key(records):
    candidate_counts = defaultdict(int)
    candidate_distinct_values = defaultdict(set)

    for record in records:
        if not isinstance(record, dict):
            continue

        for key, value in record.items():
            if value is None or isinstance(value, (dict, list)):
                continue
            candidate_counts[key] += 1
            candidate_distinct_values[key].add(value)

    if not candidate_counts:
        return None

    unique_candidates = [
        key for key in candidate_counts
        if len(candidate_distinct_values[key]) == candidate_counts[key]
    ]

    if "id" in unique_candidates:
        return "id"

    if not unique_candidates:
        return None

    return min(
        unique_candidates,
        key=lambda key: comparison_key_score(
            key,
            distinct_count=len(candidate_distinct_values[key]),
            candidate_count=candidate_counts[key],
        ),
    )


def build_root_path(key_name, key_value):
    if key_name is None:
        return "record"
    return f"{key_name}={key_value}"


def has_identity_key(records, key):
    return any(isinstance(record, dict) and key in record for record in records)


def compare_data(sql_data, mongo_data, root_key=None):
    mismatches = []
    identity_key = root_key or find_identifier_key((sql_data or []) + (mongo_data or []))

    if identity_key and (
        not has_identity_key(sql_data or [], identity_key)
        or not has_identity_key(mongo_data or [], identity_key)
    ):
        fallback_key = find_identifier_key((sql_data or []) + (mongo_data or []))
        if (
            fallback_key
            and has_identity_key(sql_data or [], fallback_key)
            and has_identity_key(mongo_data or [], fallback_key)
        ):
            identity_key = fallback_key
        else:
            compare_list(sql_data or [], mongo_data or [], mismatches, "records")
            return mismatches

    if not sql_data and mongo_data:
        for doc in mongo_data:
            doc_key = identity_key or find_identifier_key([doc])
            info = classify_mismatch("MISSING_IN_SQL")
            mismatches.append({
                "path": build_root_path(doc_key, doc.get(doc_key) if doc_key else None),
                "sql_value": None,
                "mongo_value": doc,
                "error": "Missing in SQL",
                "status": "MISSING_IN_SQL",
                **info,
            })
        return mismatches

    if sql_data and not mongo_data:
        for doc in sql_data:
            doc_key = identity_key or find_identifier_key([doc])
            info = classify_mismatch("MISSING_IN_MONGO")
            mismatches.append({
                "path": build_root_path(doc_key, doc.get(doc_key) if doc_key else None),
                "sql_value": doc,
                "mongo_value": None,
                "error": "Missing in Mongo",
                "status": "MISSING_IN_MONGO",
                **info,
            })
        return mismatches

    if identity_key is None:
        compare_list(sql_data, mongo_data, mismatches, "records")
        return mismatches

    sql_index = defaultdict(list)
    for doc in sql_data:
        if identity_key in doc:
            sql_index[doc[identity_key]].append(doc)

    mongo_index = defaultdict(list)
    for doc in mongo_data:
        if identity_key in doc:
            mongo_index[doc[identity_key]].append(doc)

    if not sql_index and not mongo_index and (sql_data or mongo_data):
        compare_list(sql_data, mongo_data, mismatches, "records")
        return mismatches

    for record_id, sql_docs in sql_index.items():
        sql_doc = sql_docs[0]
        root_path = build_root_path(identity_key, record_id)

        if len(sql_docs) > 1:
            info = classify_mismatch("DUPLICATE_IN_SQL")
            mismatches.append({
                "path": root_path,
                "sql_value": sql_docs,
                "mongo_value": None,
                "error": "Duplicate records in SQL",
                "status": "DUPLICATE_IN_SQL",
                **info,
            })

        mongo_docs = mongo_index.get(record_id)

        if mongo_docs and len(mongo_docs) > 1:
            info = classify_mismatch("DUPLICATE_IN_MONGO")
            mismatches.append({
                "path": root_path,
                "sql_value": None,
                "mongo_value": mongo_docs,
                "error": "Duplicate records in Mongo",
                "status": "DUPLICATE_IN_MONGO",
                **info,
            })

        if not mongo_docs:
            info = classify_mismatch("MISSING_IN_MONGO")
            mismatches.append({
                "path": root_path,
                "sql_value": sql_doc,
                "mongo_value": None,
                "error": "Missing in Mongo",
                "status": "MISSING_IN_MONGO",
                **info,
            })
            continue

        matched = False
        temp_mismatches = []

        for mongo_doc in mongo_docs:
            candidate_mismatches = []
            compare_dict(sql_doc, mongo_doc, candidate_mismatches, path=root_path)
            if not candidate_mismatches:
                matched = True
                break
            temp_mismatches = candidate_mismatches

        if not matched:
            mismatches.extend(temp_mismatches)

    for record_id, mongo_docs in mongo_index.items():
        if record_id not in sql_index:
            info = classify_mismatch("MISSING_IN_SQL")
            for mongo_doc in mongo_docs:
                mismatches.append({
                    "path": build_root_path(identity_key, record_id),
                    "sql_value": None,
                    "mongo_value": mongo_doc,
                    "error": "Extra record in Mongo (missing in SQL)",
                    "status": "MISSING_IN_SQL",
                    **info,
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
                    **info,
                })


def detect_key(l1, l2):
    records = [item for item in l1 + l2 if isinstance(item, dict)]
    if not records:
        return None

    common_keys = set(records[0].keys())
    for record in records[1:]:
        common_keys &= set(record.keys())

    if not common_keys:
        return None

    if "id" in common_keys:
        candidate_values = [item.get("id") for item in records]
        if (
            all(value is not None and not isinstance(value, (dict, list)) for value in candidate_values)
            and len(set(candidate_values)) == len(candidate_values)
        ):
            return "id"

    ranked = sorted(
        common_keys,
        key=lambda key: comparison_key_score(key),
    )
    for key in ranked:
        values1 = [item.get(key) for item in l1 if isinstance(item, dict) and key in item]
        values2 = [item.get(key) for item in l2 if isinstance(item, dict) and key in item]
        combined_values = values1 + values2
        if (
            combined_values
            and all(value is not None and not isinstance(value, (dict, list)) for value in combined_values)
            and len(set(values1)) == len(values1)
            and len(set(values2)) == len(values2)
        ):
            return key

    return None


def compare_list(l1, l2, mismatches, path):
    key = detect_key(l1, l2)

    if key:
        dict1 = {item[key]: item for item in l1 if isinstance(item, dict) and key in item}
        dict2 = {item[key]: item for item in l2 if isinstance(item, dict) and key in item}

        used_mongo = set()

        for k1, item1 in dict1.items():
            found_match = False

            for k2, item2 in dict2.items():
                if k2 in used_mongo:
                    continue

                if k1 == k2:
                    compare_dict(item1, item2, mismatches, f"{path}[{key}={k1}]")
                    used_mongo.add(k2)
                    found_match = True
                    break

                if is_similar(k1, k2):
                    compare_dict(item1, item2, mismatches, f"{path}[{key}~={k1}]")
                    used_mongo.add(k2)
                    found_match = True
                    break

            if not found_match:
                info = classify_mismatch("MISSING_IN_MONGO")
                mismatches.append({
                    "path": f"{path}[{key}={k1}]",
                    "sql_value": item1,
                    "mongo_value": None,
                    "error": "Missing in Mongo",
                    "status": "MISSING_IN_MONGO",
                    **info,
                })

        for k2, item2 in dict2.items():
            if k2 not in used_mongo:
                info = classify_mismatch("MISSING_IN_SQL")
                mismatches.append({
                    "path": f"{path}[{key}={k2}]",
                    "sql_value": None,
                    "mongo_value": item2,
                    "error": "Missing in SQL",
                    "status": "MISSING_IN_SQL",
                    **info,
                })
        return

    if all(isinstance(item, dict) for item in l1 + l2):
        used_mongo = set()

        for index, item1 in enumerate(l1):
            best_match_index = None
            best_candidate = None
            best_score = None

            for mongo_index, item2 in enumerate(l2):
                if mongo_index in used_mongo:
                    continue

                candidate = []
                compare_dict(item1, item2, candidate, f"{path}[{index}]")
                score = len(candidate)

                if best_score is None or score < best_score:
                    best_score = score
                    best_match_index = mongo_index
                    best_candidate = candidate

                if score == 0:
                    break

            if best_match_index is None:
                info = classify_mismatch("MISSING_IN_MONGO")
                mismatches.append({
                    "path": f"{path}[{index}]",
                    "sql_value": item1,
                    "mongo_value": None,
                    "error": "Missing in Mongo",
                    "status": "MISSING_IN_MONGO",
                    **info,
                })
                continue

            used_mongo.add(best_match_index)
            mismatches.extend(best_candidate)

        for mongo_index, item2 in enumerate(l2):
            if mongo_index in used_mongo:
                continue
            info = classify_mismatch("MISSING_IN_SQL")
            mismatches.append({
                "path": f"{path}[{mongo_index}]",
                "sql_value": None,
                "mongo_value": item2,
                "error": "Missing in SQL",
                "status": "MISSING_IN_SQL",
                **info,
            })
        return

    max_len = max(len(l1), len(l2))

    for i in range(max_len):
        new_path = f"{path}[{i}]"

        if i >= len(l1):
            mismatches.append({"path": new_path, "error": "Missing in SQL"})
            continue

        if i >= len(l2):
            mismatches.append({"path": new_path, "error": "Missing in Mongo"})
            continue

        v1 = l1[i]
        v2 = l2[i]

        if isinstance(v1, dict) and isinstance(v2, dict):
            compare_dict(v1, v2, mismatches, new_path)
        elif normalize_value(v1) != normalize_value(v2):
            mismatches.append({
                "path": new_path,
                "sql_value": v1,
                "mongo_value": v2,
            })
