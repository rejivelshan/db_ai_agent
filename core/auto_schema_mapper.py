import difflib


def singularize_token(token):
    if token.endswith("ies") and len(token) > 3:
        return token[:-3] + "y"
    if token.endswith("sses") and len(token) > 4:
        return token[:-2]
    if token.endswith("ss"):
        return token
    if token.endswith("ses") and len(token) > 3:
        return token[:-2]
    if token.endswith("s") and len(token) > 3:
        return token[:-1]
    return token


def normalize_name(name):
    name = name.lower()
    tokens = name.split("_")
    normalized = [singularize_token(token) for token in tokens]
    return " ".join(normalized)


def tokenize(name):
    return set(name.lower().split("_"))


def is_identifier_like(name):
    tokens = name.lower().split("_")
    return any(token in {"id", "number", "code", "ref", "key"} for token in tokens)


def similarity(a, b):
    return difflib.SequenceMatcher(None, a, b).ratio()


def get_type(value):
    if isinstance(value, list):
        return "array"
    if isinstance(value, dict):
        return "object"
    return "primitive"


def extract_field_samples(data, parent="", samples=None):
    if samples is None:
        samples = {}

    if isinstance(data, list):
        for item in data:
            extract_field_samples(item, parent, samples)
    elif isinstance(data, dict):
        for key, value in data.items():
            full_key = f"{parent}.{key}" if parent else key
            samples.setdefault(full_key, []).append(value)
            extract_field_samples(value, full_key, samples)

    return samples


def value_similarity(v1_list, v2_list):
    v1_list = v1_list[:5]
    v2_list = v2_list[:5]

    score = 0
    for value1 in v1_list:
        for value2 in v2_list:
            if str(value1) == str(value2):
                score += 1

    return score / (len(v1_list) * len(v2_list) + 1e-5)


def structure_similarity(v1_list, v2_list):
    keys1, keys2 = set(), set()

    for value in v1_list[:3]:
        if isinstance(value, dict):
            keys1.update(value.keys())

    for value in v2_list[:3]:
        if isinstance(value, dict):
            keys2.update(value.keys())

    if not keys1 or not keys2:
        return 0

    return len(keys1 & keys2) / len(keys1 | keys2)


def token_similarity(a, b):
    ta = tokenize(a)
    tb = tokenize(b)

    if not ta or not tb:
        return 0

    return len(ta & tb) / len(ta | tb)


def cardinality_score(v1_list, v2_list):
    t1 = get_type(v1_list[0])
    t2 = get_type(v2_list[0])
    return 1 if t1 == t2 else -1


def are_types_compatible(sql_type, mongo_type):
    if sql_type == mongo_type:
        return True

    return (
        (sql_type == "array" and mongo_type == "object")
        or (sql_type == "object" and mongo_type == "array")
    )


def parent_similarity(sql_field_full, mongo_field_full):
    if "." not in sql_field_full or "." not in mongo_field_full:
        return 0

    sql_parent = sql_field_full.rsplit(".", 1)[0]
    mongo_parent = mongo_field_full.rsplit(".", 1)[0]
    return similarity(sql_parent, mongo_parent)


def directional_penalty(sql_field, mongo_field):
    sql_tokens = sql_field.lower().split("_")
    mongo_tokens = mongo_field.lower().split("_")

    if set(sql_tokens).issubset(set(mongo_tokens)) or set(mongo_tokens).issubset(set(sql_tokens)):
        return 0

    if not sql_tokens or not mongo_tokens:
        return 0

    if sql_tokens[-1] != mongo_tokens[-1]:
        return 0

    directional_tokens = {
        "from",
        "to",
        "source",
        "target",
        "origin",
        "destination",
        "dest",
        "sender",
        "receiver",
        "incoming",
        "outgoing",
    }

    if sql_tokens[0] in directional_tokens or mongo_tokens[0] in directional_tokens:
        if sql_tokens[0] != mongo_tokens[0]:
            return 0.35

    return 0


def auto_map_fields(sql_data, mongo_data, threshold=0.5):
    sql_samples = extract_field_samples(sql_data)
    mongo_samples = extract_field_samples(mongo_data)

    mapping = {}
    used_targets = set()

    direct_matches = {}

    for sql_field_full, sql_values in sql_samples.items():
        sql_field = sql_field_full.split(".")[-1]
        sql_type = get_type(sql_values[0]) if sql_values else "primitive"

        candidates = []
        for mongo_field_full, mongo_values in mongo_samples.items():
            mongo_field = mongo_field_full.split(".")[-1]
            mongo_type = get_type(mongo_values[0]) if mongo_values else "primitive"

            if sql_field != mongo_field:
                continue

            if not are_types_compatible(sql_type, mongo_type):
                continue

            candidates.append(
                (
                    parent_similarity(sql_field_full, mongo_field_full),
                    structure_similarity(sql_values, mongo_values),
                    value_similarity(sql_values, mongo_values),
                    mongo_field_full,
                )
            )

        if candidates:
            candidates.sort(reverse=True)
            direct_match = candidates[0][-1]
            direct_matches[sql_field_full] = direct_match
            used_targets.add(direct_match)

    for sql_field_full, sql_values in sql_samples.items():
        if sql_field_full in direct_matches:
            continue

        sql_field = sql_field_full.split(".")[-1]
        sql_type = get_type(sql_values[0]) if sql_values else "primitive"

        best_match = None
        best_score = 0

        for mongo_field_full, mongo_values in mongo_samples.items():
            mongo_field = mongo_field_full.split(".")[-1]
            mongo_type = get_type(mongo_values[0]) if mongo_values else "primitive"

            if is_identifier_like(sql_field) and is_identifier_like(mongo_field):
                if normalize_name(sql_field) != normalize_name(mongo_field):
                    continue

            if not are_types_compatible(sql_type, mongo_type):
                continue

            if mongo_field_full in used_targets:
                continue

            name_score = similarity(normalize_name(sql_field), normalize_name(mongo_field))
            token_score = token_similarity(sql_field, mongo_field)

            if normalize_name(sql_field) == normalize_name(mongo_field):
                name_score += 0.5

            sql_tokens = sql_field.lower().split("_")
            mongo_tokens = mongo_field.lower().split("_")
            if sql_tokens[-1].rstrip("s") == mongo_tokens[-1].rstrip("s"):
                token_score += 0.5

            data_score = value_similarity(sql_values, mongo_values)
            struct_score = structure_similarity(sql_values, mongo_values)
            card_score = cardinality_score(sql_values, mongo_values)

            total_score = (
                0.25 * name_score
                + 0.25 * token_score
                + 0.25 * struct_score
                + 0.15 * data_score
                + 0.10 * card_score
            )

            if normalize_name(sql_field) == normalize_name(mongo_field):
                total_score = max(total_score, 0.95)

            if parent_similarity(sql_field_full, mongo_field_full) > 0.7:
                total_score += 0.2

            total_score -= directional_penalty(sql_field, mongo_field)

            if total_score > best_score:
                best_score = total_score
                best_match = mongo_field_full

        if best_match and best_score >= threshold:
            final_key = best_match.split(".")[-1]

            if sql_field != final_key:
                mapping[sql_field] = final_key
                used_targets.add(best_match)
                print(f"Mapping: {sql_field} -> {final_key} (score={best_score:.2f})")

    return mapping
