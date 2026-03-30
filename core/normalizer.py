from core import schema_mapper_runtime


def is_identifier_like(field_name):
    return "id" in str(field_name).lower().split("_")


def find_identifier_field(record, table=None):
    if not isinstance(record, dict):
        return None, None

    if schema_mapper_runtime.GLOBAL_SCHEMA and table:
        table_schema = schema_mapper_runtime.GLOBAL_SCHEMA.get(table, {})
        primary_keys = table_schema.get("primary_key", [])
        if primary_keys and all(pk in record and record[pk] is not None for pk in primary_keys):
            if len(primary_keys) == 1:
                pk = primary_keys[0]
                return pk, record[pk]
            return "|".join(primary_keys), tuple(record[pk] for pk in primary_keys)

    identifier_candidates = [key for key in record.keys() if is_identifier_like(key) and record[key] is not None]
    if identifier_candidates:
        non_fk_candidates = [
            key for key in identifier_candidates
            if not schema_mapper_runtime.is_foreign_key(key)
        ]
        chosen_pool = non_fk_candidates or identifier_candidates
        chosen_key = sorted(chosen_pool)[0]
        return chosen_key, record[chosen_key]

    if schema_mapper_runtime.GLOBAL_SCHEMA and not table:
        best_match = None
        best_overlap = -1

        for table_schema in schema_mapper_runtime.GLOBAL_SCHEMA.values():
            primary_keys = table_schema.get("primary_key", [])
            if primary_keys and all(pk in record and record[pk] is not None for pk in primary_keys):
                overlap = len(set(record.keys()) & set(table_schema.get("columns", {}).keys()))
                if overlap > best_overlap:
                    best_overlap = overlap
                    if len(primary_keys) == 1:
                        best_match = primary_keys[0]
                    else:
                        best_match = "|".join(primary_keys)

        if best_match:
            if "|" in best_match:
                parts = best_match.split("|")
                return best_match, tuple(record[part] for part in parts)
            return best_match, record[best_match]

    fallback_keys = []
    for key, value in record.items():
        if value is None or isinstance(value, (dict, list)):
            continue
        fallback_keys.append(key)

    if not fallback_keys:
        return None, None

    chosen_key = "id" if "id" in fallback_keys else sorted(fallback_keys)[0]
    return chosen_key, record[chosen_key]

def recursive_group(data):
    """
    Recursively group lists by *_id dynamically
    NO hardcoding
    """

    if isinstance(data, dict):
        for key, value in data.items():

            if isinstance(value, list):
                # 🔥 group list if it contains dicts with *_id
                data[key] = merge_list_by_id(value)

                # recurse inside each item
                for item in data[key]:
                    recursive_group(item)

            else:
                recursive_group(value)

    elif isinstance(data, list):
        for item in data:
            recursive_group(item)

def merge_list_by_id(items):

    """
    Merge list of dicts by *_id key
    """

    merged = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        # find id field
        item_id = find_identifier_field(item)

        if not item_id or item_id[0] is None:
            continue

        key_name, key_value = item_id

        if key_value not in merged:
            merged[key_value] = item
        else:
            existing = merged[key_value]

            for k, v in item.items():
                if isinstance(v, list):
                    existing.setdefault(k, [])
                    for sub in v:
                        if sub not in existing[k]:
                            existing[k].append(sub)

                elif isinstance(v, dict):
                    existing[k] = v

                else:
                    existing[k] = v

    return list(merged.values())


def deep_merge(existing, incoming):
    """
    Recursively merge nested objects and lists
    """

    for key, value in incoming.items():

        if key not in existing:
            existing[key] = value
            continue

        # 🔥 list merge (deduplicate)
        if isinstance(value, list):
            existing.setdefault(key, [])
            for item in value:
                if item not in existing[key]:
                    existing[key].append(item)

        # 🔥 dict merge (recursive)
        elif isinstance(value, dict):
            if isinstance(existing.get(key), dict):
                deep_merge(existing[key], value)
            else:
                existing[key] = value

        # 🔥 primitive overwrite
        else:
            existing[key] = value


def normalize_sql_data(rows, mapping_tree):
    """
    Fully dynamic SQL → Nested JSON normalizer
    Works for ANY schema using mapping_tree
    """

    if not rows:
        return []

    def get_table_fields(row, table):
        """Extract all fields for a given table from row"""
        data = {}
        for key, value in row.items():
            if key.startswith(f"{table}_"):
                field = key[len(table) + 1:]
                if value is not None:
                    data[field] = value
        return data

    def get_primary_key(table, data):
        return find_identifier_field(data, table)

    def build_node(node, row, cache):
        table = node["table"]

        data = get_table_fields(row, table)

        if not data:
            return None

        pk_key, pk_value = get_primary_key(table, data)

        if pk_value is None:
            return None

        cache_key = f"{table}_{pk_value}"

        if cache_key in cache:
            return cache[cache_key]

        obj = dict(data)
        cache[cache_key] = obj

        # 🔥 Process children dynamically
        for child in node["children"]:
            child_table = child["table"]

            child_obj = build_node(child, row, cache)

            if not child_obj:
                continue

            # 🔥 ALWAYS treat children as list (correct for JOIN explosion)
            obj.setdefault(child_table, [])

            if child_obj not in obj[child_table]:
                obj[child_table].append(child_obj)

        return obj

    # 🔥 MAIN AGGREGATION
    result = {}

    for row in rows:
        cache = {}

        root_obj = build_node(mapping_tree, row, cache)

        if not root_obj:
            continue

        root_pk_key, root_pk_value = get_primary_key(mapping_tree["table"], root_obj)

        if root_pk_value is None:
            continue

        if root_pk_value not in result:
            result[root_pk_value] = root_obj
        else:
            existing = result[root_pk_value]

            # 🔥 Merge logic
            # 🔥 DEEP MERGE (FIXES DUPLICATES PROPERLY)
            deep_merge(existing, root_obj)

    final = []

    for obj in result.values():
        # 🔥 APPLY GENERIC GROUPING
        recursive_group(obj)

        final.append(obj)

    return final
