from collections import defaultdict

from core import schema_mapper_runtime


IDENTIFIER_TOKENS = {"id", "number", "code", "ref", "key"}


def is_identifier_like(field_name):
    tokens = str(field_name).lower().split("_")
    return any(token in IDENTIFIER_TOKENS for token in tokens)


def get_table_key_columns(table_schema):
    return table_schema.get("primary_key") or table_schema.get("inferred_primary_key") or []


def find_identifier_field(record, table=None):
    if not isinstance(record, dict):
        return None, None

    if schema_mapper_runtime.GLOBAL_SCHEMA and table:
        table_schema = schema_mapper_runtime.GLOBAL_SCHEMA.get(table, {})
        key_columns = get_table_key_columns(table_schema)
        if key_columns and all(column in record and record[column] is not None for column in key_columns):
            if len(key_columns) == 1:
                key = key_columns[0]
                return key, record[key]
            return "|".join(key_columns), tuple(record[column] for column in key_columns)

    identifier_candidates = [
        key for key in record.keys()
        if is_identifier_like(key) and record[key] is not None
    ]
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
            key_columns = get_table_key_columns(table_schema)
            if key_columns and all(column in record and record[column] is not None for column in key_columns):
                overlap = len(set(record.keys()) & set(table_schema.get("columns", {}).keys()))
                if overlap > best_overlap:
                    best_overlap = overlap
                    if len(key_columns) == 1:
                        best_match = key_columns[0]
                    else:
                        best_match = "|".join(key_columns)

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

    ranked = sorted(
        fallback_keys,
        key=lambda key: (0 if is_identifier_like(key) else 1, key),
    )
    chosen_key = ranked[0]
    return chosen_key, record[chosen_key]


def recursive_group(data):
    if isinstance(data, dict):
        for key, value in data.items():
            if isinstance(value, list):
                data[key] = merge_list_by_id(value)
                for item in data[key]:
                    recursive_group(item)
            else:
                recursive_group(value)
    elif isinstance(data, list):
        for item in data:
            recursive_group(item)


def merge_list_by_id(items):
    merged = {}

    for item in items:
        if not isinstance(item, dict):
            continue

        item_id = find_identifier_field(item)
        if not item_id or item_id[0] is None:
            continue

        _, key_value = item_id

        if key_value not in merged:
            merged[key_value] = item
            continue

        existing = merged[key_value]
        for key, value in item.items():
            if isinstance(value, list):
                existing.setdefault(key, [])
                for sub_item in value:
                    if sub_item not in existing[key]:
                        existing[key].append(sub_item)
            elif isinstance(value, dict):
                existing[key] = value
            else:
                existing[key] = value

    return list(merged.values())


def deep_merge(existing, incoming):
    for key, value in incoming.items():
        if key not in existing:
            existing[key] = value
            continue

        if isinstance(value, list):
            existing.setdefault(key, [])
            for item in value:
                if item not in existing[key]:
                    existing[key].append(item)
        elif isinstance(value, dict):
            if isinstance(existing.get(key), dict):
                deep_merge(existing[key], value)
            else:
                existing[key] = value
        else:
            existing[key] = value


def make_hashable(value):
    if isinstance(value, dict):
        return tuple(sorted((key, make_hashable(val)) for key, val in value.items()))
    if isinstance(value, list):
        return tuple(make_hashable(item) for item in value)
    return value


def make_key(row, columns):
    values = []
    for column in columns:
        value = row.get(column)
        if value is None:
            return None
        values.append(make_hashable(value))
    return tuple(values)


def normalize_sql_tables(table_rows, mapping_tree):
    root_table = mapping_tree["table"]
    relation_state_cache = {}

    def build_relation_state(parent_table, child_table, relation):
        cache_key = (
            parent_table,
            child_table,
            tuple(relation.get("parent_columns", [])),
            tuple(relation.get("child_columns", [])),
            relation.get("mode"),
        )
        if cache_key in relation_state_cache:
            return relation_state_cache[cache_key]

        parent_groups = defaultdict(list)
        child_groups = defaultdict(list)
        parent_order = {}

        for index, row in enumerate(table_rows.get(parent_table, [])):
            key = make_key(row, relation["parent_columns"])
            if key is None:
                continue
            parent_order[index] = len(parent_groups[key])
            parent_groups[key].append(index)

        for index, row in enumerate(table_rows.get(child_table, [])):
            key = make_key(row, relation["child_columns"])
            if key is None:
                continue
            child_groups[key].append(index)

        state = {
            "parent_groups": parent_groups,
            "child_groups": child_groups,
            "parent_order": parent_order,
        }
        relation_state_cache[cache_key] = state
        return state

    def get_child_row_indexes(parent_table, parent_index, child_table, relation):
        if not relation:
            return []

        parent_row = table_rows.get(parent_table, [])[parent_index]
        key = make_key(parent_row, relation.get("parent_columns", []))
        if key is None:
            return []

        state = build_relation_state(parent_table, child_table, relation)
        child_indexes = state["child_groups"].get(key, [])

        if relation.get("mode") == "aligned":
            ordinal = state["parent_order"].get(parent_index)
            if ordinal is None or ordinal >= len(child_indexes):
                return []
            return [child_indexes[ordinal]]

        return child_indexes

    def build_node(node, row_index):
        table = node["table"]
        row = table_rows.get(table, [])[row_index]
        obj = dict(row)

        for child in node["children"]:
            child_table = child["table"]
            relation = child.get("relation")
            child_indexes = get_child_row_indexes(table, row_index, child_table, relation)
            if not child_indexes:
                continue

            child_objects = [build_node(child, child_index) for child_index in child_indexes]
            obj[child_table] = child_objects

        return obj

    final = []
    for row_index in range(len(table_rows.get(root_table, []))):
        obj = build_node(mapping_tree, row_index)
        recursive_group(obj)
        final.append(obj)

    return final


def normalize_joined_rows(rows, mapping_tree):
    if not rows:
        return []

    def get_table_fields(row, table):
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

        _, pk_value = get_primary_key(table, data)
        if pk_value is None:
            return None

        cache_key = f"{table}_{pk_value}"
        if cache_key in cache:
            return cache[cache_key]

        obj = dict(data)
        cache[cache_key] = obj

        for child in node["children"]:
            child_table = child["table"]
            child_obj = build_node(child, row, cache)
            if not child_obj:
                continue
            obj.setdefault(child_table, [])
            if child_obj not in obj[child_table]:
                obj[child_table].append(child_obj)

        return obj

    result = {}

    for row in rows:
        cache = {}
        root_obj = build_node(mapping_tree, row, cache)
        if not root_obj:
            continue

        _, root_pk_value = get_primary_key(mapping_tree["table"], root_obj)
        if root_pk_value is None:
            continue

        if root_pk_value not in result:
            result[root_pk_value] = root_obj
        else:
            deep_merge(result[root_pk_value], root_obj)

    final = []
    for obj in result.values():
        recursive_group(obj)
        final.append(obj)

    return final


def normalize_sql_data(source, mapping_tree, schema=None):
    if isinstance(source, dict):
        return normalize_sql_tables(source, mapping_tree)

    return normalize_joined_rows(source, mapping_tree)
