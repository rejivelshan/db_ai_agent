import difflib
import itertools
from collections import Counter


def singularize(name):
    if name.endswith("ies") and len(name) > 3:
        return name[:-3] + "y"
    if name.endswith("s") and len(name) > 3:
        return name[:-1]
    return name


PREFERRED_OWNERSHIP_PREFIXES = {
    "from": 4,
    "source": 4,
    "owner": 4,
    "primary": 3,
}

DEPRIORITIZED_PREFIXES = {
    "to": -4,
    "target": -4,
    "destination": -4,
    "dest": -4,
    "receiver": -3,
}

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
GENERIC_MATCH_TOKENS = {"date", "name", "status", "time", "type", "value"}
MEASURE_TOKENS = {"amount", "fare", "price", "balance", "rating", "status", "date", "time"}


def tokenize(name):
    return [singularize(token) for token in name.lower().split("_") if token]


def normalize_label(name):
    return " ".join(tokenize(name))


def similarity(left, right):
    return difflib.SequenceMatcher(None, normalize_label(left), normalize_label(right)).ratio()


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


def count_keys(rows, columns):
    counts = Counter()
    for row in rows:
        key = make_key(row, columns)
        if key is not None:
            counts[key] += 1
    return counts


def is_identifier_like(name):
    return any(token in IDENTIFIER_TOKENS for token in tokenize(name))


def is_entity_key_like(name):
    return any(token in ENTITY_KEY_TOKENS for token in tokenize(name))


def is_measure_like(name):
    return any(token in MEASURE_TOKENS for token in tokenize(name))


def unique_key_score(combo):
    identifier_count = sum(1 for column in combo if is_identifier_like(column))
    entity_key_count = sum(1 for column in combo if is_entity_key_like(column))
    attribute_count = sum(
        1
        for column in combo
        if any(token in ATTRIBUTE_TOKENS for token in tokenize(column))
    )
    specificity = sum(
        sum(token not in ATTRIBUTE_TOKENS for token in tokenize(column))
        for column in combo
    )
    return (
        attribute_count,
        sum(1 for column in combo if is_measure_like(column)),
        -identifier_count,
        -entity_key_count,
        -specificity,
        len(combo),
        combo,
    )


def infer_unique_key_columns(rows, columns, max_size=3):
    if not rows or not columns:
        return []

    columns = sorted(columns)
    limit = min(max_size, len(columns))

    for size in range(1, limit + 1):
        valid_combos = []
        for combo in itertools.combinations(columns, size):
            counts = count_keys(rows, combo)
            if len(counts) != len(rows):
                continue
            if sum(counts.values()) != len(rows):
                continue
            valid_combos.append(combo)

        if valid_combos:
            return list(min(valid_combos, key=unique_key_score))

    return []


def profile_schema(schema, table_rows):
    profiled = {}

    for table, details in schema.items():
        updated = {
            **details,
            "columns": dict(details.get("columns", {})),
            "primary_key": list(details.get("primary_key", [])),
            "foreign_keys": list(details.get("foreign_keys", [])),
        }

        rows = table_rows.get(table, [])
        updated["row_count"] = len(rows)
        updated["inferred_primary_key"] = []

        if not updated["primary_key"]:
            inferred = infer_unique_key_columns(rows, updated["columns"].keys())
            if inferred:
                updated["inferred_primary_key"] = inferred

        profiled[table] = updated

    return profiled


def get_key_columns(details):
    return details.get("primary_key") or details.get("inferred_primary_key") or []


def get_reference_candidates(table_name, parent_table, parent_key_columns):
    parent_base = singularize(parent_table.lower())
    table_base = singularize(table_name.lower())

    candidates = [
        f"{parent_base}_id",
        f"{parent_table.lower()}_id",
        f"{table_base}_{parent_base}_id",
    ]

    for key_column in parent_key_columns:
        if key_column not in candidates:
            candidates.append(key_column)

    return candidates


def score_reference_column(table_name, column, parent_table, parent_key_column):
    column_tokens = set(tokenize(column))
    parent_tokens = set(tokenize(parent_table))
    key_tokens = set(tokenize(parent_key_column))
    strong_column_tokens = column_tokens - GENERIC_MATCH_TOKENS
    strong_key_tokens = key_tokens - GENERIC_MATCH_TOKENS

    legacy_candidates = set(get_reference_candidates(table_name, parent_table, [parent_key_column]))
    score = 0

    if column == parent_key_column:
        return 100

    if column in legacy_candidates:
        score = max(score, 95)

    if key_tokens and column_tokens == key_tokens:
        score = max(score, 90)

    if strong_key_tokens and strong_key_tokens.issubset(column_tokens):
        score = max(score, 82)

    if strong_column_tokens and strong_column_tokens.issubset(key_tokens):
        score = max(score, 72)

    shared_key_tokens = len((column_tokens & key_tokens) - GENERIC_MATCH_TOKENS)
    shared_generic_tokens = len((column_tokens & key_tokens) & GENERIC_MATCH_TOKENS)
    shared_parent_tokens = len(column_tokens & parent_tokens)

    score += shared_key_tokens * 18
    score += shared_generic_tokens * 3
    score += shared_parent_tokens * 10

    if shared_key_tokens and shared_parent_tokens:
        score += 12

    tokens = tokenize(column)
    if tokens:
        score += PREFERRED_OWNERSHIP_PREFIXES.get(tokens[0], 0)
        score += DEPRIORITIZED_PREFIXES.get(tokens[0], 0)

    return score


def resolve_join_relationship(table, parent, schema, inferred_only=False):
    details = schema[table]
    explicit_matches = [
        fk for fk in details["foreign_keys"]
        if fk["references"]["table"] == parent
    ]

    if explicit_matches and not inferred_only:
        if len(explicit_matches) == 1:
            fk = explicit_matches[0]
            return {
                "parent_columns": [fk["references"]["column"]],
                "child_columns": [fk["column"]],
                "mode": "match",
                "score": 1000,
                "explicit": True,
            }

        ranked_matches = sorted(
            explicit_matches,
            key=lambda fk: (
                -score_reference_column(table, fk["column"], parent, fk["references"]["column"]),
                fk["column"],
            ),
        )
        best_match = ranked_matches[0]
        return {
            "parent_columns": [best_match["references"]["column"]],
            "child_columns": [best_match["column"]],
            "mode": "match",
            "score": 1000,
            "explicit": True,
        }

    parent_key_columns = get_key_columns(schema[parent])
    if not parent_key_columns:
        return None

    if len(parent_key_columns) > 1:
        if all(column in details["columns"] for column in parent_key_columns):
            return {
                "parent_columns": list(parent_key_columns),
                "child_columns": list(parent_key_columns),
                "mode": "match",
                "score": 95,
                "explicit": False,
            }
        return None

    parent_key_column = parent_key_columns[0]
    best_match = None
    second_best_score = -1

    for column in details["columns"]:
        score = score_reference_column(table, column, parent, parent_key_column)

        if best_match is None or score > best_match["score"]:
            if best_match is not None:
                second_best_score = max(second_best_score, best_match["score"])
            best_match = {
                "parent_columns": [parent_key_column],
                "child_columns": [column],
                "mode": "match",
                "score": score,
                "explicit": False,
            }
        else:
            second_best_score = max(second_best_score, score)

    if not best_match:
        return None

    if best_match["score"] < 40:
        return None

    if second_best_score >= 0 and best_match["score"] < second_best_score + 4:
        return None

    return best_match


def infer_shared_column_relationship(parent_table, child_table, schema, table_rows):
    if not table_rows:
        return None

    parent_rows = table_rows.get(parent_table, [])
    child_rows = table_rows.get(child_table, [])
    if not parent_rows or not child_rows:
        return None

    common_columns = sorted(
        set(schema[parent_table]["columns"]) & set(schema[child_table]["columns"])
    )
    if not common_columns:
        return None

    best_match = None

    max_size = min(3, len(common_columns))
    for size in range(max_size, 0, -1):
        for combo in itertools.combinations(common_columns, size):
            parent_counts = count_keys(parent_rows, combo)
            child_counts = count_keys(child_rows, combo)
            if not parent_counts or not child_counts:
                continue

            matched_keys = set(parent_counts) & set(child_counts)
            if not matched_keys:
                continue

            matched_child = sum(child_counts[key] for key in matched_keys)
            matched_parent = sum(parent_counts[key] for key in matched_keys)
            child_total = sum(child_counts.values())
            parent_total = sum(parent_counts.values())

            child_coverage = matched_child / child_total if child_total else 0
            parent_coverage = matched_parent / parent_total if parent_total else 0

            if child_coverage < 0.5:
                continue

            exact_partition = (
                matched_keys
                and child_coverage == 1
                and parent_coverage == 1
                and all(child_counts[key] == parent_counts[key] for key in matched_keys)
            )

            score = (
                child_coverage * 60
                + parent_coverage * 20
                + len(combo) * 12
            )

            if exact_partition:
                score += 20
            elif all(parent_counts[key] == 1 for key in matched_keys):
                score += 8

            candidate = {
                "parent_columns": list(combo),
                "child_columns": list(combo),
                "mode": "aligned" if exact_partition else "match",
                "score": score,
                "explicit": False,
            }

            if best_match is None or candidate["score"] > best_match["score"]:
                best_match = candidate

    if best_match and best_match["score"] >= 55:
        return best_match

    return None


def infer_parent_relationship(table, schema, graph, table_rows=None):
    best_parent = None
    second_best_score = -1

    for potential_parent in schema:
        if potential_parent == table:
            continue

        relationship = resolve_join_relationship(table, potential_parent, schema, inferred_only=True)
        if not relationship:
            relationship = infer_shared_column_relationship(
                potential_parent,
                table,
                schema,
                table_rows,
            )

        if not relationship:
            continue

        score = relationship["score"]
        if best_parent is None or score > best_parent["relationship"]["score"]:
            if best_parent is not None:
                second_best_score = max(second_best_score, best_parent["relationship"]["score"])
            best_parent = {
                "table": potential_parent,
                "relationship": relationship,
            }
        else:
            second_best_score = max(second_best_score, score)

    if not best_parent:
        return

    if second_best_score >= 0 and best_parent["relationship"]["score"] < second_best_score + 4:
        return

    parent = best_parent["table"]
    if table not in graph[parent]["children"]:
        graph[parent]["children"].append(table)
    if parent not in graph[table]["parents"]:
        graph[table]["parents"].append(parent)


def build_relationship_graph(schema, table_rows=None):
    graph = {}

    for table in schema:
        graph[table] = {
            "children": [],
            "parents": [],
        }

    for table, details in schema.items():
        for fk in details["foreign_keys"]:
            parent = fk["references"]["table"]
            if table not in graph[parent]["children"]:
                graph[parent]["children"].append(table)
            if parent not in graph[table]["parents"]:
                graph[table]["parents"].append(parent)

    for table, details in schema.items():
        if details["foreign_keys"]:
            continue
        infer_parent_relationship(table, schema, graph, table_rows)

    return graph


def count_descendants(graph, table, visited=None):
    visited = visited or set()
    total = 0

    for child in graph[table]["children"]:
        if child in visited:
            continue
        visited.add(child)
        total += 1 + count_descendants(graph, child, visited)

    return total


def unwrap_mongo_schema(node):
    if not isinstance(node, dict):
        return {}

    if "type" not in node:
        return node

    if node["type"] == "array":
        return node.get("items", {}) if isinstance(node.get("items"), dict) else {}

    if node["type"] == "object":
        return node.get("schema", {}) if isinstance(node.get("schema"), dict) else {}

    return {}


def get_nested_mongo_fields(node):
    schema_node = unwrap_mongo_schema(node)
    nested = []

    if not isinstance(schema_node, dict):
        return nested

    for key, value in schema_node.items():
        if isinstance(value, dict) and value.get("type") in {"array", "object"}:
            nested.append((key, value))

    return nested


def get_mongo_field_names(node):
    schema_node = unwrap_mongo_schema(node)
    if not isinstance(schema_node, dict):
        return set()
    return set(schema_node.keys())


def score_table_for_mongo_field(table, mongo_field_name, mongo_node, schema):
    table_columns = set(schema[table]["columns"])
    mongo_fields = get_mongo_field_names(mongo_node)
    key_columns = set(get_key_columns(schema[table]))

    overlap = len(table_columns & mongo_fields)
    key_overlap = len(key_columns & mongo_fields)
    name_score = similarity(table, mongo_field_name)

    score = overlap * 12 + key_overlap * 6

    if singularize(table) == singularize(mongo_field_name):
        score += 30
    else:
        score += int(name_score * 20)

    for column in table_columns:
        for field_name in mongo_fields:
            if similarity(column, field_name) >= 0.85:
                score += 3

    return score


def get_candidate_tables(table, graph, schema, table_rows=None):
    candidates = []

    for neighbor in graph.get(table, {}).get("children", []) + graph.get(table, {}).get("parents", []):
        if neighbor not in candidates:
            candidates.append(neighbor)

    for other_table in schema:
        if other_table == table or other_table in candidates:
            continue
        if resolve_table_connection(table, other_table, schema, table_rows):
            candidates.append(other_table)

    return candidates


def score_root_against_mongo(table, graph, schema, mongo_schema, table_rows=None):
    table_columns = set(schema[table]["columns"])
    key_columns = set(get_key_columns(schema[table]))
    mongo_fields = get_mongo_field_names(mongo_schema)

    primitive_overlap = len(table_columns & mongo_fields)
    key_overlap = len(key_columns & mongo_fields)

    score = primitive_overlap * 25 + key_overlap * 10

    for field_name, field_schema in get_nested_mongo_fields(mongo_schema):
        best_neighbor_score = 0
        for neighbor in get_candidate_tables(table, graph, schema, table_rows):
            if resolve_table_connection(table, neighbor, schema, table_rows):
                best_neighbor_score = max(
                    best_neighbor_score,
                    score_table_for_mongo_field(neighbor, field_name, field_schema, schema),
                )
        score += best_neighbor_score

    return score


def root_score(table, graph, schema=None, mongo_schema=None, table_rows=None):
    descendants = count_descendants(graph, table, visited={table})
    children = len(graph[table]["children"])
    score = descendants * 20 + children * 5 - len(graph[table]["parents"]) * 50

    if schema:
        key_columns = set(get_key_columns(schema[table]))
        non_key_columns = set(schema[table]["columns"]) - key_columns
        score += len(non_key_columns)

    if schema and mongo_schema:
        score += score_root_against_mongo(table, graph, schema, mongo_schema, table_rows)

    return score


def find_root_table(graph, schema=None, mongo_schema=None, table_rows=None):
    if mongo_schema:
        candidates = list(graph.keys())
    else:
        candidates = [table for table, relations in graph.items() if not relations["parents"]]
        if not candidates:
            candidates = list(graph.keys())

    return max(
        candidates,
        key=lambda table: (root_score(table, graph, schema, mongo_schema, table_rows), table),
    ) if candidates else None


def choose_primary_parent(table, graph, schema, table_rows=None):
    parents = graph[table]["parents"]
    if not parents:
        return None

    child_tokens = set(tokenize(table))
    ranked = []

    for parent in parents:
        relationship = resolve_table_connection(parent, table, schema, table_rows)
        score = relationship["score"] if relationship else 0

        parent_tokens = set(tokenize(parent))
        if child_tokens & parent_tokens:
            score += 25
        if parent_tokens and parent_tokens.issubset(child_tokens):
            score += 15

        ranked.append((score, parent))

    ranked.sort(reverse=True)
    return ranked[0][1]


def resolve_table_connection(parent_table, child_table, schema, table_rows=None):
    candidates = []

    child_references_parent = resolve_join_relationship(child_table, parent_table, schema)
    if child_references_parent:
        candidates.append({
            **child_references_parent,
            "parent_table": parent_table,
            "child_table": child_table,
            "score": child_references_parent["score"],
            "_preference": 3,
        })

    shared = infer_shared_column_relationship(parent_table, child_table, schema, table_rows)
    if shared:
        candidates.append({
            **shared,
            "parent_table": parent_table,
            "child_table": child_table,
            "_preference": 2,
        })

    parent_references_child = resolve_join_relationship(parent_table, child_table, schema)
    if parent_references_child:
        adjusted_score = parent_references_child["score"]
        if not parent_references_child["explicit"]:
            adjusted_score -= 12

        candidates.append({
            "parent_table": parent_table,
            "child_table": child_table,
            "parent_columns": parent_references_child["child_columns"],
            "child_columns": parent_references_child["parent_columns"],
            "mode": parent_references_child["mode"],
            "score": adjusted_score,
            "explicit": parent_references_child["explicit"],
            "_preference": 1,
        })

    if candidates:
        chosen = max(
            candidates,
            key=lambda relationship: (
                relationship["score"],
                1 if relationship.get("explicit") else 0,
                relationship.get("_preference", 0),
            ),
        )
        chosen.pop("_preference", None)
        return chosen

    return None


def format_relation_columns(relation):
    return ", ".join(
        f"{parent}={child}"
        for parent, child in zip(relation["parent_columns"], relation["child_columns"])
    )


def generate_join_query(mapping_tree, schema, table_rows=None):
    root = mapping_tree["table"]
    lines = [f"ROOT {root}"]

    def traverse(node, depth=0):
        indent = "  " * depth
        for child in node["children"]:
            relation = child.get("relation") or resolve_table_connection(
                node["table"],
                child["table"],
                schema,
                table_rows,
            )
            if relation:
                mode = relation.get("mode", "match")
                relation_desc = format_relation_columns(relation)
                if mode == "aligned":
                    lines.append(
                        f"{indent}{node['table']} -> {child['table']} "
                        f"(row-aligned on {relation_desc})"
                    )
                else:
                    lines.append(
                        f"{indent}{node['table']} -> {child['table']} "
                        f"(match on {relation_desc})"
                    )
            else:
                lines.append(f"{indent}{node['table']} -> {child['table']}")
            traverse(child, depth + 1)

    traverse(mapping_tree)
    return "\n".join(lines)


def build_mapping_tree(graph, root, schema=None, mongo_schema=None, table_rows=None):
    schema = schema or {}
    primary_parents = {
        table: choose_primary_parent(table, graph, schema, table_rows) if schema else None
        for table in graph
    }
    primary_parents[root] = None

    def build_node(table, visited, mongo_node=None, relation=None):
        visited = visited | {table}
        children = []
        used_tables = set()

        if mongo_node:
            for field_name, field_schema in get_nested_mongo_fields(mongo_node):
                best_candidate = None
                best_relation = None
                best_score = 0

                for candidate in get_candidate_tables(table, graph, schema, table_rows):
                    if candidate in visited or candidate in used_tables:
                        continue

                    relationship = resolve_table_connection(table, candidate, schema, table_rows)
                    if not relationship:
                        continue

                    score = relationship["score"] + score_table_for_mongo_field(
                        candidate,
                        field_name,
                        field_schema,
                        schema,
                    )
                    if score > best_score:
                        best_score = score
                        best_candidate = candidate
                        best_relation = relationship

                if best_candidate and best_score >= 20:
                    children.append(
                        build_node(
                            best_candidate,
                            visited,
                            field_schema,
                            best_relation,
                        )
                    )
                    used_tables.add(best_candidate)

        if not mongo_node:
            for candidate in graph[table]["children"]:
                if candidate in visited or candidate in used_tables:
                    continue
                if primary_parents.get(candidate) != table:
                    continue
                relationship = resolve_table_connection(table, candidate, schema, table_rows)
                children.append(build_node(candidate, visited, None, relationship))

        node = {
            "table": table,
            "children": children,
        }

        if relation:
            node["relation"] = relation

        return node

    return build_node(root, set(), mongo_schema)
