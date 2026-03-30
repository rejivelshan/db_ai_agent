import difflib


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


def tokenize(name):
    return [singularize(token) for token in name.lower().split("_") if token]


def normalize_label(name):
    return " ".join(tokenize(name))


def similarity(left, right):
    return difflib.SequenceMatcher(None, normalize_label(left), normalize_label(right)).ratio()


def get_reference_candidates(table_name, parent_table, parent_primary_keys):
    parent_base = singularize(parent_table.lower())
    table_base = singularize(table_name.lower())

    candidates = [
        f"{parent_base}_id",
        f"{parent_table.lower()}_id",
        f"{table_base}_{parent_base}_id",
    ]

    for primary_key in parent_primary_keys:
        if primary_key not in candidates:
            candidates.append(primary_key)

    return candidates


def score_reference_column(table_name, column, parent_table, parent_primary_key):
    column_tokens = set(tokenize(column))
    parent_tokens = set(tokenize(parent_table))
    primary_key_tokens = set(tokenize(parent_primary_key))

    legacy_candidates = set(get_reference_candidates(table_name, parent_table, [parent_primary_key]))
    score = 0

    if column == parent_primary_key:
        return 100

    if column in legacy_candidates:
        score = max(score, 95)

    if primary_key_tokens and column_tokens == primary_key_tokens:
        score = max(score, 90)

    if primary_key_tokens and primary_key_tokens.issubset(column_tokens):
        score = max(score, 82)

    if primary_key_tokens and column_tokens.issubset(primary_key_tokens) and column_tokens:
        score = max(score, 72)

    shared_primary_key_tokens = len(column_tokens & primary_key_tokens)
    shared_parent_tokens = len(column_tokens & parent_tokens)

    score += shared_primary_key_tokens * 18
    score += shared_parent_tokens * 10

    if shared_primary_key_tokens and shared_parent_tokens:
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
                "child_column": fk["column"],
                "parent_column": fk["references"]["column"],
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
            "child_column": best_match["column"],
            "parent_column": best_match["references"]["column"],
            "score": 1000,
            "explicit": True,
        }

    best_match = None
    second_best_score = -1

    for parent_primary_key in schema[parent]["primary_key"]:
        for column in details["columns"]:
            score = score_reference_column(table, column, parent, parent_primary_key)

            if best_match is None or score > best_match["score"]:
                if best_match is not None:
                    second_best_score = max(second_best_score, best_match["score"])
                best_match = {
                    "child_column": column,
                    "parent_column": parent_primary_key,
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


def infer_parent_relationship(table, schema, graph):
    best_parent = None
    second_best_score = -1

    for potential_parent in schema:
        if potential_parent == table:
            continue

        parent_details = schema[potential_parent]
        if not parent_details["primary_key"]:
            continue

        relationship = resolve_join_relationship(table, potential_parent, schema, inferred_only=True)
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


def build_relationship_graph(schema):
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
        infer_parent_relationship(table, schema, graph)

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
    primary_keys = set(schema[table]["primary_key"])

    overlap = len(table_columns & mongo_fields)
    primary_key_overlap = len(primary_keys & mongo_fields)
    name_score = similarity(table, mongo_field_name)

    score = overlap * 12 + primary_key_overlap * 6

    if singularize(table) == singularize(mongo_field_name):
        score += 30
    else:
        score += int(name_score * 20)

    for column in table_columns:
        for field_name in mongo_fields:
            if similarity(column, field_name) >= 0.85:
                score += 3

    return score


def score_root_against_mongo(table, graph, schema, mongo_schema):
    table_columns = set(schema[table]["columns"])
    primary_keys = set(schema[table]["primary_key"])
    mongo_fields = get_mongo_field_names(mongo_schema)

    primitive_overlap = len(table_columns & mongo_fields)
    primary_key_overlap = len(primary_keys & mongo_fields)

    score = primitive_overlap * 25 + primary_key_overlap * 10

    for field_name, field_schema in get_nested_mongo_fields(mongo_schema):
        best_neighbor_score = 0
        for neighbor in get_neighbor_tables(table, graph):
            if resolve_table_connection(table, neighbor, schema):
                best_neighbor_score = max(
                    best_neighbor_score,
                    score_table_for_mongo_field(neighbor, field_name, field_schema, schema),
                )
        score += best_neighbor_score

    return score


def root_score(table, graph, schema=None, mongo_schema=None):
    descendants = count_descendants(graph, table, visited={table})
    children = len(graph[table]["children"])
    score = descendants * 20 + children * 5 - len(graph[table]["parents"]) * 50

    if schema:
        primary_keys = set(schema[table]["primary_key"])
        non_key_columns = set(schema[table]["columns"]) - primary_keys
        score += len(non_key_columns)

    if schema and mongo_schema:
        score += score_root_against_mongo(table, graph, schema, mongo_schema)

    return score


def find_root_table(graph, schema=None, mongo_schema=None):
    if mongo_schema:
        candidates = list(graph.keys())
    else:
        candidates = [table for table, relations in graph.items() if not relations["parents"]]
        if not candidates:
            candidates = list(graph.keys())

    return max(
        candidates,
        key=lambda table: (root_score(table, graph, schema, mongo_schema), table),
    ) if candidates else None


def choose_primary_parent(table, graph, schema):
    parents = graph[table]["parents"]
    if not parents:
        return None

    child_tokens = set(tokenize(table))
    ranked = []

    for parent in parents:
        relationship = resolve_join_relationship(table, parent, schema)
        score = relationship["score"] if relationship else 0

        parent_tokens = set(tokenize(parent))
        if child_tokens & parent_tokens:
            score += 25
        if parent_tokens and parent_tokens.issubset(child_tokens):
            score += 15

        ranked.append((score, parent))

    ranked.sort(reverse=True)
    return ranked[0][1]


def resolve_table_connection(parent_table, child_table, schema):
    child_references_parent = resolve_join_relationship(child_table, parent_table, schema)
    if child_references_parent:
        return {
            "parent_table": parent_table,
            "child_table": child_table,
            "parent_column": child_references_parent["parent_column"],
            "child_column": child_references_parent["child_column"],
            "child_references_parent": True,
        }

    parent_references_child = resolve_join_relationship(parent_table, child_table, schema)
    if parent_references_child:
        return {
            "parent_table": parent_table,
            "child_table": child_table,
            "parent_column": parent_references_child["child_column"],
            "child_column": parent_references_child["parent_column"],
            "child_references_parent": False,
        }

    return None


def get_neighbor_tables(table, graph):
    neighbors = []

    for neighbor in graph[table]["children"] + graph[table]["parents"]:
        if neighbor not in neighbors:
            neighbors.append(neighbor)

    return neighbors


def generate_join_query(mapping_tree, schema):
    root = mapping_tree["table"]

    select_fields = []
    joins = []
    alias_map = {}
    alias_counter = 1

    def assign_alias(table):
        nonlocal alias_counter
        alias = f"t{alias_counter}"
        alias_counter += 1
        alias_map[table] = alias
        return alias

    def traverse(node, parent=None):
        table = node["table"]
        alias = alias_map.get(table) or assign_alias(table)

        for col in schema[table]["columns"]:
            select_fields.append(f"{alias}.{col} AS {table}_{col}")

        if parent:
            parent_alias = alias_map[parent]
            relationship = resolve_table_connection(parent, table, schema)
            if relationship:
                joins.append(
                    f"LEFT JOIN {table} {alias} ON "
                    f"{parent_alias}.{relationship['parent_column']} = {alias}.{relationship['child_column']}"
                )

        for child in node["children"]:
            traverse(child, table)

    assign_alias(root)
    traverse(mapping_tree)

    query = f"""
    SELECT {', '.join(select_fields)}
    FROM {root} {alias_map[root]}
    {' '.join(joins)}
    """

    return query


def build_mapping_tree(graph, root, schema=None, mongo_schema=None):
    schema = schema or {}
    primary_parents = {
        table: choose_primary_parent(table, graph, schema) if schema else None
        for table in graph
    }
    primary_parents[root] = None

    def build_node(table, visited, mongo_node=None):
        visited = visited | {table}
        children = []
        used_tables = set()

        if mongo_node:
            for field_name, field_schema in get_nested_mongo_fields(mongo_node):
                best_candidate = None
                best_score = 0

                for candidate in get_neighbor_tables(table, graph):
                    if candidate in visited or candidate in used_tables:
                        continue
                    if not resolve_table_connection(table, candidate, schema):
                        continue

                    score = score_table_for_mongo_field(candidate, field_name, field_schema, schema)
                    if score > best_score:
                        best_score = score
                        best_candidate = candidate

                if best_candidate and best_score >= 20:
                    children.append(build_node(best_candidate, visited, field_schema))
                    used_tables.add(best_candidate)

        for candidate in graph[table]["children"]:
            if candidate in visited or candidate in used_tables:
                continue
            if primary_parents.get(candidate) != table:
                continue
            children.append(build_node(candidate, visited, None))

        return {
            "table": table,
            "children": children,
        }

    return build_node(root, set(), mongo_schema)
