def build_relationship_graph(schema):
    graph = {}

    # init
    for table in schema:
        graph[table] = {
            "children": [],
            "parents": []
        }

    # 🔥 1. real FK
    for table, details in schema.items():
        for fk in details["foreign_keys"]:
            parent = fk["references"]["table"]

            graph[parent]["children"].append(table)
            graph[table]["parents"].append(parent)

    # 🔥 2. SAFE INFERENCE (FIXED)
    for table, details in schema.items():
        for column in details["columns"]:

            if column.endswith("_id"):

                for potential_parent in schema:
                    if potential_parent == table:
                        continue

                    # 🔥 ONLY match parent PK
                    parent_pk = schema[potential_parent]["primary_key"]

                    if column in parent_pk:
                        if potential_parent not in graph[table]["parents"]:
                            graph[potential_parent]["children"].append(table)
                            graph[table]["parents"].append(potential_parent)

    return graph


def find_root_table(graph):
    for table, relations in graph.items():
        if not relations["parents"]:
            return table
    return None

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

        if table not in alias_map:
            alias = assign_alias(table)
        else:
            alias = alias_map[table]

        # 🔥 SELECT fields
        for col in schema[table]["columns"]:
            select_fields.append(f"{alias}.{col} AS {table}_{col}")

        # 🔥 JOIN logic
        if parent:
            parent_alias = alias_map[parent]

            # find FK or inferred relationship
            join_found = False

            # check FK
            for fk in schema[table]["foreign_keys"]:
                if fk["references"]["table"] == parent:
                    joins.append(
                        f"LEFT JOIN {table} {alias} ON {alias}.{fk['column']} = {parent_alias}.{fk['references']['column']}"
                    )
                    join_found = True
                    break

            # 🔥 fallback inference
            if not join_found:
                for col in schema[table]["columns"]:
                    if col.endswith("_id") and col in schema[parent]["primary_key"]:
                        joins.append(
                            f"LEFT JOIN {table} {alias} ON {alias}.{col} = {parent_alias}.{col}"
                        )
                        break

        # 🔥 RECURSIVE JOIN (THIS WAS MISSING ❌)
        for child in node["children"]:
            traverse(child, table)

    # root alias
    assign_alias(root)

    traverse(mapping_tree)

    query = f"""
    SELECT {', '.join(select_fields)}
    FROM {root} {alias_map[root]}
    {' '.join(joins)}
    """

    return query


def build_mapping_tree(graph, root):
    def build_node(table):
        return {
            "table": table,
            "children": [build_node(child) for child in graph[table]["children"]]
        }

    return build_node(root)
