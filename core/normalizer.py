def normalize_sql_data(rows, mapping_tree):
    """
    Convert flat SQL rows into nested JSON using mapping tree
    """

    root_table = mapping_tree["table"]
    root_key = f"{root_table}_user_id" if root_table == "users" else None

    result = {}

    def get_value(row, table, column):
        return row.get(f"{table}_{column}")

    for row in rows:
        # 🔥 ROOT LEVEL (users)
        root_id = get_value(row, root_table, "user_id")

        if root_id is None:
            continue

        if root_id not in result:
            result[root_id] = {
                "user_id": root_id,
                "name": get_value(row, root_table, "name"),
                "email": get_value(row, root_table, "email"),
                "orders": {}
            }

        user = result[root_id]

        # 🔥 ORDERS LEVEL
        order_id = get_value(row, "orders", "order_id")

        if order_id is None:
            continue

        if order_id not in user["orders"]:
            user["orders"][order_id] = {
                "order_id": order_id,
                "total_amount": get_value(row, "orders", "total_amount"),
                "items": []
            }

        order = user["orders"][order_id]

        # 🔥 ITEMS LEVEL
        product_name = get_value(row, "order_items", "product_name")
        quantity = get_value(row, "order_items", "quantity")

        if product_name is not None:
            order["items"].append({
                "product_name": product_name,
                "quantity": quantity
            })

    # 🔥 FINAL FORMAT CLEANUP
    final_result = []
    for user in result.values():
        user["orders"] = list(user["orders"].values())
        final_result.append(user)

    return final_result