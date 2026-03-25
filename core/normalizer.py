def normalize_sql_data(rows):
    users = {}

    for row in rows:
        user_id = row["user_id"]

        if user_id not in users:
            users[user_id] = {
                "user_id": user_id,
                "name": row["name"],
                "email": row["email"],
                "orders": {}
            }

        order_id = row["order_id"]

        if order_id not in users[user_id]["orders"]:
            users[user_id]["orders"][order_id] = {
                "order_id": order_id,
                "total_amount": row["total_amount"],
                "items": []
            }

        users[user_id]["orders"][order_id]["items"].append({
            "product_name": row["product_name"],
            "quantity": row["quantity"]
        })

    result = []
    for user in users.values():
        user["orders"] = list(user["orders"].values())
        result.append(user)

    return result