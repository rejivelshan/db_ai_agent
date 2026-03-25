from connectors.postgres_connector import PostgresConnector
from connectors.mongo_connector import MongoConnector
from core.normalizer import normalize_sql_data
from core.comparator import compare_data
from core.reporter import export_to_csv

# PostgreSQL
pg = PostgresConnector()
pg.connect()

sql_query = """
SELECT 
    u.user_id,
    u.name,
    u.email,
    o.order_id,
    o.total_amount,
    oi.product_name,
    oi.quantity
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id;
"""

sql_data = pg.fetch_data(sql_query)

# Normalize SQL
normalized_sql = normalize_sql_data(sql_data)

print("\n✅ Normalized SQL:")
print(normalized_sql)

# Mongo
mongo = MongoConnector()
mongo.connect()

mongo_data = mongo.fetch_data("orders_embedded")

print("\n✅ Mongo Data:")
print(mongo_data)

mismatches = compare_data(normalized_sql, mongo_data)

print("\n🔍 Comparison Result:")

if not mismatches:
    print("✅ PERFECT MATCH — Data is identical")
else:
    print("❌ Mismatches found:")
    for m in mismatches:
        print(m)

print("\n📊 Summary:")

total = len(mismatches)
print(f"Total mismatches: {total}")

if total > 0:
    affected_users = set()
    for m in mismatches:
        if "user_id=" in m["path"]:
            uid = m["path"].split("=")[1].split(".")[0]
            affected_users.add(uid)

    print(f"Affected users: {list(affected_users)}")

export_to_csv(mismatches, "reports/report.csv")