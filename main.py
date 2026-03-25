from connectors.postgres_connector import PostgresConnector
from connectors.mongo_connector import MongoConnector
from core.normalizer import normalize_sql_data

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