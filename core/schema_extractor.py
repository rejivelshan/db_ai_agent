import psycopg2


def extract_postgres_schema(conn):
    schema = {}

    cursor = conn.cursor()

    # 🔹 1. Get all tables
    cursor.execute("""
        SELECT table_name
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """)

    tables = [row[0] for row in cursor.fetchall()]

    for table in tables:
        schema[table] = {
            "columns": {},
            "primary_key": [],
            "foreign_keys": []
        }

        # 🔹 2. Get columns
        cursor.execute(f"""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = '{table}'
        """)

        for col, dtype in cursor.fetchall():
            schema[table]["columns"][col] = dtype

        # 🔹 3. Get primary keys
        cursor.execute(f"""
            SELECT kcu.column_name
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            WHERE tc.table_name = '{table}'
            AND tc.constraint_type = 'PRIMARY KEY'
        """)

        schema[table]["primary_key"] = [row[0] for row in cursor.fetchall()]

        # 🔹 4. Get foreign keys
        cursor.execute(f"""
            SELECT
                kcu.column_name,
                ccu.table_name AS foreign_table,
                ccu.column_name AS foreign_column
            FROM information_schema.table_constraints tc
            JOIN information_schema.key_column_usage kcu
            ON tc.constraint_name = kcu.constraint_name
            JOIN information_schema.constraint_column_usage ccu
            ON ccu.constraint_name = tc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            AND tc.table_name = '{table}'
        """)

        for row in cursor.fetchall():
            schema[table]["foreign_keys"].append({
                "column": row[0],
                "references": {
                    "table": row[1],
                    "column": row[2]
                }
            })

    cursor.close()
    return schema