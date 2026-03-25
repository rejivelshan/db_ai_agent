import psycopg2
import os
from dotenv import load_dotenv

load_dotenv()


class PostgresConnector:
    def __init__(self):
        self.conn = None

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=os.getenv("POSTGRES_HOST"),
                database=os.getenv("POSTGRES_DB"),
                user=os.getenv("POSTGRES_USER"),
                password=os.getenv("POSTGRES_PASSWORD"),
                port=os.getenv("POSTGRES_PORT")
            )
            print("✅ PostgreSQL Connected")
        except Exception as e:
            print("❌ PostgreSQL Connection failed:", e)

    def fetch_data(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]

        except Exception as e:
            print("❌ PostgreSQL Fetch failed:", e)
            return []