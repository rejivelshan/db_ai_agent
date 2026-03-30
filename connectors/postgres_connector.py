import os

import psycopg2
from dotenv import load_dotenv

load_dotenv(override=True)


class PostgresConnector:
    def __init__(
        self,
        host=None,
        database=None,
        user=None,
        password=None,
        port=None,
    ):
        self.conn = None
        self.host = host or os.getenv("POSTGRES_HOST")
        self.database = database or os.getenv("POSTGRES_DB")
        self.user = user or os.getenv("POSTGRES_USER")
        self.password = password or os.getenv("POSTGRES_PASSWORD")
        self.port = port or os.getenv("POSTGRES_PORT")

    def connect(self):
        try:
            self.conn = psycopg2.connect(
                host=self.host,
                database=self.database,
                user=self.user,
                password=self.password,
                port=self.port,
            )
            print(f"PostgreSQL Connected: db={self.database}")
        except Exception as exc:
            print("PostgreSQL Connection failed:", exc)

    def fetch_data(self, query):
        try:
            cursor = self.conn.cursor()
            cursor.execute(query)

            columns = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()

            return [dict(zip(columns, row)) for row in rows]
        except Exception as exc:
            print("PostgreSQL Fetch failed:", exc)
            return []
