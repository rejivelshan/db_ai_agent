import os

from dotenv import load_dotenv
from pymongo import MongoClient

load_dotenv(override=True)


class MongoConnector:
    def __init__(self, mongo_uri=None, mongo_db=None):
        self.client = None
        self.db = None
        self.mongo_uri = mongo_uri or os.getenv("MONGO_URI", "mongodb://localhost:27017")
        self.mongo_db = mongo_db or os.getenv("MONGO_DB", "testdb")

    def connect(self):
        try:
            self.client = MongoClient(self.mongo_uri)
            self.db = self.client[self.mongo_db]
            print(f"MongoDB Connected: db={self.mongo_db}")
        except Exception as exc:
            print("Mongo Connection failed:", exc)

    def fetch_data(self, collection_name):
        collection = self.db[collection_name]
        return list(collection.find({}, {"_id": 0}))
