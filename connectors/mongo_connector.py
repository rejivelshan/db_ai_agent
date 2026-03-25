from pymongo import MongoClient


class MongoConnector:
    def __init__(self):
        self.client = None
        self.db = None

    def connect(self):
        try:
            self.client = MongoClient("mongodb://localhost:27017")
            self.db = self.client["testdb"]
            print("✅ MongoDB Connected")
        except Exception as e:
            print("❌ Mongo Connection failed:", e)

    def fetch_data(self, collection_name):
        collection = self.db[collection_name]
        return list(collection.find({}, {"_id": 0}))