from pymongo import MongoClient
import json

class InterscityCollection:
    def __init__(self, databaseName, collectionName) -> None:
        self.client = MongoClient(host='localhost')
        self.db = self.client[databaseName]
        self.collection = self.db[collectionName]
        self.collection_columns = self.db[collectionName+'_columns']

    def insert_one(self, jsonString):
        o = json.loads(jsonString)

        insertedDocument = self.collection.insert_one(o)

        for field in o:
            if not field.startswith('_'):
                self.collection_columns.update_one({'name': field}, {'$set' : {'name' : field}, '$push' : {'documents' : insertedDocument.inserted_id}}, upsert=True)


myCollection = InterscityCollection('interscity', 'collectionTest')
myCollection.insert_one('{"coluna1": "seila"}')
        