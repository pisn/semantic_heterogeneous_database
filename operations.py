from bson.objectid import ObjectId
from pymongo import MongoClient
import json

class InterscityCollection:
    def __init__(self, databaseName, collectionName) -> None:
        self.client = MongoClient(host='localhost')
        self.db = self.client[databaseName]
        self.collection = self.db[collectionName]
        self.collection_columns = self.db[collectionName+'_columns']
        self.collection_versions = self.db[collectionName + '_versions']
        self.current_version = self.collection_versions.find_one({"current_version":1})
        
        #initializing first version
        if(self.current_version == None):
            self.current_version = 0
           
            first_version = {
                "current_version":1,
                "previous_version":None, 
                "previous_operation":None,
                "version_number":0, 
                "next_version":None,
                "next_operation":None
            }
            self.collection_versions.insert_one(first_version)                       
        else:
            self.current_version = self.current_version['version_number']

    def insert_one(self, jsonString):
        o = json.loads(jsonString)        

        insertedDocument = self.collection.insert_one({("version_" + str(self.current_version)): o, 'first_version': self.current_version, 'last_version': self.current_version, 'original_version' : self.current_version})        

        for field in o:
            if not field.startswith('_'):
                updateResult = self.collection_columns.update_one({'name': field}, {'$set' : {'name' : field}, '$push' : {'documents' : insertedDocument.inserted_id}}, upsert=True)
                
                if(updateResult.upserted_id != None):
                    self.collection_columns.update_one({'_id':updateResult.upserted_id},{'$set' : {'first_version' : self.current_version ,'last_version': self.current_version}})

    def execute_translation(self, fieldName, oldValue, newValue, eagerlyTranslate):        
        #bater nas colunas e gerar novas versoes se eagerly
        #salvar traducao em algum lugar para ser consultada de qualquer maneira
        new_version = {
            "current_version":1,
            "previous_version":self.current_version,
            "previous_operation": {
                "type": "translation",
                "field":fieldName,
                "from":newValue,
                "to":oldValue
            },
            "next_version":None,
            "next_operation":None,
            "version_number":self.current_version + 1
        }

        next_operation = {
            "type": "translation",
            "field": fieldName,
            "from":oldValue,
            "to":newValue
        }
        
        res = self.collection_versions.update_one({'current_version': 1}, {'$set' : {'current_version' : 0, 'next_operation': next_operation}})
        
        if(res.matched_count != 1):
            print("Current version not matched")

        self.collection_versions.insert_one(new_version)        
        self.current_version = self.current_version + 1

        self.collection_columns.update_one({'name':fieldName}, {'$set' : {'last_version' : self.current_version}})

        if(eagerlyTranslate):
            for document in self.collection.find():                
                self.evolute(document, self.current_version)

                
    def evolute(self, document, targetVersion):
        lastVersion = int(document['last_version'])
        firstVersion = int(document['first_version'])

        if targetVersion > lastVersion:
            while lastVersion < targetVersion:
                lastVersionDocument = document['version_' + str(lastVersion)]

                versionRegister = self.collection_versions.find_one({'version_number' : lastVersion})

                if(versionRegister == None):
                    raise 'version register not found'

                evolutionOperation = versionRegister['next_operation']

                if(evolutionOperation['type'] == 'translation'):
                    field = evolutionOperation['field']
                    oldValue = evolutionOperation['from']
                    newValue = evolutionOperation['to']

                    if(field in lastVersionDocument):
                        if lastVersionDocument[field] == oldValue:
                            lastVersionDocument[field] = newValue
                else:
                    raise 'Unrecognized evolution type:' + evolutionOperation['type']        
                
                lastVersion = lastVersion + 1
                self.collection.update_one({'_id' : document['_id']}, {'$set': {'version_' + str(lastVersion) : lastVersionDocument, 'last_version':lastVersion}})                
        else:
            while firstVersion > targetVersion:
                firstVersionDocument = document['version_' + str(firstVersion)]

                versionRegister = self.collection_versions.find_one({'version_number' : firstVersion})

                if(versionRegister == None):
                    raise 'version register not found'

                evolutionOperation = versionRegister['previous_operation']

                if(evolutionOperation['type'] == 'translation'):
                    field = evolutionOperation['field']
                    oldValue = evolutionOperation['to']
                    newValue = evolutionOperation['from']

                    if(field in firstVersionDocument):
                        if firstVersionDocument[field] == oldValue:
                            firstVersionDocument[field] = newValue
                else:
                    raise 'Unrecognized evolution type:' + evolutionOperation['type']        
                
                firstVersion = firstVersion - 1
                self.collection.update_one({'_id' : document['_id']}, {'$set': {'version_' + str(firstVersion) : firstVersionDocument, 'first_version':firstVersion}})                
            
        

myCollection = InterscityCollection('interscity', 'collectionTest')
myCollection.insert_one('{"coluna1": "seila"}')
myCollection.insert_one('{"coluna2": "seila"}')
myCollection.insert_one('{"coluna1": "seila2"}')
myCollection.execute_translation("coluna1","seila","seila3", True)        