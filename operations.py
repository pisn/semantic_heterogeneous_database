from platform import version
from bson.objectid import ObjectId
from pymongo import MongoClient
import json

class InterscityCollection:
    def __init__(self, databaseName, collectionName) -> None:
        self.client = MongoClient(host='localhost')
        self.db = self.client[databaseName]
        self.collection = self.db[collectionName]
        self.collection_processed = self.db[collectionName+'_processed']
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
        o['_first_processed_version'] = self.current_version
        o['_last_processed_version']=self.current_version
        o['_original_version']=self.current_version       

        insertedDocument = self.collection.insert_one(o)        

        p = json.loads(jsonString)
        p['_version_number'] = self.current_version
        p['_original_id'] = insertedDocument.inserted_id

        self.collection_processed.insert_one(p)

        for field in o:
            if not field.startswith('_'):
                updateResult = self.collection_columns.update_one({'field_name': field}, {'$set' : {'field_name' : field}, '$push' : {'documents' : insertedDocument.inserted_id}}, upsert=True)
                
                if(updateResult.upserted_id != None):
                    self.collection_columns.update_one({'_id':updateResult.upserted_id},{'$set' : {'first_edit_version' : self.current_version ,'last_edit_version': self.current_version}})

    def execute_translation(self, fieldName, oldValue, newValue, eagerlyTranslate):                
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
        
        res = self.collection_versions.update_one({'current_version': 1}, {'$set' : {'current_version' : 0, 'next_operation': next_operation, 'next_version':self.current_version + 1}})
        
        if(res.matched_count != 1):
            print("Current version not matched")

        self.collection_versions.insert_one(new_version)        
        self.current_version = self.current_version + 1

        self.collection_columns.update_one({'field_name':fieldName}, {'$set' : {'last_edit_version' : self.current_version}})

        if(eagerlyTranslate):
            for document in self.collection.find():                
                self.evolute(document, self.current_version)

                
    def evolute(self, rawDocument, targetVersion):
        lastVersion = int(rawDocument['_last_processed_version'])
        firstVersion = int(rawDocument['_first_processed_version'])        

        if targetVersion > lastVersion:
            while lastVersion < targetVersion:                
                
                lastVersionDocument = self.collection_processed.find_one({'_original_id' : rawDocument['_id'], '_version_number': lastVersion})                
                lastVersionDocument.pop('_id')

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
                
                if(versionRegister['next_version'] != None):
                    lastVersion = int(versionRegister['next_version'])
                else: #Chegou a ultima versao disponivel, aumentar um ponto
                    lastVersion = self.current_version
                
                lastVersionDocument['_version_number'] = lastVersion
                rawDocument['_last_processed_version'] = lastVersion

                self.collection_processed.insert_one(lastVersionDocument)
        else:
            while firstVersion > targetVersion:
                firstVersionDocument = self.collection_processed.find_one({'_original_id' : rawDocument['_id'], '_version_number': firstVersion})                

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
                
                firstVersion = int(versionRegister['previous_version'])
                firstVersionDocument['_version_number'] = firstVersion
                rawDocument['_first_processed_version'] = firstVersion

                self.collection_processed.insert_one(firstVersionDocument)

        self.collection.update_one({'_id':rawDocument['_id']}, {'$set': {'_first_processed_version': rawDocument['_first_processed_version'], '_last_processed_version': rawDocument['_last_processed_version']}})
    
    def query(self, query, version_number = None):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(version_number == None):
            version_number = self.current_version   

        max_version_number = version_number
        min_version_number = version_number
        
        ##obtaining version to be queried
        for field in query.keys():
            fieldRegister = self.collection_columns.find_one({'field_name':field})

            if fieldRegister == None:
                raise 'Field not found in collection: ' + field
            
            lastFieldVersion = int(fieldRegister['last_edit_version'])
            firstFieldVersion = int(fieldRegister['first_edit_version'])

            if version_number > lastFieldVersion and lastFieldVersion > max_version_number:
                max_version_number = lastFieldVersion

            elif version_number < firstFieldVersion and firstFieldVersion < min_version_number:
                min_version_number = firstFieldVersion

        ###Vou assumir por enquanto que estou sempre consultando a ultima versao, e que portanto sempre vou evoluir. Mas pensar no caso de que seja necessário um retrocesso
        version_number = max_version_number

        ###Obtaining records which have not been translated yet to the target version and translate them
        to_translate_up = self.collection.find({'_last_processed_version' : {'$lt' : version_number}})
        to_translate_down = self.collection.find({'_first_processed_version' : {'$gt' : version_number}})

        for record in to_translate_up:
            self.evolute(record, version_number)
        
        for record in to_translate_down:
            self.evolute(record, version_number)

        query['_version_number'] = version_number ##Retornando registros traduzidos. 
        return self.collection_processed.find(query)


        
          
        

myCollection = InterscityCollection('interscity', 'collectionTest')
myCollection.insert_one('{"pais": "Brasil", "cidade":"Vila Rica"}')
myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}')
myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}')
myCollection.execute_translation("cidade","Vila Rica","Ouro Preto", False)        
myCollection.insert_one('{"pais": "Brasil", "cidade":"São Paulo"}')
testeQuery = myCollection.query({'cidade' : 'Ouro Preto'})

for record in testeQuery:
    print(record['_id'])



