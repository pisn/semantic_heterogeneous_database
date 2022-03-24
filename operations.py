from argparse import ArgumentError
from concurrent.futures import process
from platform import version
from bson.objectid import ObjectId
from pymongo import MongoClient, ASCENDING,DESCENDING
from datetime import datetime
import json
import csv
import pandas as pd

class InterscityCollection:
    def __init__(self, databaseName, collectionName) -> None:
        self.client = MongoClient(host='localhost')
        self.database_name = databaseName
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
                "version_valid_from":datetime(1,1,1),
                "previous_version":None, 
                "previous_operation":None,
                "version_number":0, 
                "next_version":None,
                "next_operation":None
            }
            self.collection_versions.insert_one(first_version)                       
        else:
            self.current_version = self.current_version['version_number']

    def insert_one(self, jsonString, valid_from_date:datetime):
        versions = self.collection_versions.find({'version_valid_from':{'$lte' : valid_from_date}}).sort('version_valid_from',DESCENDING)
        version = next(versions, None)
        
        self.insert_one_by_version(jsonString, version['version_number'], valid_from_date)

    def insert_one_by_version(self, jsonString, versionNumber, valid_from_date:datetime):
        o = json.loads(jsonString)                
        o['_original_version']=versionNumber           
        o['_first_processed_version'] = versionNumber
        o['_last_processed_version'] = versionNumber
        o['_valid_from'] = valid_from_date        

        insertedDocument = self.collection.insert_one(o)        

        p = json.loads(jsonString)
        p['_min_version_number'] = versionNumber
        p['_max_version_number'] = versionNumber
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = versionNumber
        p['_valid_from'] = valid_from_date
        p['_evoluted'] = False

        self.collection_processed.insert_one(p)

        for field in o:
            if not field.startswith('_'):
                updateResult = self.collection_columns.update_one({'field_name': field}, {'$set' : {'field_name' : field}, '$push' : {'documents' : insertedDocument.inserted_id}}, upsert=True)
                
                if(updateResult.upserted_id != None):
                    self.collection_columns.update_one({'_id':updateResult.upserted_id},{'$set' : {'first_edit_version' : versionNumber ,'last_edit_version': versionNumber}})   
    

    def execute_translation(self, fieldName, oldValue, newValue, refDate : datetime): 
        if not isinstance(refDate,datetime):
            raise ArgumentError('RefDate argument is not a datetime.')


        #Determining new version number based on versions registers

        previous_version = self.collection_versions.find({'version_valid_from' : {'$lt' : refDate}}).sort('version_valid_from',-1)        
        previous_version = next(previous_version, None)

        next_version = self.collection_versions.find({'version_valid_from' : {'$gte' : refDate}}).sort('version_valid_from')        

        #If there are no versions starting from dates greater than the refDate, the new version number is just one increment after the last version before the refDate
        #In the other hand, if there are, this new version must be registered with a number before the previous version and the next version
        
        if(next_version.count() > 0):
            next_version = next(next_version,None)
            new_version_number = previous_version['version_number'] + (next_version['version_number'] - previous_version['version_number'])/2
        else:
            next_version = None
            self.current_version = self.current_version + 1 #this is the newest version now
            new_version_number = self.current_version
            
            
        ##For processed records unaffected by the translation, ending in the previous version, version interval should be extended to include the new version,        

        res = self.collection_processed.update_many({'$and' : [{'_max_version_number' : previous_version['version_number']},
                                                               {fieldName : {'$ne' : oldValue}}
                                                              ]}, {'$set' : {'_max_version_number' : new_version_number}})
        
        
        ##Spliting processed registers affected by the translation where the new version is within the min and max version number
        if(next_version != None):
            res = self.collection_processed.update_many({'$and' : [{'_min_version_number' : next_version['version_number']},
                                                                   {'$or' : [{fieldName : oldValue},
                                                                              {fieldName : newValue}
                                                                             ]
                                                                   }
                                                              ]}, {'$set' :{'_min_version_number' : new_version_number}})                                                              
      
            
            #Copying all records in this situation
            res = self.collection_processed.aggregate([{ '$match': {'$and': [
                                                                            {'_min_version_number' : {'$lte' : previous_version['version_number']}},
                                                                            {'_max_version_number' : {'$gte' : next_version['version_number']}},
                                                                            {'$or' : [{fieldName : oldValue},
                                                                                    {fieldName : newValue}
                                                                                    ]
                                                                            } 
                                                                        ]
                                                              } 
                                                  }, 
                                                  {'$set' : {'_id' : ObjectId()}},
                                                  { '$out' : "to_split" } ])

            ##part 1 of split - old registers is cut until last version before translation          
            res = self.collection_processed.update_many({'$and': [
                                                                    {'_min_version_number' : {'$lte' : previous_version['version_number']}},
                                                                    {'_max_version_number' : {'$gte' : next_version['version_number']}},
                                                                    {'$or' : [{fieldName : oldValue},
                                                                              {fieldName : newValue}
                                                                             ]
                                                                    } 
                                                          ]
                                                        },
                                                        {'$set' : {'_max_version_number' : previous_version['version_number']}}
                                                       )
            
            ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
            #have been splitted in two parts. 
            res = self.db['to_split'].update_many({},
                                                  {'$set' : {'_min_version_number' : new_version_number}}
                                                 )

            res = self.db['to_split'].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection_processed.name, 'whenMatched' : 'fail'}}])          
            self.db['to_split'].drop()
        
        else: #New version is terminal, no need to split, but need to create another node
            #copying records
            res = self.collection_processed.aggregate([{ '$match': {'$and': [
                                                                            {'_max_version_number' : previous_version['version_number']},                                                                            
                                                                            {fieldName : oldValue}                                                                                                                                                                                                                                                   
                                                                            ]
                                                                   } 
                                                        },
                                                        {'$set' : {'_id' : ObjectId()}},
                                                      { '$out' : "to_split" } ])
            #updating version 
            res = self.db['to_split'].update_many({},
                                                  {'$set' : {'_min_version_number' : new_version_number, '_max_version_number' : new_version_number}}
                                                 )

            res = self.db['to_split'].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection_processed.name, 'whenMatched' : 'fail'}}])          
            self.db['to_split'].drop()


        new_version = {
            "current_version": 1 if next_version == None else 0,
            "version_valid_from":refDate,
            "previous_version":previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": "translation",
                "field":fieldName,
                "from":newValue,
                "to":oldValue                
            },
            "next_version":None,
            "next_operation":None,
            "version_number":new_version_number
        }        

        next_operation = {
            "type": "translation",
            "field": fieldName,
            "from":oldValue,
            "to":newValue
        }

        res = self.collection_versions.update_one({'version_number': previous_version['version_number']}, {'$set' : {'next_operation': next_operation, 'next_version':new_version_number, 'next_version_valid_from' : refDate}})
        
        if(res.matched_count != 1):
            print("Previous version not matched")

        if(next_version != None):
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']            

            res = self.collection_versions.update_one({'version_number': next_version['version_number']},{'$set':{'previous_version':new_version_number}})        
            if(res.matched_count != 1):
                print("Next version not matched")

        column = self.collection_columns.find_one({'field_name':fieldName}) 
        if column['last_edit_version'] < new_version_number:
            self.collection_columns.update_one({'field_name':fieldName}, {'$set' : {'last_edit_version' : new_version_number}})
        elif column['first_edit_version'] < new_version_number:
            self.collection_columns.update_one({'field_name':fieldName}, {'$set' : {'first_edit_version' : new_version_number}})        

        self.collection_versions.insert_one(new_version)    


        ##Update value of processed versions

        res = self.collection_versions.find({'$and': [{'next_operation.field' : fieldName},
                                                      {'next_operation.type' : 'translation'}                                                      
                                                     ]}).sort('next_version_valid_from',ASCENDING)

        for version_change in res:
            res = self.collection_processed.update_many({'$and':[{'_min_version_number':{'$gte' : version_change['next_version']}},
                                                                 {'_valid_from' : {'$lte': version_change['next_version_valid_from']}},
                                                                 {version_change['next_operation']['field'] : version_change['next_operation']['from']},                                                                 
                                                                ]
                                                        }, 
                                                        {'$set': {version_change['next_operation']['field']: version_change['next_operation']['to'], '_evoluted' : True}})   
        

        ##Pre-existing records have already been processed in the new version. We can update this in the original records collection. 

        self.collection.update_many({'$and': [{'_last_processed_version': previous_version['version_number']}
                                             ]                                     
                                    },
                                    {'$set' :{'_last_processed_version' : new_version_number}})

        if(next_version != None):
            self.collection.update_many({'$and': [{'_first_processed_version': next_version['version_number']}
                                                ]                                     
                                        },
                                        {'$set':{'_first_processed_version' : new_version_number}})
                

                
    def evolute(self, rawDocument, targetVersion):
        lastVersion = float(rawDocument['_last_processed_version'])
        firstVersion = float(rawDocument['_first_processed_version'])        

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
                            lastVersionDocument['_evoluted'] = True
                else:
                    raise 'Unrecognized evolution type:' + evolutionOperation['type']        
                
                if(versionRegister['next_version'] != None):
                    lastVersion = float(versionRegister['next_version'])
                else: #Chegou a ultima versao disponivel, aumentar um ponto
                    lastVersion = self.current_version
                
                lastVersionDocument['_version_number'] = lastVersion                
                rawDocument['_last_processed_version'] = lastVersion

                self.collection_processed.insert_one(lastVersionDocument)
        else:
            while firstVersion > targetVersion:
                firstVersionDocument = self.collection_processed.find_one({'_original_id' : rawDocument['_id'], '_version_number': firstVersion})                
                firstVersionDocument.pop('_id')

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
                            firstVersionDocument['_evoluted'] = True
                else:
                    raise 'Unrecognized evolution type:' + evolutionOperation['type']        
                
                firstVersion = float(versionRegister['previous_version'])
                firstVersionDocument['_version_number'] = firstVersion                
                rawDocument['_first_processed_version'] = firstVersion

                self.collection_processed.insert_one(firstVersionDocument)

        self.collection.update_one({'_id':rawDocument['_id']}, {'$set': {'_first_processed_version': rawDocument['_first_processed_version'], '_last_processed_version': rawDocument['_last_processed_version']}})
    
    def query_specific(self, query, version_number = None):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(version_number == None):
            version_number = self.current_version   

        max_version_number = version_number
        min_version_number = version_number
        
        ##obtaining version to be queried
        
        to_process = []
        to_process.append(query)  #Fazer isso aqui funcionar                   
        
        while len(to_process) > 0:
            field = to_process.pop()

            if isinstance(field, dict): 
                if(len(field.keys()) > 1):
                    for f in field.keys():
                        to_process.append({field: field[f]})
                    continue           
                else:
                    key = list(field.keys())[0]
                    value = field[key]
                    
                    if not isinstance(value, str):
                        to_process.append(value)
                    
                    field = key #keep process going for this iteration
            
            elif isinstance(field, list):
                to_process.extend(field)
                continue

            if field[0] == '$': #pymongo operators like $and, $or, etc
                # if isinstance(query[field],list):
                #     to_process.extend(query[field])
                continue                   

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
            print('Translating up')
            self.evolute(record, version_number)
        
        for record in to_translate_down:
            print('Translating down')
            self.evolute(record, version_number)

        query['_min_version_number'] = {'$lte' : version_number} ##Retornando registros traduzidos. 
        query['_max_version_number'] = {'$gte' : version_number} ##Retornando registros traduzidos. 
        return self.collection_processed.find(query)

    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def query(self, query):

        queryTerms = {}        

        
        for field in query.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(query[field])

            to_process = []
            to_process.append(query[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                versions = self.collection_versions.find({'next_operation.field':field,'next_operation.from':fieldValue})

                if(versions.count() > 0):
                    for version in versions:
                        new_term = version['next_operation']['to']
                        to_process.append(new_term)
                else:
                    queryTerms[field].add(fieldValue) #besides from the original query, this value could also represent a record that were translated in the past from the original query term. Therefore, it must be considered in the query

                
        
        ands = []

        for field in queryTerms.keys():
            ors = []

            for value in queryTerms[field]:
                ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}                  
        
        return self.query_specific(finalQuery)        

    def pretty_print(self, records):      

        fieldLengths = {}
             
        records = list(records)
        
        for record in records:            

            if record['_evoluted'] == True:
                originalRecord = self.collection.find_one({'_id' : record['_original_id']})

            for field in record.keys():               

                if not field.startswith('_'):                    
                    if(field not in fieldLengths):
                        fieldLengths[field] = len(field) #Header is the first set length

                    if record['_evoluted'] == True:
                        processedValue = record[field]
                        originalValue = originalRecord[field]

                        if processedValue != originalValue:
                            record[field] = f'{processedValue} (originaly: {originalValue})'

                    if(fieldLengths[field] < len(record[field])):
                        fieldLengths[field] = len(record[field])

        lineLength = 0
        for field in fieldLengths.keys():
            p = '|' + field + ' '*(fieldLengths[field] - len(field))
            print(p, end='')
            lineLength = lineLength + len(p)
        
        print('|')                   
        print ('-'*(lineLength+1))
        
        for record in records:            
            for field in record.keys():  #calculate number of spaces so as fields have the same length in all records
                if not field.startswith('_'):
                    print('|' + record[field] + ' '*(fieldLengths[field] - len(record[field])), end='')
            print('|')
        
    def drop_database(self):
        self.client.drop_database(self.database_name)

    def insert_many_by_csv(self, filePath, valid_from_field, valid_from_date_format='%Y-%m-%d', delimiter=','):
        df = pd.read_csv(filePath, delimiter=delimiter)

        chunks = df.groupby([valid_from_field])

        for version_valid_from, group in chunks:
            version_valid_from = datetime.strptime(version_valid_from, valid_from_date_format)
            versions = self.collection_versions.find({'version_valid_from':{'$lte' : version_valid_from}}).sort('version_valid_from',DESCENDING)
            version = next(versions, None)

            self.insert_many_by_version(group.drop(valid_from_field, 1), version['version_number'], version_valid_from)        

    
    def insert_many_by_version(self, group: pd.DataFrame, version_number:int, valid_from_date: datetime):       
        processed_group = group.copy()
       
        group['_first_processed_version'] = version_number
        group['_last_processed_version']=version_number
        group['_original_version']=version_number           
        group['_valid_from'] = valid_from_date       

        insertedDocuments = self.collection.insert_many(group.to_dict('records'))       

        processed_group.insert(len(processed_group.columns),'_original_id', insertedDocuments.inserted_ids) 

        processed_group['_min_version_number'] = version_number        
        processed_group['_max_version_number'] = version_number        
        processed_group['_original_version'] = version_number
        processed_group['_valid_from'] = valid_from_date
        processed_group['_evoluted'] = False                     
        
        self.collection_processed.insert_many(processed_group.to_dict('records'))

        for field in group.columns:
            if not field.startswith('_'):
                column_register = self.collection_columns.find_one({'field_name': field})

                if(column_register == None):
                    self.collection_columns.insert_one({'field_name':field, 'first_edit_version' : version_number ,'last_edit_version': version_number})           

    def execute_translations_by_csv(self, filePath):
        with open(filePath, 'r') as csvFile:
            reader = csv.DictReader(csvFile)

            for row in reader:
                self.execute_translation(row['field'], row['from'], row['to'], datetime.strptime(row['RefDate'], '%Y-%m-%d'))

                    

