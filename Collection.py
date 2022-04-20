import pandas as pd
import SemanticOperation
from argparse import ArgumentError
from bson.objectid import ObjectId
from pymongo import MongoClient, ASCENDING,DESCENDING
from datetime import datetime
import json
import csv

class Collection:
    def __init__ (self,DatabaseName, CollectionName, Host='localhost'):        
        self.operations = {}

        self.client = MongoClient(Host)
        self.database_name = DatabaseName
        self.db = self.client[DatabaseName]
        self.collection = self.db[CollectionName]
        self.collection_processed = self.db[CollectionName+'_processed']
        self.collection_columns = self.db[CollectionName+'_columns']
        self.collection_versions = self.db[CollectionName + '_versions']
        self.current_version = self.collection_versions.find_one({"current_version":1})
        self.semantic_operations = {}
        
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

    def register_operation(self, OperationKey, SemanticOperationClass):
        if not SemanticOperation.implementedBy(SemanticOperationClass):
            raise ArgumentError("SemanticOperationClass", "SemanticOperationClass provided does not implement SemanticOperation interface")

        self.semantic_operations[OperationKey] = SemanticOperationClass


    def insert_one(self, JsonString, ValidFromDate:datetime):
        """ Insert one documet in the collection.

        Args:
            JsonString(): string of the json representation of the document being inserted.
            valid_from_date(): date of when the register becomes valid. This is important in the treatment of semantic changes along time
        
        """        
        versions = self.collection_versions.find({'version_valid_from':{'$lte' : ValidFromDate}}).sort('version_valid_from',DESCENDING)
        version = next(versions, None)
        
        self.__insert_one_by_version(JsonString, version['version_number'], ValidFromDate)

    def __insert_one_by_version(self, JsonString, VersionNumber, ValidFromDate:datetime):
        o = json.loads(JsonString)                
        o['_original_version']=VersionNumber           
        o['_first_processed_version'] = VersionNumber
        o['_last_processed_version'] = VersionNumber
        o['_valid_from'] = ValidFromDate        

        insertedDocument = self.collection.insert_one(o)        

        p = json.loads(JsonString)
        p['_min_version_number'] = VersionNumber
        p['_max_version_number'] = VersionNumber
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = VersionNumber
        p['_valid_from'] = ValidFromDate
        p['_evoluted'] = False

        self.collection_processed.insert_one(p)

        for field in o:
            if not field.startswith('_'):
                updateResult = self.collection_columns.update_one({'field_name': field}, {'$set' : {'field_name' : field}, '$push' : {'documents' : insertedDocument.inserted_id}}, upsert=True)
                
                if(updateResult.upserted_id != None):
                    self.collection_columns.update_one({'_id':updateResult.upserted_id},{'$set' : {'first_edit_version' : VersionNumber ,'last_edit_version': VersionNumber}})   

    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        """ Insert many recorDs in the collection using a csv file. 

        Args: 
            FilePath (): path to the csv file
            validValidFromField_from_field (): csv field which should be used to determine the date of when the register becomes valid. This is important in the treatment of semantic changes along time
            ValidFromDateFormat (): the expected format of the date contained in the valid_from_field values
            Delimiter (): delimiter used in the file. 

        """
        df = pd.read_csv(FilePath, delimiter=Delimiter)

        chunks = df.groupby([ValidFromField])

        for versionValidFrom, group in chunks:
            versionValidFrom = datetime.strptime(versionValidFrom, ValidFromDateFormat)
            versions = self.collection_versions.find({'version_valid_from':{'$lte' : versionValidFrom}}).sort('version_valid_from',DESCENDING)
            version = next(versions, None)

            self.__insert_many_by_version(group.drop(ValidFromField, 1), version['version_number'], versionValidFrom)        

    def __insert_many_by_version(self, Group: pd.DataFrame, VersionNumber:int, ValidFromDate:datetime):

        processed_group = Group.copy()
       
        Group['_first_processed_version'] = VersionNumber
        Group['_last_processed_version']=VersionNumber
        Group['_original_version']=VersionNumber           
        Group['_valid_from'] = ValidFromDate       

        insertedDocuments = self.collection.insert_many(Group.to_dict('records'))       

        processed_group.insert(len(processed_group.columns),'_original_id', insertedDocuments.inserted_ids) 

        processed_group['_min_version_number'] = VersionNumber        
        processed_group['_max_version_number'] = VersionNumber        
        processed_group['_original_version'] = VersionNumber
        processed_group['_valid_from'] = ValidFromDate
        processed_group['_evoluted'] = False                     
        
        self.collection_processed.insert_many(processed_group.to_dict('records'))

        for field in Group.columns:
            if not field.startswith('_'):
                column_register = self.collection_columns.find_one({'field_name': field})

                if(column_register == None):
                    self.collection_columns.insert_one({'field_name':field, 'first_edit_version' : VersionNumber ,'last_edit_version': VersionNumber})           

    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        """ Query the collection with the supplied queryString

        Args:
            queryString(): string of the query in json. The syntax is the same as the pymongo syntax.
        
        Returns: a cursor for the records returned by the query

        """

        queryTerms = {}        

        
        for field in QueryString.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

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
        
        return self.__query_specific(finalQuery)     

    def __query_specific(self, Query, VersionNumber=None):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(VersionNumber == None):
            VersionNumber = self.current_version   

        max_version_number = VersionNumber
        min_version_number = VersionNumber
        
        ##obtaining version to be queried
        
        to_process = []
        to_process.append(Query) 
        
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

            if VersionNumber > lastFieldVersion and lastFieldVersion > max_version_number:
                max_version_number = lastFieldVersion

            elif VersionNumber < firstFieldVersion and firstFieldVersion < min_version_number:
                min_version_number = firstFieldVersion

        ###Vou assumir por enquanto que estou sempre consultando a ultima versao, e que portanto sempre vou evoluir. Mas pensar no caso de que seja necessário um retrocesso
        VersionNumber = max_version_number

        ###Obtaining records which have not been translated yet to the target version and translate them
        to_translate_up = self.collection.find({'_last_processed_version' : {'$lt' : VersionNumber}})
        to_translate_down = self.collection.find({'_first_processed_version' : {'$gt' : VersionNumber}})

        for record in to_translate_up:
            print('Translating up')
            self.evolute(record, VersionNumber)
        
        for record in to_translate_down:
            print('Translating down')
            self.evolute(record, VersionNumber)

        query['_min_version_number'] = {'$lte' : version_number} ##Retornando registros traduzidos. 
        query['_max_version_number'] = {'$gte' : version_number} ##Retornando registros traduzidos. 
        return self.collection_processed.find(query)

    def pretty_print(self, recordsCursor):
        """ Pretty print the records in the console. Semantic changes will be presented in the format "CurrentValue (originally: OriginalValue)"

        Args:
            recordsCursor(): the cursor for the records, usually obtained through the find functions
        
        """
        