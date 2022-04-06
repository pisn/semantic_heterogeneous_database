import pandas as pd
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
                    self.collection_columns.update_one({'_id':updateResult.upserted_id},{'$set' : {'first_edit_version' : versionNumber ,'last_edit_version': versionNumber}})   

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

    def find_many(self, queryString):
        """ Query the collection with the supplied queryString

        Args:
            queryString(): string of the query in json. The syntax is the same as the pymongo syntax.
        
        Returns: a cursor for the records returned by the query

        """

        pass

    def pretty_print(self, recordsCursor):
        """ Pretty print the records in the console. Semantic changes will be presented in the format "CurrentValue (originally: OriginalValue)"

        Args:
            recordsCursor(): the cursor for the records, usually obtained through the find functions
        
        """
        