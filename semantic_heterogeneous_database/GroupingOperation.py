from distutils import version
import random
import uuid
import pandas as pd
from .SemanticOperation import SemanticOperation
import datetime
from argparse import ArgumentError
from pymongo import MongoClient, ASCENDING, DESCENDING

class GroupingOperation:
    def __init__(self, Collection_):
        self.collection = Collection_.collection
        self.forward_processable = False
        self.backward_processable = True

    def execute_operation(self, validFromDate:datetime, args:dict):
        if 'oldValues' not in args:
            raise ArgumentError("oldValues","Missing 'oldValues' parameter for grouping")
        
        if not isinstance(args['oldValues'],list):
            raise ArgumentError('oldValues','OldValues argument must be a list')
        
        if 'newValue' not in args:
            raise ArgumentError("newValue","Missing 'newValue' parameter for grouping")

        if 'fieldName' not in args:
            raise ArgumentError("fieldName","Missing 'fieldName' parameter for grouping")

        oldValues = args['oldValues']
        newValue=args['newValue']
        fieldName = args['fieldName']

        previous_version = self.collection.collection_versions.find({'version_valid_from' : {'$lt' : validFromDate}}).sort([('version_valid_from',-1),('version_number',-1)])        
        previous_version = next(previous_version, None)

        next_version_count = self.collection.collection_versions.count_documents({'version_valid_from' : {'$gte' : validFromDate}})
        

        #If there are no versions starting from dates greater than the refDate, the new version number is just one increment after the last version before the refDate
        #In the other hand, if there are, this new version must be registered with a number before the previous version and the next version
        
        if(next_version_count > 0):
            next_versions = self.collection.collection_versions.find({'version_valid_from' : {'$gte' : validFromDate}}).sort([('version_valid_from', 1), ('version_number', 1)])
            next_version = next(next_versions, None)
            if next_version and next_version['version_valid_from'] == validFromDate:
                same_date_versions = [next_version] + [v for v in next_versions if v['version_valid_from'] == validFromDate]
                if len(same_date_versions) > 1:
                    next_version = random.choice(same_date_versions)
                    previous_version = self.collection.collection_versions.find({'version_number': next_version['previous_version']}).sort([('version_valid_from', -1), ('version_number', -1)])
                    previous_version = next(previous_version, None)
            
            new_version_number = random.uniform(previous_version['version_number'], next_version['version_number'])
        else:
            next_version = {'version_number': float('inf')}
            self.collection.current_version = self.collection.current_version + 1000000 #this is the newest version now
            new_version_number = self.collection.current_version        
            
        
        if self.collection.operation_mode == 'preprocess': 

            ##Grouping do not work forward    

            ## Forward
            temporary_collection = str(uuid.uuid4())
            #Copying all records affected by the translation to a temporary collection
            res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                            {'_min_version_number' : {'$lte' : new_version_number}},
                                                                            {'_max_version_number' : {'$gt' : new_version_number}},
                                                                            {fieldName : {'$in':oldValues}} 
                                                                        ]
                                                            } 
                                                }, 
                                                {'$unset': '_id'},
                                                { '$out' : temporary_collection } ])

            ##part 1 of split - old registers are cut until last version before translation          
            res = self.collection.collection_processed.update_many({'$and': [
                                                                    {'_min_version_number' : {'$lte' : new_version_number}},
                                                                    {'_max_version_number' : {'$gt' : new_version_number}},                                                                    
                                                                    {fieldName : {'$in':oldValues}}                                                   
                                                        ]
                                                        },
                                                        {'$set' : {'_max_version_number' : new_version_number}}
                                                    )
            
            ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
            #have been splitted in two parts. 
            res = self.collection.db[temporary_collection].update_many({},
                                                {'$set' : {'_min_version_number' : new_version_number, fieldName:newValue}}
                                                )

            res = self.collection.db[temporary_collection].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])          
            self.collection.db[temporary_collection].drop()                               
    
                
            


        new_version = {
            "current_version": 1 if next_version == None else 0,
            "version_valid_from":validFromDate,
            "previous_version":previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": "grouping",
                "field":fieldName,
                "from":newValue,
                "to":oldValues                
            },
            "next_version":None,
            "next_operation":None,
            "version_number":new_version_number
        }        

        next_operation = {
            "type": "grouping",
            "field": fieldName,
            "from":oldValues,
            "to":newValue
        }

        res = self.collection.collection_versions.update_one({'version_number': previous_version['version_number']}, {'$set' : {'next_operation': next_operation, 'next_version':new_version_number, 'next_version_valid_from' : validFromDate, 'current_version':0}})        
        

        if(next_version != None and 'version_valid_from' in next_version):
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']            

            res = self.collection.collection_versions.update_one({'version_number': next_version['version_number']},{'$set':{'previous_version':new_version_number}})                    

        i = self.collection.collection_versions.insert_one(new_version)

        

        self.collection.update_versions()
        
        ##Update value of processed versions
        
        if self.collection.operation_mode == 'preprocess':

            if next_version != None:
                self.collection.collection_processed.update_many({'_evolution_list':previous_version['_id']}, {'$push' : {'_evolution_list':i.inserted_id}})

            self.collection.check_if_operation_affected_forward(fieldName, newValue, new_version_number)
            self.collection.check_if_operation_affected_backward(fieldName, newValue, new_version_number)

            for value in oldValues:
                self.collection.check_if_operation_affected_forward(fieldName, value, new_version_number)
                self.collection.check_if_operation_affected_backward(fieldName, value, new_version_number)

    def check_if_affected(self, Document):
        versions_df = self.collection.versions_df
        return_obj = set()
        
        
        original_version = versions_df.loc[versions_df['version_number'] == Document['_original_version']].iloc[0]

        if 'next_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[(versions_df['next_operation.type'] == 'grouping') & (versions_df['next_version_valid_from'] > original_version['version_valid_from']) & (versions_df['next_version'] <= Document['_max_version_number']) & (versions_df['next_version'] >= Document['_min_version_number']) ]

            if len(versions_df_p) > 0:
                if {'next_operation.type','next_operation.field', 'next_operation.from'}.issubset(versions_df.columns):  
                    versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['next_operation.field'], None), axis=1)
                    versions_df_p = versions_df_p.loc[versions_df_p.apply(
                        lambda row: row['field_value'] in row['next_operation.from'] 
                        if isinstance(row['next_operation.from'], list) 
                        else row['field_value'] == row['next_operation.from'], 
                        axis=1
                    )]
                    
                    if len(versions_df_p) > 0:
                        for ind,v in versions_df_p.iterrows():
                            return_obj.add((float(v['version_number']),float(v['next_version']),'forward'))
        
        # Agrupamento nao pode andar backwards

        return list(return_obj)

    ## Function is executed when is already known the document suffered changes
    def evolute_forward(self, Document, operation):        
        if Document[operation['next_operation.field'].values[0]] in operation['next_operation.from'].values[0]:
            Document = Document.copy()
            Document[operation['next_operation.field'].values[0]] = operation['next_operation.to'].values[0]            
            return Document
        else:
            raise BaseException('Record should not be evoluted')
        

    def evolute_backward(self, Document, operation):        
        pass

    def check_if_many_affected(self, DocumentsDataFrame):
        versions_df = self.collection.versions_df        
        return_obj = list()

        ## Grouping operation can only be applied forward.

        if 'next_operation.type' in versions_df:
            versions_df_p = self.collection.versions_df.loc[versions_df['next_operation.type'] == 'grouping']
            versions_df_p = versions_df_p.explode('next_operation.from') #Abrindo listas dos grupos em linhas diferentes

            if len(versions_df_p) > 0:
                grouped_df = versions_df_p.groupby(by='next_operation.field')

                for field, group in grouped_df:                    
                    versions_g = versions_df_p.loc[versions_df_p['next_operation.field'] == field]
                    merged_records = pd.merge(DocumentsDataFrame, versions_g, how='left', left_on=field, right_on='next_operation.from')

                    merged_records['match'] = (merged_records['next_operation.field'].notna()) & (merged_records['next_version_valid_from'] > merged_records['_valid_from']) & (merged_records['next_version'] < merged_records['_max_version_number']) & (merged_records['next_version'] >= merged_records['_min_version_number'])
                    matched = merged_records.loc[merged_records['match']]                    
                    
                    return_obj.append((field, matched, 'forward'))           

        return return_obj

    def evolute_many_forward(self, field, DocumentOperationDataFrame):
        d = DocumentOperationDataFrame.copy()
        d[field] = d['next_operation.to']
        return d   
    
    def reapply_operation_forward(self, version_change):        
            
        # A previous evolution has been hit by this new evolution. We need to reprocess it.

        temp_collection_name = str(uuid.uuid4())

        #Copying all records affected by the translation to a temporary collection
        res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                        {'_min_version_number' : {'$lte' : version_change['next_version']}},
                                                                        {'_max_version_number' : {'$gt' : version_change['next_version']}},
                                                                        {'$or' : [{version_change['next_operation']['field'] : {'$in' : version_change['next_operation']['from']}}
                                                                                ]
                                                                        } 
                                                                    ]
                                                        } 
                                            }, 
                                            {'$unset': '_id'},
                                            { '$out' : temp_collection_name } ])

        ##part 1 of split - old registers are cut until last version before translation          
        res = self.collection.collection_processed.update_many({'$and': [
                                                                {'_min_version_number' : {'$lte' : version_change['next_version']}},
                                                                {'_max_version_number' : {'$gt' : version_change['next_version']}},                                                                    
                                                                {'$or' : [{version_change['next_operation']['field'] : {'$in' : version_change['next_operation']['from']}}
                                                                        ]
                                                                } 
                                                    ]
                                                    },
                                                    {'$set' : {'_max_version_number' : version_change['next_version']}}
                                                )
        
        ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
        #have been splitted in two parts. 
        res = self.collection.db[temp_collection_name].update_many({},
                                            {'$set' : {'_min_version_number' : version_change['next_version'],version_change['next_operation']['field'] : version_change['next_operation']['to']}}
                                            )

        res = self.collection.db[temp_collection_name].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])          
        self.collection.db[temp_collection_name].drop()

        ##Recheck
        self.collection.check_if_operation_affected_forward(version_change['next_operation']['field'], version_change['next_operation']['to'],version_change['next_version'])#Recheck if affected any other evolution