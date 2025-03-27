import random
import uuid
import pandas as pd
from  .SemanticOperation import SemanticOperation
import datetime
from argparse import ArgumentError
from pymongo import MongoClient, ASCENDING, DESCENDING

class UngroupingOperation:
    def __init__(self, Collection_):
        self.collection = Collection_.collection
        self.forward_processable = True
        self.backward_processable = False

    def execute_operation(self, validFromDate:datetime, args:dict):
        if 'oldValue' not in args:
            raise ArgumentError("oldValue","Missing 'oldValue' parameter for ungrouping")       
        
        if 'newValues' not in args:
            raise ArgumentError("newValues","Missing 'newValues' parameter for ungrouping")

        if not isinstance(args['newValues'],list):
            raise ArgumentError('newValues','NewValues argument must be a list')

        if 'fieldName' not in args:
            raise ArgumentError("fieldName","Missing 'fieldName' parameter for ungrouping")

        oldValue = args['oldValue']
        newValues=args['newValues']
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

        print(f'New version number: {new_version_number}')
            

        if self.collection.operation_mode == 'preprocess':           
            
            ## Backward
            
            temporary_collection = str(uuid.uuid4())            
            #Copying all records affected by the translation to a temporary collection
            res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                            {'_min_version_number' : {'$lte' : new_version_number}},
                                                                            {'_max_version_number' : {'$gt' : new_version_number}},
                                                                            {fieldName : {'$in' : newValues}}
                                                                            
                                                                        ]
                                                            } 
                                                }, 
                                                {'$unset': '_id'},
                                                { '$out' : temporary_collection } ])

            ##part 1 of split - old registers are cut until last version before translation          
            res = self.collection.collection_processed.update_many({'$and': [
                                                                    {'_min_version_number' : {'$lte' : new_version_number}},
                                                                    {'_max_version_number' : {'$gt' : new_version_number}},                                                                    
                                                                    {fieldName : {'$in' : newValues}}
                                                        ]
                                                        },
                                                        {'$set' : {'_min_version_number' : new_version_number}}
                                                    )
            
            ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
            #have been splitted in two parts. 
            res = self.collection.db[temporary_collection].update_many({},
                                                {'$set' : {'_max_version_number' : new_version_number, fieldName:oldValue}}
                                                )

            res = self.collection.db[temporary_collection].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])          
            self.collection.db[temporary_collection].drop()    
           


        new_version = {
            "current_version": 1 if next_version == None else 0,
            "version_valid_from":validFromDate,
            "previous_version":previous_version['version_number'],
            "previous_version_valid_from": previous_version['version_valid_from'],
            "previous_operation": {
                "type": "ungrouping",
                "field":fieldName,
                "from":newValues,
                "to":oldValue                
            },
            "next_version":None,
            "next_operation":None,
            "version_number":new_version_number
        }        

        next_operation = {
            "type": "ungrouping",
            "field": fieldName,
            "from":oldValue,
            "to":newValues
        }

        res = self.collection.collection_versions.update_one({'version_number': previous_version['version_number']}, {'$set' : {'next_operation': next_operation, 'next_version':new_version_number, 'next_version_valid_from' : validFromDate, 'current_version':0}})
        
        if(res.matched_count != 1):
            print("Previous version not matched")

        if(next_version != None and 'version_valid_from' in next_version):
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']            

            res = self.collection.collection_versions.update_one({'version_number': next_version['version_number']},{'$set':{'previous_version':new_version_number}})        
            if(res.matched_count != 1):
                print("Next version not matched")

        i = self.collection.collection_versions.insert_one(new_version)        

        self.collection.update_versions()


        if self.collection.operation_mode == 'preprocess':
            ##Update value of processed versions

            if next_version != None:
                self.collection.collection_processed.update_many({'_evolution_list':previous_version['_id']}, {'$push' : {'_evolution_list':i.inserted_id}})

            self.collection.check_if_operation_affected_forward(fieldName, oldValue, new_version_number)
            self.collection.check_if_operation_affected_backward(fieldName, oldValue, new_version_number)

            for value in newValues:
                self.collection.check_if_operation_affected_forward(fieldName, value, new_version_number)
                self.collection.check_if_operation_affected_backward(fieldName, value, new_version_number)


    def check_if_affected(self, Document):
        versions_df = self.collection.versions_df
        return_obj = list()

        original_version = versions_df.loc[versions_df['version_number'] == Document['_original_version']].iloc[0]
                
        if 'previous_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[(versions_df['previous_operation.type'] == 'ungrouping')& (versions_df['previous_version_valid_from'] < original_version['version_valid_from']) & (versions_df['previous_version'] <= Document['_max_version_number']) & (versions_df['previous_version'] >= Document['_min_version_number']) ] #Operacao precisa partir de versao igual ou inferior a atual            
            versions_df_p = versions_df_p.explode('previous_operation.from')

            if len(versions_df_p) > 0:
                if {'previous_operation.type','previous_operation.field', 'previous_operation.from'}.issubset(versions_df.columns):  
                    versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['previous_operation.field'],None), axis=1)
                    versions_df_p = versions_df_p.loc[ versions_df_p['field_value'] == versions_df_p['previous_operation.from']]
                    versions_df_p.sort_values('version_number', inplace=True)

                    if len(versions_df_p) > 0:                        
                        return_obj.append((float(versions_df_p.iloc[0]['version_number']),float(versions_df_p.iloc[0]['previous_version']),'backward'))


        ## Ungrouping cannot be applied forward
        return list(return_obj)

    def evolute_backward(self, Document, operation):
        if Document[operation['previous_operation.field'].values[0]] == operation['previous_operation.from'].values[0]:
            Document = Document.copy()
            Document[operation['previous_operation.field'].values[0]] = operation['previous_operation.to'].values[0]
            return Document                
        else:
            raise BaseException('Record should not be evoluted')


    def check_if_many_affected(self, DocumentsDataFrame):
        versions_df = self.collection.versions_df        
        return_obj = list()

        ##Ungrouping can only be applied backwards

        if 'previous_operation.type' in versions_df:
            versions_df_p = self.collection.versions_df.loc[versions_df['previous_operation.type'] == 'ungrouping']
            versions_df_p = versions_df_p.explode('previous_operation.from')

            if len(versions_df_p) > 0:
                grouped_df = versions_df_p.groupby(by='previous_operation.field')

                for field, group in grouped_df:                    
                    versions_g = versions_df_p.loc[versions_df_p['previous_operation.field'] == field]
                    merged_records = pd.merge(DocumentsDataFrame, versions_g, how='left', left_on=field, right_on='previous_operation.from')

                    merged_records['match'] = (merged_records['previous_operation.field'].notna()) & (merged_records['previous_version_valid_from'] < merged_records['_valid_from']) & (merged_records['previous_version'] <= merged_records['_max_version_number']) & (merged_records['previous_version'] >= merged_records['_min_version_number'])
                    matched = merged_records.loc[merged_records['match']]                    
                    
                    return_obj.append((field, matched, 'backward'))      

        return return_obj      

    def evolute_many_backward(self, field, DocumentOperationDataFrame):        
        d = DocumentOperationDataFrame.copy()
        d[field] = d['previous_operation.to']
        return d    