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
            next_version = self.collection.collection_versions.find({'version_valid_from' : {'$gte' : validFromDate}}).sort([('version_valid_from',1),('version_number',1)])        
            next_version = next(next_version,None)
            new_version_number = previous_version['version_number'] + (next_version['version_number'] - previous_version['version_number'])/2
        else:
            next_version = None
            self.collection.current_version = self.collection.current_version + 1 #this is the newest version now
            new_version_number = self.collection.current_version
            

        if self.collection.operation_mode == 'preprocess':
            
            ##For processed records unaffected by the grouping, ending in the previous version, version interval should be extended to include the new version,        

            res = self.collection.collection_processed.update_many({'$and' : [{'_max_version_number' : previous_version['version_number']},
                                                                {fieldName : {'$nin' : newValues}},
                                                                {fieldName : {'$ne' : oldValue}}
                                                                ]}, {'$set' : {'_max_version_number' : new_version_number}})
            
            
            ##Spliting processed registers affected by the grouping where the new version is within the min and max version number
            if(next_version != None):
                res = self.collection.collection_processed.update_many({'$and' : [{'_min_version_number' : next_version['version_number']},
                                                                    {'$or' : [{fieldName : {'$in' : newValues}},
                                                                                {fieldName : oldValue}
                                                                                ]
                                                                    }
                                                                ]}, {'$set' :{'_min_version_number' : new_version_number}})                                                              
        
                
                #Copying all records in this situation
                res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                                {'_min_version_number' : {'$lte' : previous_version['version_number']}},
                                                                                {'_max_version_number' : {'$gte' : next_version['version_number']}},
                                                                                {'$or' : [{fieldName : oldValue},
                                                                                        {fieldName : {'$in' : newValues}}
                                                                                        ]
                                                                                } 
                                                                            ]
                                                                } 
                                                    }, 
                                                    {'$unset': '_id'},
                                                    { '$out' : "to_split" } ])

                ##part 1 of split - old registers is cut until last version before translation          
                res = self.collection.collection_processed.update_many({'$and': [
                                                                        {'_min_version_number' : {'$lte' : previous_version['version_number']}},
                                                                        {'_max_version_number' : {'$gte' : next_version['version_number']}},
                                                                        {'$or' : [{fieldName : oldValue},
                                                                                {fieldName : {'$in' : newValues}}
                                                                                ]
                                                                        } 
                                                            ]
                                                            },
                                                            {'$set' : {'_max_version_number' : previous_version['version_number']}}
                                                        )
                
                ##part 2 of split - inserting registers starting from new version. Therefore, in the end of the process, records
                #have been splitted in two parts. 
                res = self.collection.db['to_split'].update_many({},
                                                    {'$set' : {'_min_version_number' : new_version_number}}
                                                    )

                res = self.collection.db['to_split'].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])          
                self.collection.db['to_split'].drop()
            
            else: #New version is terminal, no need to split, but need to create another node
                #copying records
                res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                                {'_max_version_number' : previous_version['version_number']},                                                                            
                                                                                {'$or' : [{fieldName : {'$in' : newValues}},                                                                                                                                                                                                                                                   
                                                                                        {fieldName : oldValue}
                                                                                        ]
                                                                                } 
                                                                                ]
                                                                    } 
                                                            },
                                                            {'$unset': '_id'},
                                                        { '$out' : "to_split" } ])
                #updating version 
                res = self.collection.db['to_split'].update_many({},
                                                    {'$set' : {'_min_version_number' : new_version_number, '_max_version_number' : new_version_number}}
                                                    )

                res = self.collection.db['to_split'].aggregate([{'$match' : {}}, {'$merge': {'into' : self.collection.collection_processed.name, 'whenMatched' : 'fail'}}])          
                self.collection.db['to_split'].drop()


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

        if(next_version != None):
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']            

            res = self.collection.collection_versions.update_one({'version_number': next_version['version_number']},{'$set':{'previous_version':new_version_number}})        
            if(res.matched_count != 1):
                print("Next version not matched")

        i = self.collection.collection_versions.insert_one(new_version)

        if next_version != None:
            self.collection.collection_processed.update_many({'_evolution_list':previous_version['_id']}, {'$push' : {'_evolution_list':i.inserted_id}})


        if self.collection.operation_mode == 'preprocess':
            ##Update value of processed versions

            versions = self.collection.collection_versions.find({'$and': [{'next_operation.field' : fieldName},
                                                        {'next_operation.type' : 'ungrouping'}                                                      
                                                        ]}).sort('next_version_valid_from',ASCENDING)

                        
            #Ungrouping operation cannot be executed in the inverse order. Grouped documents cannot be transformed into ungrouped documents. However, it is possible to make a ghost element to represent this group in the past.
            
            versions = self.collection.collection_versions.find({'$and': [{'previous_operation.field' : fieldName},
                                                        {'previous_operation.type' : 'ungrouping'}                                                      
                                                        ]}).sort('previous_version_valid_from',DESCENDING)

            for version_change in versions:            
                previous_versions = self.collection.collection_versions.find({'version_number':version_change['previous_version']})
                previous_version_rec = previous_versions[0]

                res = self.collection.collection_processed.update_many({'$and':[{'_max_version_number':{'$lte' : version_change['previous_version']}},
                                                                    {'_valid_from' : {'$gte': version_change['previous_version_valid_from']}},
                                                                    {version_change['previous_operation']['field'] : {'$in' : version_change['previous_operation']['from']}},                                                                 
                                                                    ]
                                                            }, 
                                                            {'$set': {version_change['previous_operation']['field']: version_change['previous_operation']['to'], '_evoluted' : True},
                                                            '$push' : {'_evolution_list':previous_version_rec['_id']}
                                                            })   

                ##Lets just append to evolution list to the original records altered
                res = self.collection.collection_processed.update_many({'$and':[{'_min_version_number':{'$gte' : version_change['previous_version']}},
                                                                    {'_valid_from' : {'$gte': version_change['version_valid_from']}},
                                                                    {version_change['previous_operation']['field'] : {'$in' : version_change['previous_operation']['from']}},                                                                 
                                                                    ]
                                                            }, 
                                                            {                                                            
                                                                '$push' : {'_evolution_list':previous_version_rec['_id']}
                                                            })                                                        
            

            ##Pre-existing records have already been processed in the new version. We can update this in the original records collection. 

            self.collection.collection.update_many({'$and': [{'_last_processed_version': previous_version['version_number']}
                                                ]                                     
                                        },
                                        {'$set' :{'_last_processed_version' : new_version_number}})

            if(next_version != None):
                self.collection.collection.update_many({'$and': [{'_first_processed_version': next_version['version_number']}
                                                    ]                                     
                                            },
                                            {'$set':{'_first_processed_version' : new_version_number}})

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