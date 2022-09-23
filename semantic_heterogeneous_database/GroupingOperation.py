from ensurepip import version
import sys
from .SemanticOperation import SemanticOperation
import datetime
from argparse import ArgumentError
from pymongo import MongoClient, ASCENDING, DESCENDING

class GroupingOperation:
    def __init__(self, Collection_):
        self.collection = Collection_.collection

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

        previous_version = self.collection.collection_versions.find({'version_valid_from' : {'$lt' : validFromDate}}).sort('version_valid_from',-1)        
        previous_version = next(previous_version, None)

        next_version_count = self.collection.collection_versions.count_documents({'version_valid_from' : {'$gte' : validFromDate}})
        

        #If there are no versions starting from dates greater than the refDate, the new version number is just one increment after the last version before the refDate
        #In the other hand, if there are, this new version must be registered with a number before the previous version and the next version
        
        if(next_version_count > 0):
            next_version = self.collection.collection_versions.find({'version_valid_from' : {'$gte' : validFromDate}}).sort('version_valid_from')        
            next_version = next(next_version,None)
            new_version_number = previous_version['version_number'] + (next_version['version_number'] - previous_version['version_number'])/2
        else:
            next_version = None
            self.collection.current_version = self.collection.current_version + 1 #this is the newest version now
            new_version_number = self.collection.current_version
            
            
        ##For processed records unaffected by the grouping, ending in the previous version, version interval should be extended to include the new version,        

        res = self.collection.collection_processed.update_many({'$and' : [{'_max_version_number' : previous_version['version_number']},
                                                               {fieldName : {'$nin' : oldValues}},
                                                               {fieldName : {'$ne' : newValue}}
                                                              ]}, {'$set' : {'_max_version_number' : new_version_number}})
        
        
        ##Spliting processed registers affected by the grouping where the new version is within the min and max version number
        if(next_version != None):
            res = self.collection.collection_processed.update_many({'$and' : [{'_min_version_number' : next_version['version_number']},
                                                                   {'$or' : [{fieldName : {'$in' : oldValues}},
                                                                              {fieldName : newValue}
                                                                             ]
                                                                   }
                                                              ]}, {'$set' :{'_min_version_number' : new_version_number}})                                                              
      
            
            #Copying all records in this situation
            res = self.collection.collection_processed.aggregate([{ '$match': {'$and': [
                                                                            {'_min_version_number' : {'$lte' : previous_version['version_number']}},
                                                                            {'_max_version_number' : {'$gte' : next_version['version_number']}},
                                                                            {'$or' : [{fieldName : {'$in' : oldValues}},
                                                                                    {fieldName : newValue}
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
                                                                    {'$or' : [{fieldName : {'$in' : oldValues}},
                                                                              {fieldName : newValue}
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
                                                                            {'$or' : [{fieldName : {'$in' : oldValues}},                                                                                                                                                                                                                                                   
                                                                                      {fieldName : newValue}
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

        res = self.collection.collection_versions.update_one({'version_number': previous_version['version_number']}, {'$set' : {'next_operation': next_operation, 'next_version':new_version_number, 'next_version_valid_from' : validFromDate}})
        
        if(res.matched_count != 1):
            print("Previous version not matched")

        if(next_version != None):
            new_version['next_version'] = next_version['version_number']
            new_version['next_version_valid_from'] = next_version['version_valid_from']
            new_version['next_operation'] = previous_version['next_operation']            

            res = self.collection.collection_versions.update_one({'version_number': next_version['version_number']},{'$set':{'previous_version':new_version_number}})        
            if(res.matched_count != 1):
                print("Next version not matched")

        column = self.collection.collection_columns.find_one({'field_name':fieldName}) 
        if column['last_edit_version'] < new_version_number:
            self.collection.collection_columns.update_one({'field_name':fieldName}, {'$set' : {'last_edit_version' : new_version_number}})
        elif column['first_edit_version'] < new_version_number:
            self.collection.collection_columns.update_one({'field_name':fieldName}, {'$set' : {'first_edit_version' : new_version_number}})        

        self.collection.collection_versions.insert_one(new_version)          

        self.collection.update_versions()
        


        ##Update value of processed versions

        versions = self.collection.collection_versions.find({'$and': [{'next_operation.field' : fieldName},
                                                      {'next_operation.type' : 'grouping'}                                                      
                                                     ]}).sort('next_version_valid_from',ASCENDING)

        for version_change in versions:
            res = self.collection.collection_processed.update_many({'$and':[{'_min_version_number':{'$gte' : version_change['next_version']}},
                                                                 {'_valid_from' : {'$lte': version_change['next_version_valid_from']}},
                                                                 {version_change['next_operation']['field'] : {'$in' : version_change['next_operation']['from']}},                                                                 
                                                                ]
                                                        }, 
                                                        {'$set': {version_change['next_operation']['field']: version_change['next_operation']['to'], '_evoluted' : True},
                                                         '$push' : {'_evolution_list': version_change['version_number']}
                                                        })   

            ##Lets just append to evolution list to the original records altered
            res = self.collection.collection_processed.update_many({'$and':[{'_max_version_number':{'$lte' : version_change['next_version']}},
                                                                 {'_valid_from' : {'$lte': version_change['next_version_valid_from']}},
                                                                 {version_change['next_operation']['field'] : {'$in' : version_change['next_operation']['from']}},                                                                 
                                                                ]
                                                        }, 
                                                        {                                                            
                                                            '$push' : {'_evolution_list':version_change['next_version']}
                                                        })
            

        #Grouping operation cannot be executed in the inverse order. Grouped documents cannot be transformed into ungrouped documents. However, it is possible to make a ghost element to represent this group in the past.
        
        versions = self.collection.collection_versions.find({'$and': [{'previous_operation.field' : fieldName},
                                                      {'previous_operation.type' : 'grouping'}                                                      
                                                     ]}).sort('previous_version_valid_from',DESCENDING)

        for version_change in versions:            
            res = self.collection.collection_processed.update_many({'$and':[{'_max_version_number':{'$lte' : version_change['previous_version']}},
                                                                 {'_valid_from' : {'$gte': version_change['previous_version_valid_from']}},
                                                                 {version_change['previous_operation']['field'] : version_change['previous_operation']['from']},                                                                 
                                                                ]
                                                        }, 
                                                        {'$set': {version_change['previous_operation']['field']: ' or '.join(str(version_change['previous_operation']['to'])) + ' (grouped)', '_evoluted' : True},
                                                         '$push' : {'_evolution_list':version_change['version_number']}
                                                        })   

            ##Lets just append to evolution list to the original records altered
            res = self.collection.collection_processed.update_many({'$and':[{'_min_version_number':{'$gte' : version_change['previous_version']}},
                                                                 {'_valid_from' : {'$gte': version_change['version_valid_from']}},
                                                                 {version_change['previous_operation']['field'] : version_change['previous_operation']['from']},                                                                 
                                                                ]
                                                        }, 
                                                        {                                                            
                                                            '$push' : {'_evolution_list':version_change['previous_version']}
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
        return_obj = set()
        
        # Agrupamento nao pode andar nesse sentido
        # if 'previous_operation.type' in versions_df.columns:
        #     versions_df_p = versions_df.loc[versions_df['previous_operation.type'] == 'grouping']

        #     if len(versions_df_p) > 0:
        #         if {'previous_operation.type','previous_operation.field', 'previous_operation.from'}.issubset(versions_df.columns):  
        #             versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['previous_operation.field'],None), axis=1)
        #             versions_df_p = versions_df_p.loc[ versions_df_p['field_value'] in versions_df_p['previous_operation.from']]
                    
        #             if len(versions_df_p) > 0:
        #                 return_obj.update([float(versions_df_p['version_number']),float(versions_df_p['previous_version'])])

        if 'next_operation.type' in versions_df.columns:
            versions_df_p = versions_df.loc[versions_df['next_operation.type'] == 'grouping']

            if len(versions_df_p) > 0:
                if {'next_operation.type','next_operation.field', 'next_operation.from'}.issubset(versions_df.columns):  
                    versions_df_p['field_value'] = versions_df_p.apply(lambda row: Document.get(row['next_operation.field'], None), axis=1)
                    versions_df_p = versions_df_p.loc[ versions_df_p['field_value'] == versions_df_p['next_operation.from']]
                    
                    if len(versions_df_p) > 0:
                        return_obj.update([float(versions_df_p['version_number']),float(versions_df_p['next_version'])])
        

        return list(return_obj)

    ## Function is executed when is already known the document suffered changes
    def evolute_forward(self, Document, operation):        
        if Document[operation['next_operation.field'].values[0]] in operation['next_operation.from'].values[0]:
            raise BaseException('Operation does not change this record')
        
        Document[operation['next_operation.field'].values[0]] = operation['next_operation.to'].values[0]
        return Document

    def evolute_backward(self, Document, operation):        
        pass

    def evolute(self, Document, TargetVersion):
        lastVersion = float(Document['_last_processed_version'])
        firstVersion = float(Document['_first_processed_version'])        

        if TargetVersion > lastVersion:
            while lastVersion < TargetVersion:                
                
                lastVersionDocument = self.collection.collection_processed.find_one({'_original_id' : Document['_id'], '_max_version_number': lastVersion})                                

                versionRegister = self.collection.collection_versions.find_one({'version_number' : lastVersion})

                if(versionRegister == None):
                    raise Exception('version register not found')

                evolutionOperation = versionRegister['next_operation']

                if(evolutionOperation['type'] == 'grouping'):
                    field = evolutionOperation['field']
                    oldValues = evolutionOperation['from']
                    newValue = evolutionOperation['to']

                    if(field in lastVersionDocument):
                        if lastVersionDocument[field] in oldValues:
                            ##new row needed because register must change
                            lastVersionDocument[field] = newValue
                            lastVersionDocument['_evoluted'] = True
                            lastVersionDocument['_evolution_list'] = [versionRegister['next_version']]
                            if 'previous_version' in versionRegister:
                                lastVersionDocument['_evolution_list'].append(versionRegister['previous_version'])

                            lastVersionDocument['_min_version_number'] = versionRegister['next_version']
                            lastVersionDocument['_max_version_number'] = versionRegister['next_version']
                            lastVersionDocument.pop('_id')
                            self.collection.collection_processed.insert_one(lastVersionDocument)
                        else:
                            ##Just extend versions
                            self.collection.collection_processed.update_one({'_id':lastVersionDocument['_id']}, {'$set':{'_max_version_number':versionRegister['next_version']}})
                else:
                    raise 'Error processing evolution of wrong type:' + evolutionOperation['type']        
                
                if(versionRegister['next_version'] != None):
                    lastVersion = float(versionRegister['next_version'])
                else: #Chegou a ultima versao disponivel, aumentar um ponto
                    lastVersion = self.current_version
                
                
                Document['_last_processed_version'] = lastVersion                
        else:
            while firstVersion > TargetVersion:
                firstVersionDocument = self.collection.collection_processed.find_one({'_original_id' : Document['_id'], '_version_number': firstVersion})                                

                versionRegister = self.collection.collection_versions.find_one({'version_number' : firstVersion})

                if(versionRegister == None):
                    raise Exception('version register not found')

                evolutionOperation = versionRegister['previous_operation']

                if(evolutionOperation['type'] == 'grouping'):
                    field = evolutionOperation['field']
                    oldValue = evolutionOperation['to']
                    newValues = evolutionOperation['from']

                    if(field in firstVersionDocument):
                        if firstVersionDocument[field] == oldValue:
                            firstVersionDocument[field] = ' or '.join(newValues) + ' (grouped)'
                            firstVersionDocument['_evoluted'] = True
                            
                            firstVersionDocument['_evolution_list'] = [versionRegister['previous_version']]
                            if 'next_version' in versionRegister:
                                firstVersionDocument['_evolution_list'].append(versionRegister['next_version'])

                            firstVersionDocument['_min_version_number'] = versionRegister['previous_version']
                            firstVersionDocument['_max_version_number'] = versionRegister['previous_version']
                            firstVersionDocument.pop('_id')
                            self.collection.collection_processed.insert_one(firstVersionDocument)
                    else:
                        self.collection.collection_processed.update_one({'_id':firstVersionDocument['_id']}, {'$set':{'_min_version_number':versionRegister['previous_version']}})
                else:
                    raise 'Error processing evolution of wrong type:' + evolutionOperation['type']        
                
                firstVersion = float(versionRegister['previous_version'])                       
                Document['_first_processed_version'] = firstVersion                

        self.collection.collection.update_one({'_id':Document['_id']}, {'$set': {'_first_processed_version': Document['_first_processed_version'], '_last_processed_version': Document['_last_processed_version']}})