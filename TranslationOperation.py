import zope.interface
import sys
import SemanticOperation
from argparse import ArgumentError

@zope.interface.implementer(SemanticOperation)
class TranslationOperation:
    def __init__(self, Collection):
        self.collection = Collection

    def execute_operation(self, validFromDate:datetime, **args):
        if 'oldValue' not in args:
            raise ArgumentError("Missing 'oldValue' parameter for translation")
        
        if 'newValue' not in args:
            raise ArgumentError("Missing 'newValue' parameter for translation")

        if not isinstance(validFromDate,datetime):
            raise TypeError("'validFromDate' argument is not a datetime.")

        oldValue = args['oldValue']
        newValue=args['newValue']

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
                                                               {fieldName : {'$ne' : oldValue}},
                                                               {fieldName : {'$ne' : newValue}}
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
                                                  {'$unset': '_id'},
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
                                                                            {'$or' : [{fieldName : oldValue},                                                                                                                                                                                                                                                   
                                                                                      {fieldName : newValue}
                                                                                     ]
                                                                            } 
                                                                            ]
                                                                   } 
                                                        },
                                                        {'$unset': '_id'},
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

        versions = self.collection_versions.find({'$and': [{'next_operation.field' : fieldName},
                                                      {'next_operation.type' : 'translation'}                                                      
                                                     ]}).sort('next_version_valid_from',ASCENDING)

        for version_change in versions:
            res = self.collection_processed.update_many({'$and':[{'_min_version_number':{'$gte' : version_change['next_version']}},
                                                                 {'_valid_from' : {'$lte': version_change['next_version_valid_from']}},
                                                                 {version_change['next_operation']['field'] : version_change['next_operation']['from']},                                                                 
                                                                ]
                                                        }, 
                                                        {'$set': {version_change['next_operation']['field']: version_change['next_operation']['to'], '_evoluted' : True}})   

        versions = self.collection_versions.find({'$and': [{'previous_operation.field' : fieldName},
                                                      {'previous_operation.type' : 'translation'}                                                      
                                                     ]}).sort('previous_version_valid_from',DESCENDING)

        for version_change in versions:            
            res = self.collection_processed.update_many({'$and':[{'_max_version_number':{'$lte' : version_change['previous_version']}},
                                                                 {'_valid_from' : {'$gte': version_change['previous_version_valid_from']}},
                                                                 {version_change['previous_operation']['field'] : version_change['previous_operation']['from']},                                                                 
                                                                ]
                                                        }, 
                                                        {'$set': {version_change['previous_operation']['field']: version_change['previous_operation']['to'], '_evoluted' : True}})   
        

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
                
        
        

    def execute_many_operations_by_csv(self, filePath):
        pass

    def evolute(self, document, operationArgs):
        
        pass
        