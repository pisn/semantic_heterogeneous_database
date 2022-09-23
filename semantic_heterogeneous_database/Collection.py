from distutils.version import Version
from re import A
import time
import pandas as pd
from .SemanticOperation import SemanticOperation
from bson.objectid import ObjectId
import numpy as np
from pymongo import MongoClient, ASCENDING,DESCENDING
from pandas.io.json import json_normalize
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
                "version_valid_from":datetime(1700,1,1),
                "previous_version":None, 
                "previous_operation":None,
                "version_number":0, 
                "next_version":None,
                "next_operation":None
            }
            self.collection_versions.insert_one(first_version)                       
        else:
            self.current_version = self.current_version['version_number']

        ## Create indexes. The operation is idempotent. Nothing will be done if the index is already there

        self.collection.create_index([('_first_processed_version',ASCENDING)])
        self.collection.create_index([('_last_processed_version',ASCENDING)])        

        ## Loading columns collection in memory

        fields = self.collection_columns.find({})
        self.fields = dict([(col['field_name'], (col['first_edit_version'], col['last_edit_version'])) for col in fields])

        ## Loading semantic operations in memory       
        self.update_versions()
        
    def update_versions(self):
        normalized = json_normalize(self.collection_versions.find())
        self.versions_df = pd.DataFrame(normalized)

    def register_operation(self, OperationKey, SemanticOperationClass):
        self.semantic_operations[OperationKey] = SemanticOperationClass


    def insert_one(self, JsonString, ValidFromDate:datetime):
        """ Insert one documet in the collection.

        Args:
            JsonString(): string of the json representation of the document being inserted.
            valid_from_date(): date of when the register becomes valid. This is important in the treatment of semantic changes along time
        
        """               
        self.update_versions()

        versions = self.collection_versions.find({'version_valid_from':{'$lte' : ValidFromDate}}).sort('version_valid_from',DESCENDING)
        version = next(versions, None)                       
        
        self.__insert_one_by_version(JsonString, version['version_number'], ValidFromDate)                


    def __insert_one_by_version(self, JsonString, VersionNumber, ValidFromDate:datetime):        
        o = json.loads(JsonString)                
        o['_original_version']=VersionNumber           
        #o['_first_processed_version'] = VersionNumber ##Acho que vou acabar descontinuando esses campos se o check_affected der certo
        #o['_last_processed_version'] = VersionNumber
        o['_valid_from'] = ValidFromDate        

        insertedDocument = self.collection.insert_one(o)        

        p = json.loads(JsonString)
        # p['_min_version_number'] = VersionNumber
        # p['_max_version_number'] = VersionNumber
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = VersionNumber
        p['_valid_from'] = ValidFromDate
        p['_evoluted'] = False
        p['_evolution_list'] = []    
        

        ##Ideia é checar aqui se pode ter tido alterações semanticas para cada tipo,
        #dado que parametros sao diferentes. Ai splitar ja os processados e talvez ate processar 
        #na hora. Assim nao preciso ficar checando na consulta mais. 
        affected_versions = list()
        minVersion = float(self.versions_df['version_number'].min())
        maxVersion = float(self.versions_df['version_number'].max())
        affected_versions.append(minVersion)
        affected_versions.append(maxVersion)

        for operationType in self.semantic_operations:
            if operationType in ['grouping','translation']: ##somente para teste, depois eu implemento no desagrupamento também
                affected_versions.extend(self.semantic_operations[operationType].check_if_affected(p))                

        affected_versions.sort()
        insertion_list = list()
        original_processed_index = -1

        for i in range(0,len(affected_versions)-1, 2):
            po = p.copy()
            a = affected_versions[i]
            b = affected_versions[i+1]

            po['_min_version_number'] = float(a)
            po['_max_version_number'] = float(b)
            insertion_list.append(po)

            if a <= VersionNumber and b >= VersionNumber:
                original_processed_index = len(insertion_list) - 1
        
        for i in range(original_processed_index+1, len(insertion_list)):
            document = insertion_list[i]
            document['_evolution_list'] = [VersionNumber]
            operation = self.versions_df.loc[(self.versions_df['next_version'] == document['_min_version_number'])]

            self.semantic_operations[operation['next_operation.type'].values[0]].evolute_forward(document, operation)

        for i in range(-1,original_processed_index, -1):
            document = insertion_list[i]
            document['_evolution_list'] = [VersionNumber]
            
            operation = self.versions_df.loc[(self.versions_df['previous_version'] == document['_max_version_number'])]

            self.semantic_operations[operation['previous_operation.type'].values[0]].evolute_backward(document, operation)
        
        self.collection_processed.insert_many(insertion_list)


        new_fields = list()
        for field in o:
            if not field.startswith('_'):                
                if field not in self.fields:
                    new_field = {'field_name': field, 'first_edit_version': minVersion, 'last_edit_version': maxVersion}                
                    new_fields.append(new_field)
                    self.fields[field] = (minVersion, maxVersion)

        if len(new_fields) > 0:
            self.collection_columns.insert_many(new_fields)
        

    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        """ Insert many recorDs in the collection using a csv file. 

        Args: 
            FilePath (): path to the csv file
            validValidFromField_from_field (): csv field which should be used to determine the date of when the register becomes valid. This is important in the treatment of semantic changes along time
            ValidFromDateFormat (): the expected format of the date contained in the valid_from_field values
            Delimiter (): delimiter used in the file. 

        """
        df = pd.read_csv(FilePath, delimiter=Delimiter)
        df[ValidFromField] = pd.to_datetime(df[ValidFromField], format=ValidFromDateFormat)

        self.insert_many_by_dataframe(df, ValidFromField)
    
    def insert_many_by_dataframe(self, dataframe, ValidFromField):        
        all_versions = self.collection_versions.find(projection=['version_valid_from'])
        dates = pd.DataFrame(all_versions).sort_values(by='version_valid_from')
        dates = dates.append([{'version_valid_from':datetime(2200,12,31)}])
        dates= dates.reset_index(drop=True)

        dates_1= dates.copy().reindex(index=np.roll(dates.index,-1))
        dates_1= dates_1.reset_index(drop=True)
        dates_guide = pd.concat([dates['version_valid_from'],dates_1['version_valid_from']], axis=1).set_axis(['start','end'], axis=1)

        r = dataframe.merge(dates_guide, how='cross')
        r[ValidFromField] = pd.to_datetime(r[ValidFromField])
        r['start'] = pd.to_datetime(r['start'])
        r['end'] = pd.to_datetime(r['end'])
        r = r.loc[(r['start']<= r[ValidFromField]) & (r['end'] > r[ValidFromField])]

        chunks = r.groupby(['start'])

        for versionValidFrom, group in chunks:            
            versions = self.collection_versions.find({'version_valid_from':{'$lte' : versionValidFrom}}).sort('version_valid_from',DESCENDING)
            version = next(versions, None)

            self.__insert_many_by_version(group.drop('start', 1).drop('end',1), version['version_number'], versionValidFrom)        


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
        #processed_group['_evolution_list'] = []
        
        self.collection_processed.insert_many(processed_group.to_dict('records'))

        new_fields = list()
        for field in Group.columns:
            if not field.startswith('_'):
                if field not in self.fields:
                    new_field = {'field_name': field, 'first_edit_version': VersionNumber, 'last_edit_version': VersionNumber}                
                    new_fields.append(new_field)
                    self.fields[field] = (VersionNumber, VersionNumber)

        if len(new_fields) > 0:
            self.collection_columns.insert_many(new_fields)

        
    
    def __process_query(self,QueryString):
        forward = self.__process_query_forward(QueryString)
        backward = self.__process_query_backward(QueryString)

        return {'$or':[forward, backward]}

    def __process_query_forward(self,QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                versions = self.collection_versions.count_documents({'next_operation.field':field,'next_operation.from':fieldValue})
                version_number = None
                if(versions > 0):
                    versions = self.collection_versions.find({'next_operation.field':field,'next_operation.from':fieldValue})
                    for version in versions:
                        fieldValue = version['next_operation']['to']
                        version_number = version['version_number']

                        if isinstance(fieldValue, list):
                            for f in fieldValue:
                                to_process.append((f, version_number))
                        else:
                            to_process.append((fieldValue,version_number)) #besides from the original query, this value could also represent a record that were translated in the past from the original query term. Therefore, it must be considered in the query                        
                
                if version_number == None:
                    queryTerms[field].add(fieldValue) 
                else:
                    if isinstance(fieldValue, list):
                        for f in fieldValue:
                            queryTerms[field].add((f,version_number))     
                    else:
                        queryTerms[field].add((fieldValue,version_number)) 

                
        
        ands = []

        for field in queryTerms.keys():
            ors = []

            for value in queryTerms[field]:
                if isinstance(value,tuple):
                    ors.append({'$and':[{field:value[0]},{'_evolution_list':value[1]}]})
                else:
                    ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}    
        
        return finalQuery

    def __process_query_backward(self,QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                versions = self.collection_versions.count_documents({'previous_operation.field':field,'previous_operation.from':fieldValue})
                version_number = None
                if(versions > 0):
                    versions = self.collection_versions.find({'previous_operation.field':field,'previous_operation.from':fieldValue})
                    for version in versions:
                        fieldValue = version['previous_operation']['to']
                        version_number = version['version_number']

                        if isinstance(fieldValue,list):
                            for f in fieldValue:
                                to_process.append((f,version_number))
                        else:
                            to_process.append((fieldValue,version_number)) #besides from the original query, this value could also represent a record that were translated in the past from the original query term. Therefore, it must be considered in the query                        
                
                if version_number == None:
                    queryTerms[field].add(fieldValue) 
                else:
                    if isinstance(fieldValue, list):
                        for f in fieldValue:
                            queryTerms[field].add((f,version_number))     
                    else:
                        queryTerms[field].add((fieldValue,version_number)) 

                
        
        ands = []

        for field in queryTerms.keys():
            ors = []

            for value in queryTerms[field]:
                if isinstance(value,tuple):
                    ors.append({'$and':[{field:value[0]},{'_evolution_list':value[1]}]})
                else:
                    ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}    
        
        return finalQuery
    
    def count_documents(self, QueryString):
        finalQuery = self.__process_query(QueryString)        
        return self.__query_specific(finalQuery, isCount=True)

    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        """ Query the collection with the supplied queryString

        Args:
            queryString(): string of the query in json. The syntax is the same as the pymongo syntax.
        
        Returns: a cursor for the records returned by the query

        """
        #start = time.time()
        finalQuery = self.__process_query(QueryString)                          
        #end = time.time()
        #print('Query processing:' + str(end-start))       

        #start = time.time()
        r = self.__query_specific(finalQuery)     
        #end = time.time()
        #print('Query results:' + str(end-start))
        return r

    def __query_specific(self, Query, VersionNumber=None, isCount=False):
        #Eu preciso depois permitir que seja o tempo da versao, nao o numero
        #E tambem que dados possam ser inseridos com tempo anterior, e assumir a versao da época.

        if(VersionNumber == None):
            VersionNumber = self.current_version   

        max_version_number = VersionNumber
        min_version_number = VersionNumber
        
        ##obtaining version to be queried
        
        to_process = []
        to_process.append(Query) 
        
        start = time.time()
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

            if not isinstance(field,str) or field[0] == '$' or field[0]=='_': #pymongo operators like $and, $or, etc
                # if isinstance(query[field],list):
                #     to_process.extend(query[field])
                continue                   

            fieldRegister = self.fields.get(field, None)

            if fieldRegister == None:
                raise 'Field not found in collection: ' + field
            
            firstFieldVersion = int(fieldRegister[0])
            lastFieldVersion = int(fieldRegister[1])            

            if VersionNumber > lastFieldVersion and lastFieldVersion > max_version_number:
                max_version_number = lastFieldVersion

            elif VersionNumber < firstFieldVersion and firstFieldVersion < min_version_number:
                min_version_number = firstFieldVersion
        
        end = time.time()
        #print('Find out version:' + str(end-start))

        ###Vou assumir por enquanto que estou sempre consultando a ultima versao, e que portanto sempre vou evoluir. Mas pensar no caso de que seja necessário um retrocesso
        VersionNumber = max_version_number

        ###Obtaining records which have not been translated yet to the target version and translate them                    
        # to_translate_up = self.collection.find({'_last_processed_version' : {'$lt' : VersionNumber}})
        # to_translate_down = self.collection.find({'_first_processed_version' : {'$gt' : VersionNumber}})
        # to_translate_up = list(to_translate_up)        
        
        # for record in to_translate_up:
        #     lastVersion = record['_last_processed_version']
        #     while lastVersion < VersionNumber:
        #         start = time.time()        
        #         versionRegister = self.collection_versions.find_one({'version_number':lastVersion})
                

        #         if versionRegister == None:
        #             raise Exception('Version register not found for ' + str(lastVersion))

        #         nextOperation = versionRegister['next_operation']
        #         nextOperationType = nextOperation['type']

        #         if nextOperationType not in self.semantic_operations:
        #             raise Exception(f"Operation type not supported: {nextOperationType}")

        #         end = time.time()
        #         print('Query collection versions:' + str(end-start))

        #         start = time.time()
        #         semanticOperation = self.semantic_operations[nextOperationType]
        #         semanticOperation.evolute(record, versionRegister['next_version'])  
        #         end = time.time()
        #         print('Evolution:' + str(end-start))

        #         lastVersion = versionRegister['next_version'] 
        # end = time.time()
        # #print('Evolution up:' + str(end-start))         
        
        # start = time.time()
        # for record in to_translate_down:
        #     firstVersion = record['_first_processed_version']
        #     while firstVersion > VersionNumber:
        #         versionRegister = self.collection_versions.find_one({'version_number':firstVersion})

        #         if versionRegister == None:
        #             raise Exception('Version register not found for ' + str(firstVersion))

        #         previousOperation = versionRegister['previous_operation']
        #         previousOperationType = previousOperation['type']

        #         if previousOperationType not in self.semantic_operations:
        #             raise Exception(f"Operation type not supported: {previousOperationType}")

        #         semanticOperation = self.semantic_operations[previousOperationType]
        #         semanticOperation.evolute(record, versionRegister['previous_version'])       

        #         firstVersion = versionRegister['previous_version']     
        
        # end = time.time()
        # #print('Evolution down:' + str(end-start))

        Query['_min_version_number'] = {'$lte' : VersionNumber} ##Retornando registros traduzidos. 
        Query['_max_version_number'] = {'$gte' : VersionNumber} ##Retornando registros traduzidos. 

        if isCount:
            return self.collection_processed.count_documents(Query)
        else:
            return self.collection_processed.find(Query)

    def execute_operation(self, operationType, validFrom, args):                  
        operation = self.semantic_operations[operationType]
        operation.execute_operation(validFromDate=validFrom, args=args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromColumn):
        with open(filePath, 'r') as csvFile:
            reader = csv.DictReader(csvFile)

            for row in reader:
                operationType = row[operationTypeColumn]
                operation = self.semantic_operations[operationType]
                operation.execute_operation(validFromDate=datetime.strptime(row[validFromColumn], '%Y-%m-%d'), args=row)

    def pretty_print(self, recordsCursor):
        """ Pretty print the records in the console. Semantic changes will be presented in the format "CurrentValue (originally: OriginalValue)"

        Args:
            recordsCursor(): the cursor for the records, usually obtained through the find functions
        
        """        
        fieldLengths = {}
             
        records = list(recordsCursor)
        
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

                    if(fieldLengths[field] < len(str(record[field]))):
                        fieldLengths[field] = len(str(record[field]))

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
                    print('|' + str(record[field]) + ' '*(fieldLengths[field] - len(str(record[field]))), end='')
            print('|')