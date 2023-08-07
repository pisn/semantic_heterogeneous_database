import time
import pandas as pd
import numpy as np
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
        normalized = pd.json_normalize(self.collection_versions.find())
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
        o['_valid_from'] = ValidFromDate        

        insertedDocument = self.collection.insert_one(o)        

        p = json.loads(JsonString)        
        p['_original_id'] = insertedDocument.inserted_id
        p['_original_version'] = VersionNumber
        p['_valid_from'] = ValidFromDate
        p['_evoluted'] = False
        p['_evolution_list'] = []    
        

        ##Ideia é checar aqui se pode ter tido alterações semanticas para cada tipo,
        #dado que parametros sao diferentes. Ai splitar ja os processados e talvez ate processar 
        #na hora. Assim nao preciso ficar checando na consulta mais. 
        
        check_list = list()                
        insertion_list = list()                
        minVersion = float(self.versions_df['version_number'].min())
        maxVersion = float(self.versions_df['version_number'].max())        
        p['_min_version_number'] = minVersion
        p['_max_version_number'] = maxVersion
        check_list.append(p)        

        while len(check_list) > 0:
            document = check_list.pop()           

            for operationType in self.semantic_operations:
                if operationType in ['grouping','translation']: ##somente para teste, depois eu implemento no desagrupamento também                    
                    affected_versions = self.semantic_operations[operationType].check_if_affected(document)                                       
                    
                    if affected_versions != None:                                           
                        for v in affected_versions:
                            if v[2] == 'forward':
                                operation = self.versions_df.loc[(self.versions_df['next_version'] == v[1])]
                                new_document = self.semantic_operations[operation['next_operation.type'].values[0]].evolute_forward(document, operation)                                
                                new_document['_min_version_number'] = v[1]
                                document['_max_version_number'] = v[0]
                                new_document['_evoluted'] = True
                                new_document['_evolution_list'].append(v[0])
                                check_list.append(new_document)
                            else:
                                operation = self.versions_df.loc[(self.versions_df['previous_version'] == v[1])]
                                new_document = self.semantic_operations[operation['previous_operation.type'].values[0]].evolute_backward(document, operation)                                
                                new_document['_max_version_number'] = v[1]
                                document['_min_version_number'] = v[0]
                                new_document['_evoluted'] = True
                                new_document['_evolution_list'].append(v[0])
                                check_list.append(new_document)

            insertion_list.append(document)  ## If affected versions, document limits already updated when arrived here    
        

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
        r['_valid_from'] = r[ValidFromField]
        r = r.loc[(r['start']<= r[ValidFromField]) & (r['end'] > r[ValidFromField])]
        r.rename(columns={'start':'version_valid_from'}, inplace=True)

        ##Preciso dar um jeito de passar o version number ja. Ja estou passando o valid from também. Para tentar processar tudo de uma vez só
        r_2 = pd.merge(r, self.versions_df, on='version_valid_from')
        r_2.rename(columns={'version_number':'_original_version'}, inplace=True)

        cols = list(dataframe.columns)
        cols.append('_valid_from')
        cols.append('_original_version')


        self.__insert_many_by_version(r_2[cols])


    def __insert_many_by_version(self, Group: pd.DataFrame):

        processed_group = Group.copy()              
        

        insertedDocuments = self.collection.insert_many(Group.to_dict('records'))              

        processed_group.insert(len(processed_group.columns),'_original_id', insertedDocuments.inserted_ids) 
        
        minVersion = float(self.versions_df['version_number'].min())
        maxVersion = float(self.versions_df['version_number'].max()) 
 
        processed_group['_evoluted'] = False                     
        processed_group['_min_version_number'] = minVersion
        processed_group['_max_version_number'] = maxVersion
        #processed_group['_evolution_list'] = []

        ### Verificação e processamento das alterações semanticas
        cols = list(processed_group.columns)
        recheck_group = processed_group.copy()

        while len(recheck_group) > 0:
            g = recheck_group[cols]
            recheck_group = pd.DataFrame()

            for operationType in self.semantic_operations:                
                affected_versions = self.semantic_operations[operationType].check_if_many_affected(g)

                if affected_versions != None:                                           
                    for v in affected_versions:
                        if v[2] == 'backward':
                            altered = self.semantic_operations[operationType].evolute_many_backward(v[0], v[1])
                            altered['_max_version_number'] = altered['previous_version']
                            v[1]['_min_version_number'] = v[1]['version_number'] #matched records before semantic evolution
                            recheck_group = pd.concat([recheck_group,altered, v[1]])   
                            alt_list = list(altered['_original_id'])                             
                            g=g.loc[~g['_original_id'].isin(alt_list)] 
                        else:
                            altered = self.semantic_operations[operationType].evolute_many_forward(v[0], v[1])
                            altered['_min_version_number'] = altered['next_version']
                            v[1]['_max_version_number'] = v[1]['version_number'] #matched records before semantic evolution
                            recheck_group = pd.concat([recheck_group,altered, v[1]])
                            alt_list = list(altered['_original_id'])                             
                            g=g.loc[~g['_original_id'].isin(alt_list)] 
            
            if len(g) > 0: # O que ta no g nao foi tocado por nenhuma alteração semantica e já pode ser inserido direto
                self.collection_processed.insert_many(g.to_dict('records'))
        
    
    def __process_query(self,QueryString):
        forward = self.__process_query_forward(QueryString)
        backward = self.__process_query_backward(QueryString)

        return {'$or':[forward, backward]}

    # I will start by assuming only rewrite forward
    ## Neste novo processo, eu nao posso usar o campo version da coleção raw. 
    ## Prq esse campo teoricamente nao vai existir neste modo de operação
    def rewrite_query(self, QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                #Lets be sure we do not get in an infinite loop here
                if isinstance(fieldValue, tuple):                    
                    fieldValueQ = fieldValue[0]    
                    p_version_start = fieldValue[3]                
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ, 'version_number':{'$gt': fieldValue[1]}}                    
                else:                    
                    fieldValueQ = fieldValue
                    p_version_start = None
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)

                next_fieldValue = None
                version_number = None
                next_version_start = None
                if(versions > 0): ##Existe alguma coisa a ser processada sobre este campo ainda                    
                    versions = self.collection_versions.find(q).sort('version_number')
                    for version in versions:
                        next_fieldValue = version['next_operation']['to']
                        version_number = version['version_number']
                        next_version_start = version['next_version_valid_from']
                        
                        #besides from the original value, this value also represents a record that was translated in the past 
                        # from the original query term. Therefore, it must be considered in the query                        
                        to_process.append((next_fieldValue,version_number, p_version_start, next_version_start)) 
                else:
                    if not isinstance(fieldValue, tuple): ## não é afetado por nenhuma operacao semantica
                        queryTerms[field].add(fieldValue)
                        continue
                
                queryTerms[field].add((fieldValueQ, p_version_start, next_version_start))

                    # if version_number == None:
                    #     queryTerms[field].add(fieldValue) 
                    # else:
                    #     if isinstance(fieldValue, list):
                    #         for f in fieldValue:
                    #             queryTerms[field].add((f,next_version_start))     
                    #     else:
                    #         queryTerms[field].add((fieldValue,next_version_start)) 

                
        
        ands = []

        for field in queryTerms.keys():
            ors = []

            for value in queryTerms[field]:
                if isinstance(value,tuple):
                    p_version_start = value[1]
                    next_version_start = value[2]
                    
                    if p_version_start == None:
                        ors.append({'$and':[{field:value[0]},{'time':{'$lte':next_version_start}}]})
                    elif next_version_start == None:
                        ors.append({'$and':[{field:value[0]},{'time':{'$gt':p_version_start}}]})
                    else:
                        ors.append({'$and':[{field:value[0]},{'time':{'$gt':p_version_start}},{'time':{'$lte':next_version_start}}]})
                else:
                    ors.append({field:value})

            ands.append({'$or' : ors})

        finalQuery = {'$and' : ands}    
        
        return finalQuery

    def __process_query_forward(self,QueryString):
        queryTerms = {}        
        
        for field in QueryString.keys():             
            queryTerms[field] = set()
            queryTerms[field].add(QueryString[field])

            to_process = []
            to_process.append(QueryString[field])

            while len(to_process) > 0:
                fieldValue = to_process.pop()
                
                #Lets be sure we do not get in an infinite loop here
                if isinstance(fieldValue, tuple):                    
                    fieldValueQ = fieldValue[0]
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ, 'version_number':{'$gt': fieldValue[1]}}                    
                else:                    
                    fieldValueQ = fieldValue
                    q = {'next_operation.field':field,'next_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)

                version_number = None
                if(versions > 0):
                    versions = self.collection_versions.find(q).sort('version_number')
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

                #Lets be sure we do not get in an infinite loop here
                if isinstance(fieldValue, tuple):                    
                    fieldValueQ = fieldValue[0]
                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ, 'version_number':{'$lt': fieldValue[1]}}                    
                else:                    
                    fieldValueQ = fieldValue
                    q = {'previous_operation.field':field,'previous_operation.from':fieldValueQ}
                
                versions = self.collection_versions.count_documents(q)
                version_number = None
                if(versions > 0):
                    versions = self.collection_versions.find(q).sort('version_number', DESCENDING)
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

            if not isinstance(field,str) or field[0] == '$' or field[0]=='_': #pymongo operators like $and, $or, etc                
                continue                           

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