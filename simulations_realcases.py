import argparse
import os
import shutil
import uuid
import time
import json
import random
import math
import pandas as pd
from datetime import datetime
from pymongo import MongoClient
from database_generator import DatabaseGenerator
from semantic_heterogeneous_database import BasicCollection
import re
pd.options.mode.chained_assignment = None  # default='warn'

#python simulations_realcases.py --method='insertion_first' --sourcefolder='/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/source/' --datecolumn='ano' --destination='teste.csv' --dbname='experimento_datasus' --collectionname='db_experimento_datasus' --mode='preprocess' --operations='/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/operations_cid9_cid10.csv'

# parser=argparse.ArgumentParser()
# parser.add_argument("--method")
# parser.add_argument("--sourcefolder")
# parser.add_argument("--datecolumn")
# parser.add_argument("--destination")
# parser.add_argument("--dbname")
# parser.add_argument("--collectionname")
# parser.add_argument("--mode")
# parser.add_argument("--operations") 

# args=parser.parse_args()

# operation_mode = args.mode
# method = args.method
# dbname = args.dbname
# collectionname = args.collectionname
# source_folder = args.sourcefolder
# date_columns = args.datecolumn
# csv_destination = args.destination
# operations_file = args.operations

#print(f'Test Arguments:{str(args)}')

#operation_mode = 'rewrite'
# operation_mode = 'preprocess'
# method = 'insertion_first'
# dbname = 'experimento_datasus_2'
# collectionname = 'db_experimento_datasus'
# source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/source/'
# date_columns = 'ano'
# csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/results/'
# operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/operations_cid9_cid10.csv'
# number_of_operations = 100
# percent_of_heterogeneous_queries = 0.3
# percent_of_insertions = 0.3

# if method != 'insertion_first' and method != 'operations_first':
#     raise BaseException('Method not implemented')

# host = 'localhost'

class Comparator:
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions, execution_try, generate_hashes, output_file, core_index):
        self.operation_mode = operation_mode
        self.method = method
        self.dbname = dbname
        self.collectionname = collectionname
        self.source_folder = source_folder
        self.date_columns = date_columns
        self.csv_destination = csv_destination
        self.operations_file = operations_file
        self.number_of_operations = number_of_operations
        self.percent_of_heterogeneous_queries = percent_of_heterogeneous_queries
        self.percent_of_insertions = percent_of_insertions
        self.execution_try = execution_try
        self.host = host
        self.collection = BasicCollection(self.dbname, self.collectionname, self.host, self.operation_mode)
        self.output_file = output_file
        self.generate_hashes = generate_hashes
        self.core_index = core_index        

        os.makedirs(csv_destination, exist_ok=True)
        

    def __insert_first(self):            
        start = time.time()
        
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)        
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')

        if self.core_index and self.operation_mode=='preprocess':
            self.collection.create_index('_min_version_number',1)
            self.collection.create_index('_max_version_number',1)
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write('insertion;insertion_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def __operations_first(self):
        start = time.time()
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')
        
        # if self.core_index and self.operation_mode=='preprocess':
        #     self.collection.create_index([('_min_version_number',1),('cid',1),('RefDate',1)])
        #     self.collection.create_index([('_max_version_number',1),('cid',1),('RefDate',1)])
            # self.collection.create_index([('_min_version_number',1)])
            # self.collection.create_index([('_max_version_number',1)])

        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)

        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }
        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write('insertion;operations_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def insert(self):
        if self.method == 'insertion_first':
            return self.__insert_first()
        elif self.method == 'operations_first':
            return self.__operations_first()
        else:
            raise BaseException('Method not implemented')
    
    def generate_domain_profile(self):        
        heterogeneous_domain = {}
        non_heterogeneous_domain = {}
        
        for operation_type in ['translation','grouping','ungrouping']:
            fields = self.collection.collection.collection_versions.distinct('next_operation.field', {'next_operation.type':operation_type})

            for field in fields:
                values = self.collection.collection.collection_versions.distinct('next_operation.from', {'next_operation.type':operation_type, 'next_operation.field':field})
                values.extend(self.collection.collection.collection_versions.distinct('next_operation.to', {'next_operation.type':operation_type, 'next_operation.field':field}))

                heterogeneous_domain[field] = set(values)        

        mr = self.collection.collection.db.command('mapreduce', self.collectionname, map='function() { for (var key in this) { emit(key, null); } }', reduce='function(key, stuff) { return null; }', out=self.collectionname + '_keys')
        fields = self.collection.collection.db[self.collectionname + '_keys'].distinct('_id')
        
        for field in fields:
            if field.startswith('_'):
                continue
            values = self.collection.collection.collection.distinct(field)
            non_heterogeneous_domain[field] = set(values).difference(heterogeneous_domain.get(field, set()))

        self.collection.collection.db[self.collectionname + '_keys'].drop()

        field_types = {}

        for field in heterogeneous_domain:
            values = heterogeneous_domain[field]
            if all(isinstance(value, (int, float)) for value in values):
                field_types[field] = 'numeric'
            elif all(isinstance(value, str) for value in values):
                field_types[field] = 'text'
            elif all(isinstance(value, datetime) for value in values):
                field_types[field] = 'date'
            else:
                field_types[field] = 'mixed'

        for field in non_heterogeneous_domain:
            values = non_heterogeneous_domain[field]
            if all(isinstance(value, (int, float)) for value in values):
                field_types[field] = 'numeric'
            elif all(isinstance(value, str) for value in values):
                field_types[field] = 'text'
            elif all(isinstance(value, datetime) for value in values):
                field_types[field] = 'date'
            else:
                field_types[field] = 'mixed'

        return heterogeneous_domain, non_heterogeneous_domain, field_types    

    
    def generate_queries_list(self):
        output_file = f'queries_{str(self.percent_of_heterogeneous_queries)}_{str(self.percent_of_insertions)}_{str(self.number_of_operations)}.txt'
        queries_file = self.csv_destination + output_file

        if os.path.exists(queries_file):            
            return
                

        domain_dict_heterogeneous, domain_dict_nonheterogeneous, field_types = self.generate_domain_profile()

        updates = math.floor(self.number_of_operations*self.percent_of_insertions)
        reads = self.number_of_operations-updates

        heterogeneous_queries = math.floor(reads*self.percent_of_heterogeneous_queries)
        non_heterogeneous_queries = reads-heterogeneous_queries
        sequence = ([0]*heterogeneous_queries)
        sequence.extend([1]*non_heterogeneous_queries)

        heterogeneous_inserts = math.floor(updates*self.percent_of_heterogeneous_queries)
        non_heterogeneous_inserts = updates-heterogeneous_inserts
        sequence.extend([2]*heterogeneous_inserts)
        sequence.extend([3]*non_heterogeneous_inserts)
        
        
        random.shuffle(sequence)


        queries = []
        for s in sequence:            
            if s<=1:
                test_obj = {} ##Will be null, tests are only for insertions
                obj = {}
                for i in range(2):                    
                    if s==0 or i ==1:
                        ## Generate a non-heterogeneous query                    
                        field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                        value = random.choice(list(domain_dict_nonheterogeneous[field]))
                    elif s==1:             
                        ## Generate a heterogeneous query                       
                        field = random.choice(list(domain_dict_heterogeneous.keys()))
                        value = random.choice(list(domain_dict_heterogeneous[field]))

                    if(field_types[field]=='date'):
                        value = value.isoformat()

                    if (field_types[field] == 'numeric' or field_types[field]=='date') and random.random() < 0.5: #50% of the time we will use a range query
                        r = random.random()                

                        if r < 0.5:
                            value = {'$gt':value}
                        else:
                            value = {'$lt':value}     

                    obj[field]  = value
            else:
                obj = {}
                test_obj = {}
                for field in domain_dict_heterogeneous.keys():
                    value = random.choice(list(domain_dict_heterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value
                for field in domain_dict_nonheterogeneous.keys():
                    value = random.choice(list(domain_dict_nonheterogeneous[field]))
                    if(field_types[field]=='date'):
                        value = value.isoformat()
                    obj[field] = value       

                heterogeneous_field = random.choice(list(domain_dict_heterogeneous.keys()))                
                test_obj[heterogeneous_field] = obj[heterogeneous_field]  ## Let's test in sequence if the queries envolving this heterogeneous fields were successfull

                non_heterogeneous_field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                test_obj[non_heterogeneous_field] = obj[non_heterogeneous_field]  ## Let's test in sequence if the queries envolving this non-heterogeneous fields were successfull

            queries.append((str(s),obj,test_obj))        
                
        with open(queries_file, 'w') as file:
            file.write('type;query;test_query\n')
            for query in queries:
                file.write(f'{query[0]};{query[1]};{query[2]}\n')
    
    def DecodeDateTime(self,empDict):
        for key, value in empDict.items():
            if isinstance(value, str) and re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '', value) == '':
                empDict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return empDict

    def EncodeDateTime(self, obj):
        if isinstance(obj, datetime):
            return obj.isoformat()
        raise TypeError("Type not serializable")

    def drop_database(self):
        client = MongoClient(self.host)
        client.drop_database(self.dbname)
        print('Database Dropped')

    def execute_queries(self):       
        queries_file = f'queries_{str(self.percent_of_heterogeneous_queries)}_{str(self.percent_of_insertions)}_{str(self.number_of_operations)}.txt'

        queries = pd.read_csv(self.csv_destination + queries_file, sep=';')

        time_taken = 0.0

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write(f'Test Start;{self.operation_mode};{self.method};{str(self.number_of_operations)};{str(self.percent_of_insertions)};{str(self.percent_of_heterogeneous_queries)};{str(self.execution_try)}\n')
            results_file.write('operation_mode;type;query;hashed_result\n')            
            for idx,row in queries.iterrows():
                print('Executing Query ' + str(idx))
                query = row['query']
                operation_type = row['type']
                if operation_type <=1:                    

                    try:
                        query_str = query.replace('\'','\"')                
                        query = json.loads(query_str.strip(), object_hook=self.DecodeDateTime)
                        start = time.time()
                        query_result = self.collection.find_many(query)
                        end = time.time()                        
                    except BaseException as e:
                        results_file.write('Error;' + query_str.strip() + '\n')
                        continue
                    
                    time_taken += (end-start)

                    if self.generate_hashes:
                        result = [{k: v for k, v in sorted(d.items()) if not k.startswith('_')} for d in query_result]
                        result_sorted = sorted(result, key=lambda x: json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in x.items()}, sort_keys=True))                
                        results_file.write(f'{self.operation_mode};query;{query_str.strip()};{str(hash(str(result_sorted)))};{str(end-start)}\n')                    
                    else:
                        results_file.write(f'{self.operation_mode};query;{query_str.strip()};;{str(end-start)}\n')                    

                    
                else:
                    print('Executing Insertion ' + str(idx))
                    test_query = row['test_query']

                    ##Insertion                    
                    try:
                        
                        query_str = query.replace('\'','\"')
                        query = json.loads(query_str.strip())
                        for key, value in query.items():
                            if isinstance(value, str) and re.match(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', value):
                                query[key] = datetime.fromisoformat(value)                                

                        start = time.time()                    
                        self.collection.insert_one(json.dumps(query, default=self.EncodeDateTime), query['RefDate'])                        
                        end = time.time()
                        time_taken += (end-start)
                        results_file.write(f'{self.operation_mode};insertion;{query_str.strip()};;{str(end-start)}\n')
                    except BaseException as e:
                        results_file.write('Error;' + query_str.strip() + '\n')
                        continue                    


                    # ## Test Query
                    # try:
                    #     test_query_str = test_query.replace('\'','\"')
                    #     test_query = json.loads(test_query_str.strip(), object_hook=self.DecodeDateTime)
                    #     result = self.collection.find_many(test_query)
                    #     if self.generate_hashes:                                                
                    #         result = [{k: v for k, v in sorted(d.items()) if not k.startswith('_')} for d in result]
                    #         result_sorted = sorted(result, key=lambda x: json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in x.items()}, sort_keys=True))
                    #         results_file.write(f'{self.operation_mode};insertion;{test_query_str.strip()};{str(hash(str(result_sorted)))};{str(end-start)}\n')
                    #     else:
                    #         results_file.write(f'{self.operation_mode};insertion;{test_query_str.strip()};;{str(end-start)}\n')
                    # except BaseException as e:
                    #     results_file.write('Error;' + query_str.strip() + '\n')

            results_file.write('Time Taken;' + str(time_taken) + '\n')
                



# host = 'localhost'
# method = 'operations_first'
# dbname = 'experimento_datasus'
# collectionname = 'db_experimento_datasus'
# source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/source/'
# date_columns = 'RefDate'
# csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/results/'
# operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/operations_cid9_cid10.csv'
# generate_hashes = False

# c = Comparator(host, 'preprocess', method, dbname, collectionname, source_folder, date_columns, csv_destination,operations_file, 100, 0.2, 0.05, 1, True, 'bla.txt', False)
# c.insert()   

host = 'localhost'
method = 'operations_first'
dbname = 'experimento_datasus'
collectionname = 'db_experimento_datasus'
source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/source/'
date_columns = 'RefDate'
csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/results/'
operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_2/operations_cid9_cid10.csv'
generate_hashes = False
core_index = True

rebuild = True

with open('experiment_log.txt','w') as log_file:
    for operation_mode in ['preprocess']:                   
        for execution_try in range(10):                          
            for number_of_operations in range(100, 1000, 100):     
                for percent_of_heterogeneous_queries in [0.15,0.3]:
                    for percent_of_insertions in [0,0.05,0.5,0.95,1]:                                   
                        output_file = f'results_{str(percent_of_heterogeneous_queries)}_{str(percent_of_insertions)}_{str(number_of_operations)}_{str(operation_mode)}_{str(execution_try)}.txt'

                        try:
                            log_file.write('Executing ' + output_file)
                            c = Comparator(host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination,operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions, execution_try, generate_hashes, output_file, core_index)

                            if rebuild:                                
                                log_file.write('Inserting Data\n')
                                log_file.flush()
                                c.insert()
                                rebuild = False                                                                                                   
                        
                            log_file.write('Generating Queries\n')
                            log_file.flush()
                            c.generate_queries_list() ## in the rewrite, we gonna use the same queries generated in the preprocess

                            log_file.write('Executing Queries\n')
                            log_file.flush()
                            c.execute_queries()                            
                            log_file.write(f'Finished {output_file}\n')
                            log_file.flush()
                            time.sleep(10)
                        except BaseException:
                            log_file.write('Error executing')
                            log_file.flush()                     

        rebuild = True
        c.drop_database()         
                                    
# # if __name__ == "__main__":
# #     run_experiment()