import argparse
import os
import shutil
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

operation_mode = 'rewrite'
#operation_mode = 'preprocess'
method = 'insertion_first'
dbname = 'experimento_datasus_1'
collectionname = 'db_experimento_datasus'
source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/source/'
date_columns = 'ano'
csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/results/'
operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/operations_cid9_cid10.csv'
number_of_operations = 100
percent_of_heterogeneous_queries = 0.3
percent_of_insertions = 0.3

if method != 'insertion_first' and method != 'operations_first':
    raise BaseException('Method not implemented')

host = 'localhost'
performance_results = pd.DataFrame()

class Comparator:
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions):
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
        self.host = host
        self.collection = BasicCollection(self.dbname, self.collectionname, self.host, self.operation_mode)

        os.makedirs(csv_destination, exist_ok=True)

    def insert_first(self):            
        start = time.time()
        for file in os.listdir(self.source_folder):
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                # Print the full file path
                file_path = os.path.join(source_folder, file)
                self.collection.insert_many_by_csv(file_path, date_columns)    
        
        self.collection.execute_many_operations_by_csv(operations_file, 'operation_type', 'valid_from')
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        return ret

    def operations_first(self):
        start = time.time()
        self.collection.execute_many_operations_by_csv(operations_file, 'operation_type', 'valid_from')

        for file in os.listdir(self.source_folder):
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                # Print the full file path
                file_path = os.path.join(source_folder, file)
                self.collection.insert_many_by_csv(file_path, date_columns)           
        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        return ret

    
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
                if s==1:             
                    ## Generate a heterogeneous query   
                    field = random.choice(list(domain_dict_heterogeneous.keys()))
                    value = random.choice(list(domain_dict_nonheterogeneous[field]))
                elif s==0:
                    ## Generate a non-heterogeneous query
                    field = random.choice(list(domain_dict_nonheterogeneous.keys()))
                    value = random.choice(list(domain_dict_nonheterogeneous[field]))

                if(field_types[field]=='date'):
                    value = value.isoformat()

                if (field_types[field] == 'numeric' or field_types[field]=='date') and random.random() < 0.5: #50% of the time we will use a range query
                    r = random.random()                

                    if r < 0.5:
                        value = {'$gt':value}
                    else:
                        value = {'$lt':value}     

                obj = {field:value}        
            else:
                obj = {}
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

            queries.append((str(s),obj))
        
        self.queries = queries
        queries_file = csv_destination + 'queries.txt'
        with open(queries_file, 'w') as file:
            file.write('type' + ';' + 'query' + '\n')
            for query in queries:
                file.write(f'{query[0]};{query[1]}\n')
    
    def DecodeDateTime(self,empDict):
        for key, value in empDict.items():
            if isinstance(value, str) and re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '', value) == '':
                empDict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return empDict

    def execute_queries(self):       
        queries = pd.read_csv(csv_destination + 'queries.txt', sep=';')

        with open(f'{csv_destination}results_{self.operation_mode}.txt', 'w') as results_file:
            results_file.write('operation_mode;query;hashed_result\n')            
            for idx,row in queries.iterrows():
                query = row['query']
                operation_type = row['type']
                if operation_type <=1:
                    query_str = query.replace('\'','\"')                
                    query = json.loads(query_str.strip(), object_hook=self.DecodeDateTime)
                    result = [{k: v for k, v in sorted(d.items()) if not k.startswith('_')} for d in self.collection.find_many(query)]
                    result_sorted = sorted(result, key=lambda x: json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in x.items()}, sort_keys=True))                

                    results_file.write(f'{self.operation_mode};{query_str.strip()};{str(hash(str(result_sorted)))}\n')
                    results_file.flush()




c = Comparator(host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions)
#c.insert_first()
c.generate_queries_list()
c.execute_queries()
#
#insert_first()

# def operations_first():    
#     d = DatabaseGenerator()
#     print('Generating Records')
#     d.generate(number_of_records=number_of_records, number_of_versions=1, number_of_fields=number_of_fields,number_of_values_in_domain=number_of_values_in_domain,number_of_evolution_fields=2, operation_mode=operation_mode)
#     records = pd.DataFrame(d.records)

#     start = time.time()    
#     for i in range(number_of_versions):
#         print('Generating Version')
#         d.generate_version()        
    
#     for operation in d.operations: 
#         print('Executing version operations')       
#         print(f'OperationType:{str(operation[0])} - ValidFrom:{str(operation[1])} - Args:{str(operation[2])}')  
#         d.collection.execute_operation(operation[0],operation[1],operation[2])   
    
#     print('Inserting Records')
#     d.collection.insert_many_by_dataframe(records, 'valid_from_date')
#     end = time.time()    

#     ret = {
#         'execution_time': (end-start),
#         'generator': d
#     }
#     return ret




# def update_and_read_test(percent_of_update, insert_first_selected):
#     ### Generate database just as before    
    
#     print('Generating Database')
#     if insert_first_selected:
#         r = insert_first()   
#     else:
#         r = operations_first() 

#     print('Database Generated')
    
#     original_records = r['generator'].records.copy()

#     updates = math.floor(number_of_operations*percent_of_update)
#     reads = number_of_operations-updates

#     sequence = ([True]*updates)
#     sequence.extend([False]*reads)
#     random.shuffle(sequence)    

#     records = [r['generator'].generate_record() for i in range(updates)]
#     records_2 = records.copy()

#     queries = []   

#     for i in range(reads):        
#         field = (random.choice(r['generator'].fields))[0]
#         value = random.choice(r['generator'].field_domain[field])
#         queries.append({field:value})   

#     queries_2 = queries.copy()     

#     print('Executing operations')
#     start = time.time()
#     for operation in sequence:             
#         if operation: 
#             record = records.pop()            
#             r['generator'].collection.insert_one(json.dumps(record, default=str),record['valid_from_date'])                                    
#         else:            
#             r['generator'].collection.find_many(queries.pop())                     

#     end = time.time()      
        
#     print('Operations Executed')

#     operations_time = end-start
#     r['generator'].destroy()

#     client = MongoClient(host)        
#     db = client[r['generator'].database_name]
#     base_collection = db[r['generator'].collection_name]

#     base_collection.insert_many(original_records)

#     start = time.time()    
#     for operation in sequence:             
#         if operation: 
#             record = records_2.pop()
#             base_collection.insert_one(record)                      
#         else:
#             base_collection.find(queries_2.pop()) ##Isso nao faz exatamente sentido. Deveria gerar uma nova query 
#     end = time.time()    
#     baseline_time = (end-start)
#     client.drop_database(r['generator'].database_name)
    
#     return ({'insertion_phase': r['execution_time'],'operations_phase': operations_time, 'operations_baseline': baseline_time})
    

# for i in range(number_of_tests):
#     print('Starting test ' + str(i))
#     tests_result = update_and_read_test(update_percent, method == 'insertion_first')    
#     d = {
#         'number_of_records':number_of_records,
#         'number_of_versions':number_of_versions,
#         'number_of_fields':number_of_fields,
#         'number_of_values_in_domain':number_of_values_in_domain,
#         'number_of_tests':number_of_tests,
#         'number_of_evolution_fields':number_of_evolution_fields,
#         'number_of_operations':number_of_operations,
#         'update_percent':update_percent,
#         'operation_mode':operation_mode,
#         'method':method,
#         'insertion_phase': tests_result['insertion_phase'],
#         'operations_baseline' : tests_result['operations_baseline'],
#         'operations_phase':tests_result['operations_phase']
#     }
#     print(d)
#     performance_results = performance_results.append(d, ignore_index=True)  

# performance_results.to_csv(csv_destination)
