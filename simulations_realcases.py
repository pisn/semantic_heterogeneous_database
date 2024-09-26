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
operation_mode = 'preprocess'
method = 'insertion_first'
dbname = 'experimento_datasus_2'
collectionname = 'db_experimento_datasus'
source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/source/'
date_columns = 'ano'
csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/results/'
operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/operations_cid9_cid10.csv'
number_of_operations = 100
percent_of_heterogeneous_queries = 0.3
percent_of_insertions = 0.3

# if method != 'insertion_first' and method != 'operations_first':
#     raise BaseException('Method not implemented')

host = 'localhost'
performance_results = pd.DataFrame()

class Comparator:
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions, execution_try, generate_hashes):
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
        self.output_file = 'result_' + str(uuid.uuid4()) + '.txt'
        self.generate_hashes = generate_hashes

        os.makedirs(csv_destination, exist_ok=True)

    def insert_first(self):            
        start = time.time()
        
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)        
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write('insertion;insertion_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def operations_first(self):
        start = time.time()
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')

        for file in os.listdir(self.source_folder):
            # Check if the file is a CSV file
            if file.endswith('.csv'):
                # Print the full file path
                file_path = os.path.join(self.source_folder, file)
                self.collection.insert_many_by_csv(file_path, self.date_columns)           
        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }
        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write('insertion;operations_first;'+str(ret['execution_time'])+'\n')    

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
        
        self.queries = queries
        queries_file = self.csv_destination + 'queries.csv'
        with open(queries_file, 'w') as file:
            file.write('type;query;test_query\n')
            for query in queries:
                file.write(f'{query[0]};{query[1]};{query[2]}\n')
    
    def DecodeDateTime(self,empDict):
        for key, value in empDict.items():
            if isinstance(value, str) and re.sub(r'\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}', '', value) == '':
                empDict[key] = datetime.strptime(value, '%Y-%m-%dT%H:%M:%S')
        return empDict

    def drop_database(self):
        client = MongoClient(self.host)
        client.drop_database(self.dbname)
        print('Database Dropped')

    def execute_queries(self):       
        queries = pd.read_csv(self.csv_destination + 'queries.csv', sep=';')

        time_taken = 0.0

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write(f'Test Start;{self.operation_mode};{self.method};{str(self.number_of_operations)};{str(self.percent_of_insertions)};{str(self.percent_of_heterogeneous_queries)};{str(self.execution_try)}\n')
            results_file.write('operation_mode;type;query;hashed_result\n')            
            for idx,row in queries.iterrows():
                print('Executing Query ' + str(idx))
                query = row['query']
                operation_type = row['type']
                if operation_type <=1:
                    query_str = query.replace('\'','\"')                
                    query = json.loads(query_str.strip(), object_hook=self.DecodeDateTime)

                    start = time.time()
                    query_result = self.collection.find_many(query)
                    end = time.time()
                    time_taken += (end-start)

                    if self.generate_hashes:
                        result = [{k: v for k, v in sorted(d.items()) if not k.startswith('_')} for d in query_result]
                        result_sorted = sorted(result, key=lambda x: json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in x.items()}, sort_keys=True))                
                        results_file.write(f'{self.operation_mode};query;{query_str.strip()};{str(hash(str(result_sorted)))}\n')                    
                    else:
                        results_file.write(f'{self.operation_mode};query;{query_str.strip()};\n')                    

                    
                else:
                    print('Executing Insertion ' + str(idx))
                    test_query = row['test_query']

                    query_str = query.replace('\'','\"')
                    query = json.loads(query_str.strip(), object_hook=self.DecodeDateTime)

                    ##Insertion

                    start = time.time()
                    self.collection.insert_one(json.dumps(query, default=str), query['ano'])
                    end = time.time()
                    time_taken += (end-start)

                    ## Test Query
                    test_query_str = test_query.replace('\'','\"')
                    test_query = json.loads(test_query_str.strip(), object_hook=self.DecodeDateTime)

                    if self.generate_hashes:
                        result = [{k: v for k, v in sorted(d.items()) if not k.startswith('_')} for d in self.collection.find_many(test_query)]
                        result_sorted = sorted(result, key=lambda x: json.dumps({k: (v.isoformat() if isinstance(v, datetime) else v) for k, v in x.items()}, sort_keys=True))
                        results_file.write(f'{self.operation_mode};insertion;{test_query_str.strip()};{str(hash(str(result_sorted)))}\n')
                    else:
                        results_file.write(f'{self.operation_mode};insertion;{test_query_str.strip()};\n')

            results_file.write('Time Taken;' + str(time_taken) + '\n')
                




c = Comparator(host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, number_of_operations, percent_of_heterogeneous_queries, percent_of_insertions,1, False)
#c.insert_first()
#c.generate_queries_list()
c.execute_queries()
#insert_first()
#c.drop_database()
