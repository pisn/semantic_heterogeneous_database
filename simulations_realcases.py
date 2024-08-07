import argparse
import os
import shutil
import time
import json
import random
import math
import pandas as pd
from pymongo import MongoClient
from database_generator import DatabaseGenerator
from semantic_heterogeneous_database import BasicCollection
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

operation_mode = 'preprocess'
method = 'insertion_first'
dbname = 'experimento_datasus'
collectionname = 'db_experimento_datasus'
source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/source/'
date_columns = 'ano'
csv_destination = 'teste.csv'
operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus/operations_cid9_cid10.csv'

if method != 'insertion_first' and method != 'operations_first':
    raise BaseException('Method not implemented')

host = 'localhost'
performance_results = pd.DataFrame()

def insert_first():    
    collection = BasicCollection(dbname, collectionname, host, operation_mode)
    temp_destination = source_folder + '/temp/'
    os.makedirs(temp_destination, exist_ok=True)

    start = time.time()
    for file in os.listdir(source_folder):
        # Check if the file is a CSV file
        if file.endswith('.csv'):
            # Print the full file path
            file_path = os.path.join(source_folder, file)
            file_size = os.path.getsize(file_path)
            max_file_size = 5 * 1024 * 1024  # 5Mb in bytes

            if file_size > max_file_size:
                # Divide the file into smaller files
                chunk_size = max_file_size
                with open(file_path, 'rb') as f:
                    header = f.readline()  # Read the first row of the original file
                    chunk = f.read(chunk_size)  # Read the rest of the chunk
                    chunk_number = 1
                    while chunk:
                        # Check if the chunk ends in the middle of a row
                        if chunk[-1] != b'\n':
                            # Find the last complete row in the chunk
                            last_row_index = chunk.rfind(b'\n')
                            # Trim the chunk to the last complete row
                            resto = chunk[last_row_index+1:]
                            chunk = chunk[:last_row_index+1]
                        # Write the chunk to a new file
                        new_file_path = os.path.join(temp_destination, f"{file}_{chunk_number}")
                        with open(new_file_path, 'wb') as new_file:
                            new_file.write(header)
                            new_file.write(chunk)
                        chunk_number += 1
                        # Read the next chunk
                        chunk = resto + f.read(chunk_size)
            else:
                shutil.copy2(file_path, temp_destination)

        
    for file in os.listdir(temp_destination):        
        # Insert the entire file
        print('Inserting file:', file)
        file_path = os.path.join(temp_destination, file)
        collection.insert_many_by_csv(file_path, date_columns)
        break

    shutil.rmtree(temp_destination)
    
    collection.collection.execute_many_operations_by_csv(operations_file, 'operation_type', 'valid_from')
    
    end = time.time()    

    ret = {
        'execution_time': (end-start)        
    }

    return ret

insert_first()

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
