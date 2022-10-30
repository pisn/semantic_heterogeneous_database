import argparse, sys
import time
import json
import random
import math
import pandas as pd
from pymongo import MongoClient
from database_generator import DatabaseGenerator
pd.options.mode.chained_assignment = None  # default='warn'


parser=argparse.ArgumentParser()

parser.add_argument("--records")
parser.add_argument("--versions")
parser.add_argument("--fields")
parser.add_argument("--domain")
parser.add_argument("--repetitions")
parser.add_argument("--method")
parser.add_argument("--update_percent")
parser.add_argument("--destination")

args=parser.parse_args()

number_of_records = int(args.records)
number_of_versions = int(args.versions)
number_of_fields = int(args.fields)
number_of_values_in_domain=int(args.domain)
number_of_tests = int(args.repetitions)
update_percent = float(args.update_percent)
method = args.method
csv_destination = args.destination

if method != 'insertion_first' and method != 'operations_first':
    raise BaseException('Method not implemented')

host = 'localhost'
performance_results = pd.DataFrame()

def insert_first():    
    d = DatabaseGenerator()
    d.generate(number_of_records=number_of_records, number_of_versions=1, number_of_fields=number_of_fields,number_of_values_in_domain=number_of_values_in_domain)
    records = pd.DataFrame(d.records)

    start = time.time()
    d.collection.insert_many_by_dataframe(records, 'valid_from_date')

    for i in range(number_of_versions):
        d.generate_version()        
    
    for operation in d.operations:    
        d.collection.execute_operation(operation[0],operation[1],operation[2])
    
    end = time.time()    

    ret = {
        'execution_time': (end-start),
        'generator': d
    }
    return ret

def operations_first():    
    d = DatabaseGenerator()
    d.generate(number_of_records=number_of_records, number_of_versions=1, number_of_fields=number_of_fields,number_of_values_in_domain=number_of_values_in_domain)
    records = pd.DataFrame(d.records)

    start = time.time()
    d.collection.insert_many_by_dataframe(records.head(10), 'valid_from_date') #initial insert

    for i in range(number_of_versions):
        d.generate_version()        
    
    for operation in d.operations:          
        d.collection.execute_operation(operation[0],operation[1],operation[2])

    d.collection.insert_many_by_dataframe(records.head(-10), 'valid_from_date')
    
    end = time.time()

    ret = {
        'execution_time': (end-start),
        'generator': d
    }
    return ret

def update_and_read_test(percent_of_update, insert_first_selected):
    ### Generate database just as before    

    if insert_first_selected:
        r = insert_first()   
    else:
        r = operations_first() 
    
    original_records = r['generator'].records.copy()

    updates = math.floor(100*percent_of_update)
    reads = 100-updates

    sequence = ([True]*updates)
    sequence.extend([False]*reads)
    random.shuffle(sequence)    

    records = [r['generator'].generate_record() for i in range(updates)]
    records_2 = records.copy()

    queries = []   

    for i in range(reads):        
        field = (random.choice(r['generator'].fields))[0]
        value = random.choice(r['generator'].field_domain[field])
        queries.append({field:value})   

    queries_2 = queries.copy()     

    start = time.time()
    for operation in sequence:             
        if operation: 
            record = records.pop()            
            r['generator'].collection.insert_one(json.dumps(record, default=str),record['valid_from_date'])                                    
        else:            
            r['generator'].collection.find_many(queries.pop())                     

    end = time.time()      
        
    
    operations_time = end-start
    r['generator'].destroy()

    client = MongoClient(host)        
    db = client[r['generator'].database_name]
    base_collection = db[r['generator'].collection_name]

    base_collection.insert_many(original_records)

    start = time.time()    
    for operation in sequence:             
        if operation: 
            record = records_2.pop()
            base_collection.insert_one(record)                      
        else:
            base_collection.find(queries_2.pop()) ##Isso nao faz exatamente sentido. Deveria gerar uma nova query 
    end = time.time()    
    baseline_time = (end-start)
    client.drop_database(r['generator'].database_name)
    
    return ({'insertion_phase': r['execution_time'],'operations_phase': operations_time, 'operations_baseline': baseline_time})
    

for i in range(number_of_tests):
    tests_result = update_and_read_test(update_percent, method == 'insertion_first')
    d = {
        'insert_first': method,         
        'update_percent': update_percent,
        'insertion_phase': tests_result['insertion_phase'],
        'operations_baseline' : tests_result['operations_baseline'],
        'operations_phase':tests_result['operations_phase']
    }
    print(d)
    performance_results = performance_results.append(d, ignore_index=True)  

performance_results.to_csv(csv_destination)