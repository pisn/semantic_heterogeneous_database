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
    def __init__(self, host, operation_mode, method, dbname, collectionname, source_folder, date_columns, csv_destination, operations_file, output_file):
        self.operation_mode = operation_mode
        self.method = method
        self.dbname = dbname
        self.collectionname = collectionname
        self.source_folder = source_folder
        self.date_columns = date_columns
        self.csv_destination = csv_destination
        self.operations_file = operations_file
        self.host = host
        self.collection = BasicCollection(self.dbname, self.collectionname, self.host, self.operation_mode)
        self.output_file = output_file

        os.makedirs(csv_destination, exist_ok=True)
        

    def __insert_first(self):            
        start = time.time()

        self.collection.create_index([('cid',1),('municipio',1)])
        
        self.collection.insert_many_by_csv(self.source_folder, self.date_columns)        
        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')        
        
        end = time.time()    

        ret = {
            'execution_time': (end-start)        
        }

        with open(f'{self.csv_destination}{self.output_file}', 'a') as results_file:
            results_file.write('insertion;insertion_first;'+str(ret['execution_time'])+'\n')    

        return ret

    def __operations_first(self):
        start = time.time()
        
        self.collection.create_index([('cid',1),('municipio',1)])

        self.collection.execute_many_operations_by_csv(self.operations_file, 'operation_type', 'valid_from')
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



host = 'localhost'
# method = 'operations_first'
dbname = 'experimento_datasus'
collectionname = 'db_experimento_datasus'
source_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_operations_methods/source/'
date_columns = 'RefDate'
csv_destination = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_operations_methods/results/'
operations_file = '/home/pedro/Documents/USP/Mestrado/Pesquisa/experimentos_datasus_operations_methods/operations_cid9_cid10.csv'




i = 0

with open('experiment_log.txt','w') as log_file:    
    for method in ['insertion_first','operations_first']:
        for execution_try in range(10):                                                                                  
            output_file = f'results_operations_{method}_{str(execution_try)}.txt'

            try:
                log_file.write('Executing ' + output_file)
                c = Comparator(host, 'preprocess', method, dbname, collectionname, source_folder, date_columns, csv_destination,operations_file, output_file)                                           
                log_file.write('Inserting Data\n')
                log_file.flush()
                c.insert()                          
                log_file.write(f'Finished {output_file}\n')
                log_file.flush()
                time.sleep(10)
            except BaseException:
                log_file.write('Error executing')
                log_file.flush()                     
            c.drop_database()         
                                    
# # if __name__ == "__main__":
# #     run_experiment()