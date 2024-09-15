import shutil
from .Collection import Collection
from .GroupingOperation import GroupingOperation
from .TranslationOperation import TranslationOperation
from datetime import datetime

from .UngroupingOperation import UngroupingOperation
import os

class BasicCollection:
    def __init__ (self,DatabaseName, CollectionName, Host='localhost', operation_mode='preprocess'):        
        if not isinstance(operation_mode, str) or operation_mode not in ['preprocess','rewrite']:
            raise BaseException('Operation Mode not recognized')

        self.operation_mode = operation_mode
        self.collection = Collection(DatabaseName,CollectionName, Host, operation_mode)

        self.collection.register_operation('translation', TranslationOperation(self))
        self.collection.register_operation('grouping', GroupingOperation(self))
        self.collection.register_operation('ungrouping', UngroupingOperation(self))

   
    def insert_one(self, JsonString, ValidFromDate:datetime):
        self.collection.insert_one(JsonString, ValidFromDate)

    def insert_many_by_dataframe(self, dataframe, ValidFromField):
        self.collection.insert_many_by_dataframe(dataframe, ValidFromField)
    
    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        source_folder = os.path.dirname(FilePath)

        temp_destination = source_folder + '/temp/'
        os.makedirs(temp_destination, exist_ok=True)
        
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
            self.collection.insert_many_by_csv(file_path, ValidFromField, ValidFromDateFormat, Delimiter)            

        shutil.rmtree(temp_destination)
        
    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        return self.collection.find_many(QueryString)   

    def count_documents(self, QueryString):
        return self.collection.count_documents(QueryString)

    def pretty_print(self, recordsCursor):
        return self.collection.pretty_print(recordsCursor)

    def execute_operation(self, operationType, validFrom, args):
        self.collection.execute_operation(operationType, validFrom, args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromField):
        self.collection.execute_many_operations_by_csv(filePath, operationTypeColumn, validFromField)    