from Collection import Collection
from GroupingOperation import GroupingOperation
from TranslationOperation import TranslationOperation
from datetime import datetime

from UngroupingOperation import UngroupingOperation

class BasicCollection:
    def __init__ (self,DatabaseName, CollectionName, Host='localhost', client=None):        
        self.collection = Collection(DatabaseName,CollectionName, Host, client)

        self.collection.register_operation('translation', TranslationOperation(self))
        self.collection.register_operation('grouping', GroupingOperation(self))
        self.collection.register_operation('ungrouping', UngroupingOperation(self))

   
    def insert_one(self, JsonString, ValidFromDate:datetime):
        self.collection.insert_one(JsonString, ValidFromDate)
    
    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        self.collection.insert_many_by_csv(FilePath, ValidFromField, ValidFromDateFormat, Delimiter)
    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        return self.collection.find_many(QueryString)   

    def pretty_print(self, recordsCursor):
        return self.collection.pretty_print(recordsCursor)

    def execute_operation(self, operationType, validFrom, args):
        self.collection.execute_operation(operationType, validFrom, args)

    def execute_many_operations_by_csv(self, filePath, operationTypeColumn, validFromField):
        self.collection.execute_many_operations_by_csv(filePath, operationTypeColumn, validFromField)