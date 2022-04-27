from Collection import Collection
import TranslationOperation
from datetime import datetime

class BasicCollection:
    def __init__ (self,DatabaseName, CollectionName, Host='localhost'):        
        self.collection = Collection(DatabaseName,CollectionName, Host)

        self.collection.register_operation('translation', TranslationOperation)

   
    def insert_one(self, JsonString, ValidFromDate:datetime):
        self.collection.insert_one(JsonString, ValidFromDate)
    
    def insert_many_by_csv(self, FilePath, ValidFromField, ValidFromDateFormat='%Y-%m-%d', Delimiter=','):
        self.collection.insert_many_by_csv(FilePath, ValidFromField, ValidFromDateFormat, Delimiter)
    
    ##Before executing the query itself, lets translate all possible past terms from the query to current terms. 
    ##We do translate registers, so we should also translate queries
    def find_many(self, QueryString):
        self.collection.find_many(QueryString)   

    def pretty_print(self, recordsCursor):
        self.collection.pretty_print(recordsCursor)