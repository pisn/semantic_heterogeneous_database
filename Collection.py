import pandas as pd

class Collection:
    def __init__ (self):
        self.collections = {}
        self.operations = {}

    def insert_one(self, jsonString, valid_from_date:datetime):
        """ Insert one documet in the collection.

        Args:
            jsonString(): string of the json representation of the document being inserted.
            valid_from_date(): date of when the register becomes valid. This is important in the treatment of semantic changes along time
        
        """
        pass

    def __insert_one_by_version(self, jsonString, versionNumber, validFromDate:datetime):

        pass

    def insert_many_by_csv(self, filePath, valid_from_field, valid_from_date_format='%Y-%m-%d', delimiter=','):
        """ Insert many records in the collection using a csv file. 

        Args: 
            filePath (): path to the csv file
            valid_from_field (): csv field which should be used to determine the date of when the register becomes valid. This is important in the treatment of semantic changes along time
            valid_from_date_format (): the expected format of the date contained in the valid_from_field values
            delimiter (): delimiter used in the file. 

        """

    def __insert_many_by_version(self, group: pd.DataFrame, version_numbr:int, valid_from_date:datetime):

        pass

    def find_many(self, queryString):
        """ Query the collection with the supplied queryString

        Args:
            queryString(): string of the query in json. The syntax is the same as the pymongo syntax.
        
        Returns: a cursor for the records returned by the query

        """

        pass

    def pretty_print(self, recordsCursor):
        """ Pretty print the records in the console. Semantic changes will be presented in the format "CurrentValue (originally: OriginalValue)"

        Args:
            recordsCursor(): the cursor for the records, usually obtained through the find functions
        
        """
        