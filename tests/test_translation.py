from .test_base import TranslationBase
from datetime import datetime

class TranslationTest(TranslationBase):

    def test_translation_forward(self):       
        collection = self.BasicCollection
        collection.insert_one('{"country":"Russia", "city":"Leningrad"}',datetime(1923,12,20))        
        collection.execute_operation('translation',datetime(1924,1,26), {'fieldName':'city', 'oldValue':'Leningrad', 'newValue':'Saint Petesburg'})        
        
        count = collection.count_documents({'city':'Saint Petesburg'})
        self.assertEqual(count,1,"Saint Petesburg should have been translated from Leningrad row")

        count = collection.count_documents({'city':'Leningrad'})                
        self.assertEqual(count,1,"Leningrad must still bring results in the query") 

    def test_translation_backwards(self):       
        collection = self.BasicCollection
        collection.insert_one('{"country":"Brazil", "city":"Ouro Preto"}',datetime(2000,12,31))        
        collection.execute_operation('translation',datetime(1924,1,26), {'fieldName':'city', 'oldValue':'Vila Rica', 'newValue':'Ouro Preto'})        
        
        count = collection.count_documents({'city':'Vila Rica'})
        self.assertEqual(count,1,"Ouro Preto should have been translated from Vila Rica row")

        count = collection.count_documents({'city':'Ouro Preto'})                
        self.assertEqual(count,1,"Ouro Preto must still bring results in the query") 

    def test_translation_before_data(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"A", "city":"Name B"}',datetime(2000,12,31))        
        collection.execute_operation('translation',datetime(2003,5,26), {'fieldName':'city', 'oldValue':'Name A', 'newValue':'Name B'})        
        
        count = collection.count_documents({'city':'Name B'})
        self.assertEqual(count,1,'Name B should be untouched because its record is valid since before the translation take place')

        count = collection.count_documents({'city':'Name A'})                
        self.assertEqual(count,0,"The original record was already valid when the translation was done. No record should match the Name A") 