from .test_base import TranslationBase
from datetime import datetime

class TranslationTest(TranslationBase):

    def test_translation_forward(self):       
        collection = self.BasicCollection
        collection.insert_one('{"country":"Germany", "city":"Leningrad"}',datetime(1923,12,20))        
        collection.execute_operation('translation',datetime(1924,1,26), {'fieldName':'city', 'oldValue':'Leningrad', 'newValue':'Saint Petesburg'})        
        
        count = collection.count_documents({'city':'Saint Petesburg'})
        self.assertEqual(count,1,"Saint Petesburg should have been translated from Leningrad row")

        count = collection.count_documents({'city':'Leningrad'})                
        self.assertEqual(count,1,"Leningrad must still bring results in the query") 