<<<<<<< HEAD
from .test_base import TranslationBase
from datetime import datetime

class GroupingTest(TranslationBase):

    pass
=======
from .test_base import TestBase
from datetime import datetime

class GroupingTest(TestBase):
    def test_grouping_forward(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"F", "city":"G"}',datetime(1900,12,20))        
        collection.insert_one('{"country":"F", "city":"H"}',datetime(1910,12,20))        
        collection.execute_operation('grouping',datetime(1930,6,20), {'fieldName':'city', 'oldValues':['G','H'], 'newValue':'K'})        
        
        count = collection.count_documents({'city':'K'})
        self.assertEqual(count,2,"G and H should now be grouped into K city, resulting in two records")

        count = collection.count_documents({'city':'G'})                
        self.assertEqual(count,2,"G should still be acessible, but grouped into K. City H record should also appear here.") 

        count = collection.count_documents({'city':'H'})                
        self.assertEqual(count,2,"H should still be acessible, but grouped into K. City G record should also appear here.") 

    def test_grouping_backward(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"F", "city":"Z"}',datetime(2000,6,14))        

        collection.execute_operation('grouping',datetime(1950,11,20), {'fieldName':'city', 'oldValues':['Y','U'], 'newValue':'Z'})        

        count = collection.count_documents({'city':'Y'})                
        self.assertEqual(count,1,"Searching for Y should bring the Z record, because it was later grouped into Z") 

        count = collection.count_documents({'city':'Z'})                
        self.assertEqual(count,1,"Searching for Z should still bring results") 

    def test_grouping_before_data(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"E", "city":"J"}', datetime(1950,4,5))
        collection.execute_operation('grouping',datetime(2009,11,20), {'fieldName':'city', 'oldValues':['Je1','Je2'], 'newValue':'J'})        

        count = collection.count_documents({'city':'J'})
        self.assertEqual(count,1,'City J record should be untouched because its record is valid since before the grouping take place')

        count = collection.count_documents({'city':'Je1'})
        self.assertEqual(count,0,'The original record was already valid when the grouping took place. No record should be returned')

        
>>>>>>> df7216111a1e335aea644596b7636b5a0168cc72
