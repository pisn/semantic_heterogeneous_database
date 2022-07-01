from tests.test_base import TestBase
from datetime import datetime


class UngroupingTest(TestBase):
    def test_ungrouping_forward(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"X", "city":"Pi"}',datetime(1900,12,20))                
        collection.execute_operation('ungrouping',datetime(1930,6,20), {'fieldName':'city', 'oldValue':'Pi', 'newValues':['Q','W']})        
        
        count = collection.count_documents({'city':'Pi'})
        self.assertEqual(count,1,"Pi should still be acessible, resulting in one record")

        count = collection.count_documents({'city':'Q'})                
        self.assertEqual(count,1,"Pi has been ungrouped into Q and W. Therefore, this query should still bring the original record from P") 

        count = collection.count_documents({'city':'W'})                
        self.assertEqual(count,2,"Pi has been ungrouped into Q and W. Therefore, this query should still bring the original record from P") 

    def test_ungrouping_backward(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"C", "city":"Gr1"}',datetime(2000,6,14))        
        collection.insert_one('{"country":"C", "city":"Gr2"}',datetime(2000,6,14))        

        collection.execute_operation('ungrouping',datetime(1950,11,20), {'fieldName':'city', 'oldValue':'U', 'newValues':['Gr1','Gr2']})        

        count = collection.count_documents({'city':'U'})                
        self.assertEqual(count,2,"Searching for U should bring both Gr1 and Gr2 records, because they were later ungrouped to them") 

        count = collection.count_documents({'city':'Gr1'})                
        self.assertEqual(count,1,"Searching for Gr1 should still bring results") 

    def test_ungrouping_before_data(self):
        collection = self.BasicCollection
        collection.insert_one('{"country":"F", "city":"Hg1"}', datetime(1950,4,5))        
        collection.execute_operation('ungrouping',datetime(2009,11,20), {'fieldName':'city', 'oldValue':'Pa1', 'newValues':['Hg1','Hg2']})        

        count = collection.count_documents({'city':'Hg1'})
        self.assertEqual(count,1,'City Hg1 record should be untouched because its record is valid since before the ungrouping take place')

        count = collection.count_documents({'city':'Jr1'})
        self.assertEqual(count,0,'The original record was already valid when the ungrouping took place. No record should be returned')
