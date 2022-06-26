import unittest
from BasicCollection import BasicCollection

class TranslationBase(unittest.TestCase):

    def setUp(self):                
        self.BasicCollection = BasicCollection('test_database','test_collection','localhost')       
        self.BasicCollection.collection.client.drop_database('test_database') ##This ensures to drop the test database if already existed
        self.BasicCollection = BasicCollection('test_database','test_collection','localhost')       

    def tearDown(self):
        self.BasicCollection.collection.client.drop_database('test_database')
    
    