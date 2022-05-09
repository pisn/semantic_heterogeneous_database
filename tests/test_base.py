import unittest
from BasicCollection import BasicCollection
from mongomock import MongoClient

class TranslationBase(unittest.TestCase):

    def setUp(self):        
        self.BasicCollection = BasicCollection('test_database','test_collection','localhost',MongoClient())       
    
    