import unittest
from ..BasicCollection import BasicCollection

class TestBase(unittest.TestCase):

    def setUp(self):                
        self.BasicCollection = BasicCollection('test_database','test_collection','localhost')       
        self.BasicCollection.collection.client.drop_database('test_database') ##This ensures to drop the test database if already existed
        self.BasicCollection = BasicCollection('test_database','test_collection','localhost')       

        self.SusCollection = BasicCollection('sus_test','sus_test','localhost')       
        self.SusCollection.collection.client.drop_database('sus_test') ##This ensures to drop the test database if already existed
        self.SusCollection = BasicCollection('sus_test','sus_test','localhost')   

        sus_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/mortalidade_ano/'
        self.SusCollection.insert_many_by_csv(sus_folder + '/' + 'mortalidade_9_processed_1995.csv', 'RefDate')
        self.SusCollection.insert_many_by_csv(sus_folder + '/' + 'mortalidade_10_processed_1996.csv', 'RefDate')

    def tearDown(self):
        self.BasicCollection.collection.client.drop_database('test_database')
        self.SusCollection.collection.client.drop_database('sus_test')
    
    