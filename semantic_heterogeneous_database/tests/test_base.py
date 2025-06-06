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

        self.SusCollection_rewrite = BasicCollection('sus_test_rewrite','sus_test_rewrite','localhost','rewrite')       
        self.SusCollection_rewrite.collection.client.drop_database('sus_test_rewrite') ##This ensures to drop the test database if already existed
        self.SusCollection_rewrite = BasicCollection('sus_test_rewrite','sus_test_rewrite','localhost','rewrite')   


        sus_folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/mortalidade_ano/'
        self.SusCollection.insert_many_by_csv(sus_folder, 'RefDate')
        self.SusCollection_rewrite.insert_many_by_csv(sus_folder, 'RefDate')
      
    def tearDown(self):
        self.BasicCollection.collection.client.drop_database('test_database')
        self.SusCollection.collection.client.drop_database('sus_test')
        self.SusCollection_rewrite.collection.client.drop_database('sus_test_rewrite')
    
    