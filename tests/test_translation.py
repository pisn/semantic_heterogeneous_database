import unittest
from mongomock import MongoClient

class TranslationTest(unittest.TestCase):

    def setUp(self):        
        self.mocked_collection = MongoClient().db.collection
    
    def test_translation_before_data(self):
        self.mocked_collection.insert_one({'teste1':'bb'})
        count = self.mocked_collection.count_documents({'teste1':'ab'})

        self.assertEqual(count,1,"Deu merda aqui oh") 


if __name__ == "__main__":
    unittest.main()     