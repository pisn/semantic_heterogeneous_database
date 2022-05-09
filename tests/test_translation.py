import unittest
from mongomock import MongoClient
from .test_base import TranslationBase

class TranslationTest(TranslationBase):

    def test_translation_before_data(self):       

        self.assertEqual(0,1,"Deu merda aqui oh") 