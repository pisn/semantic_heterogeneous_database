from .test_base import TestBase
from datetime import datetime

class TranslationSusTest(TestBase):

    def test_translation_sus(self):       
        collection = self.SusCollection        
        collection.execute_operation('translation',datetime(1996,1,1), {'fieldName':'cid', 'oldValue':'191 Marasmo nutricional', 'newValue':'056 Desnutrição'})        
        
        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'191 Marasmo nutricional','ano':1995})        
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Marasmo Nutricional em 1995 deveriam continuar aparecendo (query original)")

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'056 Desnutrição','ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Marasmo Nutricional em 1995 deveriam continuar aparecendo (query com nome novo)")

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'056 Desnutrição','ano':1996})        
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam continuar aparecendo (query original)")
        
        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'191 Marasmo nutricional','ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query com nome antigo)")    

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Marasmo'},'ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome antigo)")    

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Desnutri'},'ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome novo)")         

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Marasmo'},'ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome antigo)")

        query = self.SusCollection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Desnutri'},'ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome novo)")

        