from .test_base import TestBase
from datetime import datetime

class UngroupingSusTest(TestBase):

    def test_translation_sus_preprocess(self):
        self.execute_test(self.SusCollection)        
    
    def test_translation_sus_rewrite(self):
        self.execute_test(self.SusCollection_rewrite)
    
    def execute_test(self, collection):               
        collection.execute_operation('ungrouping',datetime(1996,1,1), {'fieldName':'cid','oldValue':'08-14 Neoplasmas malignos', 'newValues':['036 Neopl malig do fígado e vias bil intrahepát','046 Neoplasia maligna da bexiga','048 Linfoma não-Hodgkin','049 Mieloma mult e neopl malig de plasmócitos','040 Neoplasia maligna da pele','044 Neoplasia maligna do ovário','052 Restante de neoplasias malignas','032-052 NEOPLASIAS']})
        
        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'08-14 Neoplasmas malignos','ano':1995})
        query = list(query)
        self.assertEqual(query[0]['ocorrencias'],3453,"Os 3453 óbitos por neoplasias em 1995 deveriam continuar aparecendo (query original)")        

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'08-14 Neoplasmas malignos','ano':1996})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),2905,"Os 2905 óbitos do desagrupamento somado deveria aparecer (query original)")

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'049 Mieloma mult e neopl malig de plasmócitos','ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Não é possível desagrupar o passado, não deveria trazer nenhum resultado (query desagrupada)")        

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'049 Mieloma mult e neopl malig de plasmócitos','ano':1996})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),125,"Os 125 óbitos por Mieloma Múltiplo deveriam aparecer (query original)")