from .test_base import TestBase
from datetime import datetime

class SusTest(TestBase):

    def test_translation_sus_preprocess(self):
        self.execute_test_translation(self.SusCollection)        
    
    def test_translation_sus_rewrite(self):
        self.execute_test_translation(self.SusCollection_rewrite)

    def execute_test_translation(self, collection):               
        collection.execute_operation('translation',datetime(1996,1,1), {'fieldName':'cid', 'oldValue':'191 Marasmo nutricional', 'newValue':'056 Desnutrição'})        
        collection.execute_operation('translation',datetime(1996,1,1), {'fieldName':'cid', 'oldValue':'038 Septicemia', 'newValue':'014 Septicemia'})        
        
        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'191 Marasmo nutricional','ano':1995})        
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Marasmo Nutricional em 1995 deveriam continuar aparecendo (query original)")

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'056 Desnutrição','ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Marasmo Nutricional em 1995 deveriam continuar aparecendo (query com nome novo)")

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'056 Desnutrição','ano':1996})        
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam continuar aparecendo (query original)")
        
        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'191 Marasmo nutricional','ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query com nome antigo)")    

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$in':['191 Marasmo nutricional','014 Septicemia']},'ano':1996})
        self.assertEqual(sum([q['ocorrencias'] for q in query]),314+465,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (nome antigo) junto com os 465 por septicemia")    

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$in':['191 Marasmo nutricional','014 Septicemia']},'ano':1995})
        self.assertEqual(sum([q['ocorrencias'] for q in query]),14+392,"Os 14 óbitos por Desnutrição em 1995 deveriam aparecer (nome novo) junto com os 392 por septicemia")    

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Marasmo'},'ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome antigo)")    

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Desnutri'},'ano':1996})
        self.assertEqual(query[0]['ocorrencias'],314,"Os 314 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome novo)")         

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Marasmo'},'ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome antigo)")

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':{'$regex':'Desnutri'},'ano':1995})
        self.assertEqual(query[0]['ocorrencias'],14,"Os 14 óbitos por Desnutrição em 1996 deveriam aparecer (query por pattern pelo nome novo)")

    def test_grouping_sus_preprocess(self):
        self.execute_test_grouping(self.SusCollection)        
    
    def test_grouping_sus_rewrite(self):
        self.execute_test_grouping(self.SusCollection_rewrite)

    def execute_test_grouping(self, collection):               
        collection.execute_operation('grouping',datetime(1996,1,1), {'fieldName':'cid', 'oldValues':['012 Shiguelose','013 Intoxicações alimentares','014 Amebíase','015 Infecções intest.dev.a outr.microorg.espec.','016 Infecções intestinais mal definidas'], 'newValue':'004 Outras doenças infecciosas intestinais'})
        
        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'004 Outras doenças infecciosas intestinais','ano':1996})        
        self.assertEqual(query[0]['ocorrencias'],7,"Os 7 óbitos por Outras doenças Intestinais em 1996 deveriam continuar aparecendo (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'013 Intoxicações alimentares','ano':1996})  
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Os 7 óbitos por Outras doenças Intestinais em 1996 nao devem aparecer, porque não se pode desagrupar o futuro (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'016 Infecções intestinais mal definidas','ano':1996})  
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Os 7 óbitos por Outras doenças Intestinais em 1996 nao devem aparecer, porque não se pode desagrupar o futuro (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'004 Outras doenças infecciosas intestinais','ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),186,"Os 184 óbitos por Infecções intestinais mal definidas + 2 por Intoxicações alimentares em 1995 deveriam ser informadas (query com nome novo)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':{'$regex':'intestinais'},'ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),186,"Os 184 óbitos por Infecções intestinais mal definidas + 2 por Intoxicações alimentares em 1995 deveriam ser informadas (query com nome novo)")

    def test_ungrouping_sus_preprocess(self):
        self.execute_test_ungrouping(self.SusCollection)        
    
    def test_ungrouping_sus_rewrite(self):
        self.execute_test_ungrouping(self.SusCollection_rewrite)
    
    def execute_test_ungrouping(self, collection):               
        collection.execute_operation('ungrouping',datetime(1996,1,1), {'fieldName':'cid','oldValue':'08-14 Neoplasmas malignos', 'newValues':['036 Neopl malig do fígado e vias bil intrahepát','046 Neoplasia maligna da bexiga','048 Linfoma não-Hodgkin','049 Mieloma mult e neopl malig de plasmócitos','040 Neoplasia maligna da pele','044 Neoplasia maligna do ovário','052 Restante de neoplasias malignas','032-052 NEOPLASIAS']})
        
        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'08-14 Neoplasmas malignos','ano':1995})
        self.assertEqual(query[0]['ocorrencias'],3453,"Os 3453 óbitos por neoplasias em 1995 deveriam continuar aparecendo (query original)")        

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'08-14 Neoplasmas malignos','ano':1996})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),2905,"Os 2905 óbitos do desagrupamento somado deveria aparecer (query original)")

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'049 Mieloma mult e neopl malig de plasmócitos','ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Não é possível desagrupar o passado, não deveria trazer nenhum resultado (query desagrupada)")        

        query = collection.find_many({'UF':'SP','municipio':'355030 SAO PAULO','cid':'049 Mieloma mult e neopl malig de plasmócitos','ano':1996})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),125,"Os 125 óbitos por Mieloma Múltiplo deveriam aparecer (query original)")