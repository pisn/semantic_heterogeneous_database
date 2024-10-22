from .test_base import TestBase
from datetime import datetime

class GroupingSusTest(TestBase):
    def test_grouping_sus_preprocess(self):
        self.execute_test(self.SusCollection)        
    
    def test_grouping_sus_rewrite(self):
        self.execute_test(self.SusCollection_rewrite)

    def execute_test(self, collection):               
        collection.execute_operation('grouping',datetime(1996,1,1), {'fieldName':'cid', 'oldValues':['012 Shiguelose','013 Intoxicações alimentares','014 Amebíase','015 Infecções intest.dev.a outr.microorg.espec.','016 Infecções intestinais mal definidas'], 'newValue':'004 Outras doenças infecciosas intestinais'})
        
        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'004 Outras doenças infecciosas intestinais','ano':1996})        
        query = list(query)
        self.assertEqual(query[0]['ocorrencias'],7,"Os 7 óbitos por Outras doenças Intestinais em 1996 deveriam continuar aparecendo (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'013 Intoxicações alimentares','ano':1996})  
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Os 7 óbitos por Outras doenças Intestinais em 1996 nao devem aparecer, porque não se pode desagrupar o futuro (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'016 Infecções intestinais mal definidas','ano':1996})  
        self.assertEqual(sum([q['ocorrencias'] for q in query]),0,"Os 7 óbitos por Outras doenças Intestinais em 1996 nao devem aparecer, porque não se pode desagrupar o futuro (query original)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':'004 Outras doenças infecciosas intestinais','ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),186,"Os 184 óbitos por Infecções intestinais mal definidas + 2 por Intoxicações alimentares em 1995 deveriam ser informadas (query com nome novo)")

        query = collection.find_many({'UF':'BA','municipio':'292740 SALVADOR','cid':{'$regex':'intestinais'},'ano':1995})        
        self.assertEqual(sum([q['ocorrencias'] for q in query]),186,"Os 184 óbitos por Infecções intestinais mal definidas + 2 por Intoxicações alimentares em 1995 deveriam ser informadas (query com nome novo)")


        