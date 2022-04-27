from BasicCollection import BasicCollection
#import TranslationOperation
from datetime import datetime
import os
import time

myCollection = BasicCollection('interscity', 'collectionTest')     
myCollection.insert_one('{"pais": "Brasil", "cidade":"Vila Rica"}', datetime(2001,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}', datetime(2002,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}',datetime(2003,1,1))


# myCollection.execute_translation("cidade","Vila Rica","Ouro Preto", datetime(2002,6,1))  ##QUando eu faco isso, preciso considerar que o registro do Rio de Janeiro deve estar em outra versao a partir de agora
# myCollection.execute_translation("cidade","Ouro Preto","Nova Ouro Preto", datetime(2004,6,1))  
# myCollection.execute_translation("cidade","Cuiabá","Nova Cuiabá", datetime(2003,6,1))  


# myCollection.insert_one('{"pais": "Brasil", "cidade":"São Paulo"}', datetime(2004,1,1))
# myCollection.execute_translation("cidade","Leningrado","São Petesburgo", datetime(2000,6,1))       

# myCollection.insert_one('{"pais": "Brasil", "cidade":"Leningrado"}', datetime(1980,1,1))

# testeQuery = myCollection.query({'pais' : 'Brasil'})
# myCollection.pretty_print(testeQuery)

# myCollection.drop_database()


myCollection = operations.InterscityCollection('IBGE', 'estimativa_populacional')     

startTime = time.time()
myCollection.insert_many_by_csv(os.path.join(os.path.dirname(__file__),'IBGE_Population/Estimativa_Populacao.csv'), 'RefDate')

executionTime = (time.time() - startTime)
print('Execution time for insertion: ' + str(executionTime))

#myCollection.execute_translations_by_csv(os.path.join(os.path.dirname(__file__), 'IBGE_NameChanges/IBGE_NameChanges.csv'))
myCollection.execute_translation("Municipio","Piçarras","Balneário Piçarras", datetime(2004,7,20))       

executionTime = (time.time() - startTime)
print('Execution time for translations: ' + str(executionTime))

query = myCollection.query({'Municipio': 'Balneário Piçarras'})
myCollection.pretty_print(query)

query = myCollection.query({'Municipio': 'Piçarras'})
myCollection.pretty_print(query)