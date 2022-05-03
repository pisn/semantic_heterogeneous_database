from BasicCollection import BasicCollection
from TranslationOperation import TranslationOperation
from datetime import datetime
import os
import time

myCollection = BasicCollection('interscity', 'collectionTest')     
myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim Oriental"}', datetime(1947,1,1))
myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim Ocidental"}', datetime(1947,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}', datetime(2002,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}',datetime(2003,1,1))

myCollection.execute_operation('grouping',datetime(1991,6,1), {'fieldName':'cidade', 'oldValues':['Berlim Oriental','Berlim Ocidental'], 'newValue' :'Berlim'})
# myCollection.execute_operation('translation',datetime(2004,6,1), {'fieldName':'cidade', 'oldValue':'Ouro Preto', 'newValue':'Nova Ouro Preto'})
# myCollection.execute_operation('translation',datetime(2003,6,1), {'fieldName':'cidade', 'oldValue':'Cuiabá', 'newValue':'Nova Cuiabá'})

testeQuery = myCollection.find_many({'pais' : 'Alemanha'})
myCollection.pretty_print(testeQuery)

# myCollection = BasicCollection('IBGE', 'estimativa_populacional')     
# translationOperation = TranslationOperation(myCollection)

# startTime = time.time()
# myCollection.insert_many_by_csv(os.path.join(os.path.dirname(__file__),'IBGE_Population/Estimativa_Populacao.csv'), 'RefDate')

# executionTime = (time.time() - startTime)
# print('Execution time for insertion: ' + str(executionTime))

# myCollection.execute_many_operations_by_csv(os.path.join(os.path.dirname(__file__), 'IBGE_NameChanges/IBGE_NameChanges.csv'), 'type', 'RefDate')
#translationOperation.execute_operation("Municipio","Piçarras","Balneário Piçarras", datetime(2004,7,20))       

# executionTime = (time.time() - startTime)
# print('Execution time for translations: ' + str(executionTime))

# query = myCollection.find_many({'Municipio': 'Balneário Piçarras'})
# myCollection.pretty_print(query)

# query = myCollection.find_many({'Municipio': 'Piçarras'})
# myCollection.pretty_print(query)