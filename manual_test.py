from BasicCollection import BasicCollection
from TranslationOperation import TranslationOperation
from datetime import datetime
import os
import time



myCollection = BasicCollection('interscity', 'collectionTest')     
# myCollection.insert_one('{"country":"A", "city":"Newly Created City"}',datetime(2000,12,31))        
# myCollection.execute_operation('translation',datetime(2003,5,26), {'fieldName':'city', 'oldValue':'Old Created City', 'newValue':'Newly Created City'})        

# q = myCollection.find_many({'city':'Old Created City'})                
# myCollection.pretty_print(q)
# myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim"}', datetime(2001,1,1))
# myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim Ocidental"}', datetime(1947,1,1))
# myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim Oriental"}', datetime(1947,1,1))
# myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}', datetime(2002,1,1))
# myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}',datetime(2003,1,1))

# myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim"}', datetime(2001,1,1))
# myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}', datetime(2002,1,1))
# myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}',datetime(2003,1,1))

# myCollection.execute_operation('grouping',datetime(1991,6,1), {'fieldName':'cidade', 'oldValues':['Berlim Oriental','Berlim Ocidental'], 'newValue' :'Berlim'})
# myCollection.execute_operation('translation',datetime(2004,6,1), {'fieldName':'cidade', 'oldValue':'Ouro Preto', 'newValue':'Nova Ouro Preto'})
# myCollection.execute_operation('translation',datetime(2003,6,1), {'fieldName':'cidade', 'oldValue':'Cuiabá', 'newValue':'Nova Cuiabá'})

#testeQuery = myCollection.find_many({'pais' : 'Alemanha'})
testeQuery = myCollection.find_many({'cidade' : 'Berlim'})
myCollection.pretty_print(testeQuery)

#myCollection = BasicCollection('IBGE', 'estimativa_populacional')     
# translationOperation = TranslationOperation(myCollection)

#startTime = time.time()
#myCollection.insert_many_by_csv(os.path.join(os.path.dirname(__file__),'IBGE_Population/Estimativa_Populacao.csv'), 'RefDate')

#executionTime = (time.time() - startTime)
#print('Execution time for insertion: ' + str(executionTime))
#myCollection.execute_operation('translation',datetime(2004,6,6),{'fieldName':'Municipio', 'oldValue':'Piçarras', 'newValue':'Balneário Piçarras'})
#myCollection.execute_many_operations_by_csv(os.path.join(os.path.dirname(__file__), 'IBGE_NameChanges/IBGE_NameChanges.csv'), 'type', 'RefDate')

# executionTime = (time.time() - startTime)
# print('Execution time for translations: ' + str(executionTime))

#query = myCollection.find_many({'Municipio': 'Balneário Piçarras'})

#query = myCollection.find_many({'Municipio': 'Piçarras'})
#myCollection.pretty_print(query)

# query = myCollection.find_many({'Municipio': 'Balneário Piçarras'})
# myCollection.pretty_print(query)

# myCollection = BasicCollection('SBBD', 'artigo')     
# myCollection.insert_one('{"city": "City A", "state":"State Z", "population": 51456}', datetime(1996,7,6))
# myCollection.insert_one('{"city": "City B", "state":"State Z", "population": 79854}', datetime(2014,4,13))

# myCollection.execute_operation('translation',datetime(2004,6,1), {'fieldName':'city', 'oldValue':'City A', 'newValue':'City B'})

# query = myCollection.find_many({'city': 'City A'})
# myCollection.pretty_print(query)