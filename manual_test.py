from semantic_heterogeneous_database import BasicCollection,TranslationOperation
from datetime import datetime
import os
import time



# myCollection = BasicCollection('interscity', 'collectionTest')  
# myCollection.insert_one('{"country":"A", "city":"Blala"}',datetime(2001,12,31))           
# myCollection.execute_operation('translation',datetime(2003,5,26), {'fieldName':'city', 'oldValue':'Old Created City', 'newValue':'Newly Created City'})        
# myCollection.execute_operation('translation',datetime(2007,5,26), {'fieldName':'city', 'oldValue':'Newly Created City', 'newValue':'Newest Created City'})        
# myCollection.insert_one('{"country":"A", "city":"Old Created City"}',datetime(2000,12,31))        


# myCollection.collection.rewrite_query({'city':'Old Created City'})

myCollection = BasicCollection('interscity', 'collectionTest', operation_mode='rewrite')  
myCollection.insert_one('{"street":"A", "number":51}',datetime(2001,12,31))           
myCollection.execute_operation('grouping',datetime(2003,5,26), {'fieldName':'number', 'oldValues':[51,252], 'newValue':500})        
myCollection.execute_operation('translation',datetime(2005,5,26), {'fieldName':'number', 'oldValue':500, 'newValue':700})        
#myCollection.execute_operation('ungrouping',datetime(2003,5,26), {'fieldName':'number', 'oldValue':51, 'newValues':[500,999]})        
#myCollection.execute_operation('translation',datetime(2007,5,26), {'fieldName':'number', 'oldValue':252, 'newValue':934})        
myCollection.insert_one('{"street":"A", "number":500}',datetime(2004,12,31))        


q = myCollection.find_many({'number':500})
#myCollection.collection.rewrite_query({'number':{'$in':51}})
#myCollection.collection.rewrite_query({'$xor':[{'number':51},{'outro_campo':99}]})
pass



# myCollection.pretty_print(q)
#myCollection.insert_one('{"pais": "Alemanha", "cidade":"Berlim"}', datetime(2001,1,1))
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
# testeQuery = myCollection.find_many({'cidade' : 'Berlim'})
# myCollection.pretty_print(testeQuery)

#myCollection = BasicCollection('IBGE', 'estimativa_populacional')     
# translationOperation = TranslationOperation(myCollection)

#startTime = time.time()
#myCollection.insert_many_by_csv(os.path.join(os.path.dirname(__file__),'IBGE_Population/Estimativa_Populacao.csv'), 'RefDate')

#executionTime = (time.time() - startTim e)
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

#myCollection.insert_one('{"country":"X", "city":"Pi"}',datetime(1900,12,20))                
#myCollection.execute_operation('ungrouping',datetime(1930,6,20), {'fieldName':'city', 'oldValue':'Pi', 'newValues':['Q','W']})        
