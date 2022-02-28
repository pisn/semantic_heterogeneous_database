import operations

myCollection = operations.InterscityCollection('interscity', 'collectionTest')     
myCollection.insert_one('{"pais": "Brasil", "cidade":"Vila Rica"}', datetime(2001,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Cuiabá"}', datetime(2002,1,1))
myCollection.insert_one('{"pais": "Brasil", "cidade":"Rio de Janeiro"}',datetime(2003,1,1))

testeQuery = myCollection.query({'pais' : 'Brasil'})


myCollection.execute_translation("cidade","Vila Rica","Ouro Preto", datetime(2002,6,1))  ##QUando eu faco isso, preciso considerar que o registro do Rio de Janeiro deve estar em outra versao a partir de agora

testeQuery = myCollection.query({'pais' : 'Brasil'})


myCollection.insert_one('{"pais": "Brasil", "cidade":"São Paulo"}', datetime(2004,1,1))
myCollection.execute_translation("cidade","Leningrado","São Petesburgo", datetime(2000,6,1))       

myCollection.insert_one('{"pais": "Brasil", "cidade":"Leningrado"}', datetime(1980,1,1))

testeQuery = myCollection.query({'pais' : 'Brasil'})


myCollection.pretty_print(testeQuery)

myCollection.drop_database()