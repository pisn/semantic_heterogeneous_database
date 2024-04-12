from semantic_heterogeneous_database import BasicCollection,TranslationOperation
from datetime import datetime
import os
import time

myCollection = BasicCollection('sus', 'mortalidade', 'localhost','preprocess')     
# translationOperation = TranslationOperation(myCollection)

#startTime = time.time()
folder = '/home/pedro/Documents/USP/Mestrado/Pesquisa/mortalidade_ano'
# for file in os.listdir(folder):
#     myCollection.insert_many_by_csv(folder + '/' + file, 'RefDate')

myCollection.insert_many_by_csv(folder + '/' + 'mortalidade_9_processed_1995.csv', 'RefDate')
myCollection.insert_many_by_csv(folder + '/' + 'mortalidade_10_processed_1996.csv', 'RefDate')

# pass

#myCollection.execute_operation('ungrouping',datetime(1996,1,1),{'fieldName':'cid', 'oldValue':'35 Doenças do aparelho urinário', 'newValues':['087 Rest doenças do aparelho geniturinário','085 D glomerulares e d renais túbulo-interstic']})
myCollection.execute_operation('grouping',datetime(1996,1,1),{'fieldName':'cid', 'oldValues':['36 Doenças dos órgãos genitais masculinos','360 Hiperplasia da próstata','37 Doenças dos órgãos genitais femininos'], 'newValue':'087 Rest doenças do aparelho geniturinário'})

# query = myCollection.find_many({'cid':'35 Doenças do aparelho urinário','ano':1995})
# myCollection.pretty_print(query)

query = myCollection.find_many({'cid':'087 Rest doenças do aparelho geniturinário','ano':1995})
myCollection.pretty_print(query)