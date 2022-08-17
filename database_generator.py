from semantic_heterogeneous_database import BasicCollection
from datetime import datetime
import random
import string
import json


class DatabaseGenerator:
    FIELD_TYPES = ['int', 'float', 'datetime']
    OPERATION_TYPE = ['grouping', 'translation']

    def __init__(self, host='localhost'):
        self.host = host
        self.generated_values = dict()

    ## Pensar depois em uma distribuição para o número de campos ao inves de ser fixo
    #  alem de um numero delimitado de valores possiveis para os campos
    def __generate_record(self):
        new_record = {}
        value = 0

        valid_from_date = datetime.fromordinal(random.randint(365*2000, 365*2100))

        for field in self.fields:
            if field[1] == 'datetime':
                value = datetime.fromordinal(random.randint(365*2000, 365*2100)).strftime('%Y-%m-%d')
            elif field[1] == 'int':
                value = random.randint(1,9999999)
            elif field[1] == 'float':
                value = random.randint(1,9999999)/random.randint(1,9999999)

            new_record[field[0]] = value
            self.generated_values.setdefault(field[0], list())
            self.generated_values[field[0]].append(value)

        self.collection.insert_one(json.dumps(new_record), valid_from_date)

    def __generate_version(self):
        version_date = datetime.fromordinal(random.randint(365*2000, 365*2100))
        operation_type = random.choice(DatabaseGenerator.OPERATION_TYPE)
        arguments = None

        if operation_type == 'translation':
            fieldName = random.choice(self.fields)[0]
            oldValue = random.choice(self.generated_values[fieldName])
            newValue = oldValue

            while newValue == oldValue:
                newValue = random.choice(self.generated_values[fieldName])
            
            arguments = {
                'fieldName' : fieldName,
                'oldValue' : oldValue,
                'newValue' : newValue
            }
            
        elif operation_type == 'grouping':
            fieldsList = list(filter(lambda f: f[1] != 'float', self.fields))
            field = random.choice(fieldsList) #It doesn't make sense to group float values
            fieldName = field[0]
            oldValues = [random.choice(self.generated_values[fieldName]), random.choice(self.generated_values[fieldName])]
            newValue = random.choice(self.generated_values[fieldName])            

            arguments = {
                'fieldName' : fieldName,
                'oldValues' : oldValues,
                'newValue' : newValue
            }

        
        self.collection.execute_operation(operation_type,version_date, arguments)


    def generate(self, number_of_records, number_of_versions, number_of_fields):
        ## Starting random database
        letters = string.ascii_lowercase
        self.database_name = ''.join(random.choice(letters) for i in range(5))
        self.collection_name = ''.join(random.choice(letters) for i in range(10))

        ##Generating fields present in the documents        
        self.fields = list()
        for i in range(number_of_fields):
            field_name = ''.join(random.choice(letters) for a in range(5))
            field_type = random.choice(DatabaseGenerator.FIELD_TYPES)
            self.fields.append((field_name, field_type))
        
        self.collection = BasicCollection(self.database_name, self.collection_name, self.host)

        for i in range(number_of_records):
            self.__generate_record()

        for i in range(number_of_versions-1):
            self.__generate_version()

d = DatabaseGenerator()
d.generate(200, 5, 11)