from semantic_heterogeneous_database import BasicCollection
from datetime import datetime
import random
import string
import json


class DatabaseGenerator:
    FIELD_TYPES = ['int', 'float', 'datetime','string']
    OPERATION_TYPE = ['grouping', 'translation']

    def __init__(self, host='localhost'):
        self.host = host        

    def __generate_field_domain(self, field_type, number_of_values_in_domain):        
        return_list = list()

        for i in range(number_of_values_in_domain):
            if field_type == 'datetime':
                value = datetime.fromordinal(random.randint(365*2000, 365*2100)).strftime('%Y-%m-%d')
            elif field_type == 'int':
                value = random.randint(1,9999999)
            elif field_type == 'float':
                value = random.randint(1,9999999)/random.randint(1,9999999)
            elif field_type == 'string':
                value = ''.join(random.choice(self.letters) for i in range(1,35))
            return_list.append(value)

        return return_list

    ## Pensar depois em uma distribuição para o número de campos ao inves de ser fixo
    #  alem de um numero delimitado de valores possiveis para os campos
    def __generate_record(self):
        new_record = {}
        value = 0

        valid_from_date = datetime.fromordinal(random.randint(365*2000, 365*2100))

        for field in self.fields:
            new_record[field[0]] = random.choice(self.field_domain[field[0]])            

        self.collection.insert_one(json.dumps(new_record), valid_from_date)

    def __generate_version(self):
        version_date = datetime.fromordinal(random.randint(365*2000, 365*2100))
        operation_type = random.choice(DatabaseGenerator.OPERATION_TYPE)
        arguments = None

        fieldsList = list(filter(lambda f: f[1] != 'float', self.fields)) #float fields are not suitable for goruping nor translation

        if operation_type == 'translation':
            fieldName = random.choice(fieldsList)[0]
            oldValue = random.choice(self.field_domain[fieldName])
            newValue = oldValue

            while newValue == oldValue:
                newValue = random.choice(self.field_domain[fieldName])
            
            arguments = {
                'fieldName' : fieldName,
                'oldValue' : oldValue,
                'newValue' : newValue
            }
            
        elif operation_type == 'grouping':            
            field = random.choice(fieldsList) #It doesn't make sense to group float values
            fieldName = field[0]
            oldValues = [random.choice(self.field_domain[fieldName]), random.choice(self.field_domain[fieldName])]
            newValue = random.choice(self.field_domain[fieldName])            

            arguments = {
                'fieldName' : fieldName,
                'oldValues' : oldValues,
                'newValue' : newValue
            }

        
        self.collection.execute_operation(operation_type,version_date, arguments)


    def generate(self, number_of_records, number_of_versions, number_of_fields, number_of_values_in_domain):
        ## Starting random database
        self.letters = string.ascii_lowercase
        self.database_name = ''.join(random.choice(self.letters) for i in range(5))
        self.collection_name = ''.join(random.choice(self.letters) for i in range(10))

        ##Generating fields present in the documents        
        self.fields = list()
        self.field_domain = dict()
        for i in range(number_of_fields):
            field_name = ''.join(random.choice(self.letters) for a in range(5))
            field_type = random.choice(DatabaseGenerator.FIELD_TYPES)
            self.fields.append((field_name, field_type))
            ##Generating fields domain of available values for each field. 
            self.field_domain[field_name] = self.__generate_field_domain(field_type, number_of_values_in_domain)

        
        self.collection = BasicCollection(self.database_name, self.collection_name, self.host)

        for i in range(number_of_records):
            self.__generate_record()

        for i in range(number_of_versions-1):
            self.__generate_version()


# import time
# #from database_generator import DatabaseGenerator

# d = DatabaseGenerator()
# d.generate(200, 5, 11,20)            