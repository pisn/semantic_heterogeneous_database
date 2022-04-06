
import zope.interface

class SemanticOperation(zope.interface.Interface):
    def __init__(self, Collection):
        pass

    def execute_operation(self, validFromDate:datetime, **args):
        """Execute the semantic operation, updating what needs to be updated and registering it in the operations catalog 
        
        Args:
            valid_from_date(): When the semantic operation has happened. This is important to decide which registers must suffer any changes and be reprocessed.

        """
        pass

    def execute_many_operations_by_csv(self, filePath):
        """ Execute operations specified by the csv file. 

        Args: 
            filePath (): path to the csv file containing the list of operations

        """

    def evolute(self, document, operationArgs):
        """Evolute a processed document with the operation arguments
        
        Args:
            document(): document being evoluted to another version
            operationArgs(): Arguments of the operation. Needed arguments might change for different operations. None of them is mandatory

        """
        pass
        