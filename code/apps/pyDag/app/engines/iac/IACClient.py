import importlib
from engines.iac.iacerror import IACError
import os

class IACClient:

    def __init__(self):
        pass    

    def __create_iac(self, name_module, script):
                
        with open('engines/iac/' + name_module + '.py', 'w') as file:
            file.write(script)
                
        mod = importlib.import_module("engines.iac." + name_module, ".")
        cls = getattr(mod, name_module)
        return cls()   

        
    def run_script(self, script, params):

        try:
            iac = self.__create_iac(script[0], script[1].format(**params))        
            iac.run_script()
            return True
        except Exception as ex:
            raise IACError('Error processing IAC script: {0}'.format(str(ex)))

        
