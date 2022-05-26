import importlib

class IACClient:

    def __init__(self):
        self.id = None        

    def __create_iac(self, name_module, script):

        with open(name_module + '.py', 'w') as file:
            file.write(script)
        
        mod = importlib.import_module("engines.iac." + name_module, ".")
        cls = getattr(mod, name_module)
        return cls()
        
    def run_script(self, script, params):     
        iac = self.__create_iac(script[0], script[1].format(**params))
        iac.run_script()

        
