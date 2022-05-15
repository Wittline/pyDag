from enum import Enum
import importlib
import json
from enums import TypeScript, TypeEngine, ScriptStorage

class ScriptHandler:

    def __init__(self, id,  script, params):
        self.id = id
        self.script = script
        self.params = params

    def __get_local_scripts(self, scr, name, params):

        typescript = TypeScript[scr[-1]]
        typeengine = TypeEngine[scr[-2]]    
        mod = importlib.import_module("scripts." + '.'.join(scr), ".")
        cls = getattr(mod, typescript.name)
        return cls.get_script(name, params), typeengine
  

    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items()
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items()
            if k[0] != '*'}                            

    def __get_script(self, _path, params):

        if _path[0] == ScriptStorage.local.name:
            return self.__get_local_scripts(_path[1:-1], _path[-1], params)
        else:
            raise Exception("Location script not found: {0}".format(_path))


    def format_script(self):
        _path = self.script.split('.')
        p_dict  = json.loads(self.params.replace("'",'"'))
        if len(_path) == 6 and len(p_dict.keys()) > 0:
            params = self.__get_parameters(p_dict)
            connections = self.__get_connections(p_dict)
            variables = self.__get_variables(p_dict)
            return self.__get_script(_path, params)
        else:
            raise Exception("Path incomplete for script {0} in task id {1}".format(self.script, self.id))

    
