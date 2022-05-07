from enum import Enum
import importlib
import json


class TypeScript(Enum):
    pyScripts  = 0
    sqlScripts = 1

class ScriptStorage(Enum):
    local = 0
    gcp = 1
    aws = 2
    database = 3

class ScriptHandler:

    def __init__(self, id,  script, params):
        self.id = id
        self.script = script
        self.params = params

    def __get_local_scripts(self, scr, name, params):

        typescript = TypeScript[scr[-1]]

        mod = importlib.import_module("scripts." + '.'.join(scr), ".")

        if typescript == TypeScript.pyScripts:
            plugin = mod.pyScripts()
        elif typescript == TypeScript.sqlScripts:
            plugin = mod.sqlScripts()
        else:
            raise Exception("Type script not found: {0}".format(scr))
        
        return plugin.get_script(name, params)
  

    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items()
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items()
            if k[0] != '*'}                            

    def __get_script(self, _path):

        if _path[0] == ScriptStorage.local.name:
            return self.__get_local_scripts(_path[1:-1], _path[-1])
        else:
            raise Exception("Location script not found: {0}".format(_path))


    def format_script(self):
        _path = self.script.split('.')
        p_dict  = json.loads(self.params.replace("'",'"'))
        if len(_path) == 5 and len(p_dict.keys()) > 0:
            params = self.__get_parameters(p_dict)
            connections = self.__get_connections(p_dict)
            variables = self.__get_variables(p_dict)
            script = self.__get_script(_path, params)
            return script
        else:
            raise Exception("Path incomplete for script {0} in task id {1}".format(self.script, self.id))

    
