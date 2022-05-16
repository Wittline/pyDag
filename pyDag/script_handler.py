from enum import Enum
import importlib
import json
from enums import TypeScript, TypeEngine, TypeStorage
from scripts.projecttest.moduletest.create_table_stg import create_table_stg

class ScriptHandler:

    def __init__(self, id,  params, script):
        self.id = id        
        self.params = params
        self.script = script

    def __get_local_scripts(self, _path, params):
                
        typeengine =  TypeEngine[_path[-2]]
        scriptname = _path[-1]        
        _path.remove(typeengine.name)        
        mod = importlib.import_module("scripts." + '.'.join(_path), ".")
        cls = getattr(mod, scriptname)
        script = cls().get_script(scriptname, params)        
        return script, typeengine

    def __get_gcs_scripts(self, scr, name, params):
        pass

    def __get_s3_scripts(self, scr, name, params):
        pass

    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items()
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items()
            if k[0] != '*'}                            

    def __get_script(self, _path, params):

        typestorage = TypeStorage[_path[0]]
                
        if typestorage == TypeStorage.local:
            return self.__get_local_scripts(_path[1:], params)
        else:
            raise Exception("Location script not found: {0}".format(_path))


    def format_script(self):
        _path = self.script.split('.')        
        p_dict  = json.loads(self.params.replace("'",'"'))
        if len(_path) == 5 and len(p_dict.keys()) > 0:
            params = self.__get_parameters(p_dict)
            connections = self.__get_connections(p_dict)
            variables = self.__get_variables(p_dict)
            return self.__get_script(_path, params)
        else:
            raise Exception("Path incomplete for script {0} in task id {1}".format(self.script, self.id))

    
