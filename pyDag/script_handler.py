from enum import Enum
import importlib
import json
from unicodedata import name
from enums import TypeEngine
from cache.cache import Cache
import configparser
from google.cloud import storage, exceptions
import os


class ScriptHandler:

    def __init__(self, id,  params, script):
        self.id = id        
        self.params = params
        self.script = script

    # def __get_local_scripts(self, _path, params):
                
    #     typeengine =  TypeEngine[_path[-2]]
    #     scriptname = _path[-1]        
    #     _path.remove(typeengine.name)        
    #     mod = importlib.import_module("scripts." + '.'.join(_path), ".")
    #     cls = getattr(mod, scriptname)
    #     script = cls().get_script(scriptname, params)       
    #     return script, typeengine

    def __get_gcs_scripts(self, _path, params, key):

        engine =  TypeEngine[_path[2]]

        try:
            config = configparser.ConfigParser()
            config.read_file(open(os.getcwd() + '/config/config.cfg'))
            bucket_name = _path[0]
            folder = _path[1]
            storage_client = storage.Client.from_service_account_json(config.get('GCP','service-account'))
            bucket = storage_client.get_bucket(bucket_name)
            blob = bucket.get_blob(folder + '/' + _path[3] + engine.value)
            script = blob.download_as_string().decode()
            if len(script) > 0:
                Cache.set(key, script)
            return script.format(**params), engine
        except exceptions.GoogleCloudError as ex:
            raise                   


    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items()
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items()
            if k[0] != '*'}                            

    def __get_script(self, _path, params):

        key = '{}.{}.{}.{}.{}'.format(self.id, *_path[1:])
        script  = Cache.get(key)

        if script is not None:
            return script.format(**params), TypeEngine[_path[-2]]
        else:
            return self.__get_gcs_scripts(_path[1:], params, key)

    def format_script(self):

        _path = self.script.split('.')   
        p_dict = {}
        if len(self.params) > 0:
            p_dict  = json.loads(self.params.replace("'",'"'))        
        params = self.__get_parameters(p_dict)
        connections = self.__get_connections(p_dict)
        variables = self.__get_variables(p_dict)
        return self.__get_script(_path, params)

    
