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

    def __init__(self, id,  params, script, dag_data):
        self.id = id        
        self.params = params
        self.script = script
        self.dag_data = dag_data

    def __get_gcs_scripts(self, _path):

        engine =  TypeEngine[_path[2]]
        try:
            config = configparser.ConfigParser()
            config.read_file(open(os.getcwd() + '/config/config.cfg'))
            service_account = config.get('GCP','service-account')
            storage_client = storage.Client.from_service_account_json(service_account)
            bucket = storage_client.get_bucket(_path[0])
            blob = bucket.get_blob(_path[1] + '/' + _path[3] + engine.value)
            script = blob.download_as_string().decode()
            return script
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

        script = None
        engine = TypeEngine[_path[-2]]
        key = '{}.{}.{}.{}.{}.{}'.format(self.dag_data['dag_id'], self.id, *_path[1:])

        if self.dag['script_cache']:
            script  = Cache.get(key)            

        if script is not None:     
            return script.format(**params), engine
        else:
            script = self.__get_gcs_scripts(_path[1:])
            if self.dag['script_cache']:
                Cache.set(key, script, self.dag['expire_cache'])                
            return script.format(**params), engine

    def format_script(self):

        _path = self.script.split('.')
        p_dict = {}
        if len(self.params) > 0:
            p_dict  = json.loads(self.params.replace("'",'"'))        
        params = self.__get_parameters(p_dict)
        connections = self.__get_connections(p_dict)
        variables = self.__get_variables(p_dict)
        return self.__get_script(_path, params)