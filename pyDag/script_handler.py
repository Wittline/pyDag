from enum import Enum
import importlib
import json
from mimetypes import types_map
from unicodedata import name
from enums import TypeEngine, TypeStorage
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


    def __add_to_json_dict(self, params_dict):
        config = configparser.ConfigParser()
        config.read_file(open(os.getcwd() + '/config/config.cfg'))
        spark_config = config.get('SPARK-CONFIG','spark.jars.packages')
        params_dict['spark.jars.packages'] = spark_config
        params_dict['id'] = self.dag_data['dag_id'] + '_' + self.id             
        return str(json.dumps(params_dict))

    def __get_connections(self, p_dict):
        return {k: v for k, v in p_dict.items() if k[0:2] == '**'}

    def __get_variables(self, p_dict):
         return {k: v for k, v in p_dict.items()
            if k[0:2] != '**' and  k[0] == '*' }

    def __get_parameters(self, p_dict):
        return {k: v for k, v in p_dict.items()
            if k[0] != '*'}

    def __get_script(self, _path, engine, params):

        script = None
        storage = TypeStorage[_path[0]]
        key = '{}.{}.{}.{}.{}.{}'.format(self.dag_data['dag_id'], self.id, *_path[1:])

        if self.dag_data['script_cache']:
            script  = Cache.get(key)

        if script is not None:     
            return script, engine, params
        else:
            script = self.__get_gcs_scripts(_path[1:])
            if self.dag_data['script_cache']:
                Cache.set(key, script, self.dag_data['expire_cache'])
            return script, engine, params

    def format_script(self):

        _path = self.script.split('.')
        _engine = TypeEngine[_path[-2]]
        params_dict = {}        

        if len(self.params) > 0:
            params_dict  = json.loads(self.params.replace("'",'"'))
            params_dict = self.__get_parameters(params_dict)
        
        if _engine  == TypeEngine.spark:
            return _path[1:], _engine, self.__add_to_json_dict(params_dict)
        else:
            return self.__get_script(_path, _engine, params_dict)