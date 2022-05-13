import configparser
import importlib
from enums import TypeEngine

class EngineHandler:

    def __init__(self, id):
        self.id = id

    def __get_engine_config(self, typeEngine):
        config = configparser.ConfigParser()
        config.read_file(open('config.cfg'))
        return config.get('engines', typeEngine)

    def __create_engine(self, config_engine):
        
        _path = config_engine.split(".")
        mod = importlib.import_module(config_engine, ".")
        cls = getattr(mod, _path[-1])        
        return cls() 
        

    def run_script(self, script, typeEngine):
        
        config_engine = self.__get_engine_config(typeEngine)
        engine = self.__create_engine(config_engine)
        return engine.run_script(script)


    