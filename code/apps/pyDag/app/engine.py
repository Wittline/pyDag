from script_handler import ScriptHandler
from engine_handler import EngineHandler
from error import *

class Engine(ScriptHandler, EngineHandler):
    def __init__(self, id, params, script, dag, logger):
        self.id = id
        self.params = params
        self.script = script
        ScriptHandler.__init__(self, id, params, script, dag, logger)
        EngineHandler.__init__(self, id, logger)
    
    def run(self):
        script, typeengine, params = self.format_script()  
        return self.run_script(script, typeengine, params)