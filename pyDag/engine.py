from script_handler import ScriptHandler
from engine_handler import EngineHandler
from error import *

class Engine(ScriptHandler, EngineHandler):

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        ScriptHandler.__init__(self, id, params, script)
        EngineHandler.__init__(self, id)
    
    def run(self):
        script, typeengine = self.format_script()
        self.run_script(script, typeengine)