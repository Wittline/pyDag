from script_handler import ScriptHandler
from livy.client import LivyClient
from engine_handler import EngineHandler
from gcp.client import BQClient
from enums import TypeScript
from error import *

class Engine(ScriptHandler, EngineHandler):

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        ScriptHandler.__init__(self, id, params, script)
        EngineHandler.__init__(self, id)
        # LivyClient.__init__(self, id)
        # BQClient.__init__(self, id)


    def run(self):
        script, typeEngine = ScriptHandler.format_script()
        EngineHandler.run_script(script, typeEngine)
        # if typeS == TypeScript.pyScripts:
        #     LivyClient.run_script(script)
        # elif typeS == TypeScript.sqlScripts:
        #     BQClient.run_script(script)
        # else:
        #     raise EngineNotFoundError(
        #         "Engine not found for script {0} in task id {1}"
        #         .format(self.script, self.id))

