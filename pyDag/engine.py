from script_handler import ScriptHandler
from livy.client import LivyClient
from gcp.client import BQClient
from enums import TypeScript
from error import *

class Engine(ScriptHandler, LivyClient, BQClient):

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        ScriptHandler().__init__(self, id, params, script)
        LivyClient().__init__(self)
        BQClient().__init__(self)


    def run(self):        
        script, typeS = self.format_script()
        if typeS == TypeScript.pyScripts:
            self.run_livy_script(script)
        elif typeS == TypeScript.sqlScripts:
            self.run_BQ_script(script)
        else:
            raise Exception("Engine not found for script {0} in task id {1}".format(self.script, self.id))

