from script_handler import ScriptHandler
from livy.client import LivyClient
from gcp.client import GCPClient


class Engine(ScriptHandler, LivyClient, GCPClient):

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        ScriptHandler().__init__(self, id, params, script)
        LivyClient().__init__(self)
        GCPClient().__init__(self)

    def run(self):
        script = self.format_script()
        self.run_script(script, self.__get_url_engine())



