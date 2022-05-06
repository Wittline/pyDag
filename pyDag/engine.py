from script_handler import ScriptHandler
from pyDag.scripts import pyscripts

class Engine:

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
    
    def __get_url_engine():


    def run(self):
        sh = ScriptHandler(self.script, self.params, self.id)
        sh.format_script()
        self.run()


