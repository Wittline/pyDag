class TaskComponent:

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        
    def execute_script(self):
        
        _script = self.script.split('.')
        print("Task id: '{0}' executing script '{1}' with params: '{2}'".format(self.id, _script[-1], self.params))



