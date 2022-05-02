class Task:
        
    def execute_script(self, param, script):
        
        _script = script.split('.')
        print("executing script {0} with params: {1}".format(_script[-1], param))



