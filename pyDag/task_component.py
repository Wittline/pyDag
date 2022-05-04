from logging import exception
from task_state import TaskState

class TaskComponent:

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        self.state = TaskState.none
        
    def execute_script(self):

        try:
            _script = self.script.split('.')
            print("Task id: '{0}' executing script '{1}' with params: '{2}'".format(
                self.id, _script[-1], self.params
                ))
            return True
        except Exception as ex:
            return False
        
        
        



