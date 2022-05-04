from logging import exception
from task_state import TaskState
import time
import random

class Task:

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        self.state = TaskState.none
        
    def execute_script(self):
        _script = self.script.split('.')
        time.sleep(random.randint(2, 5))

        # print("Task id: '{0}' executing script '{1}' with params: '{2}'".format(
        #     self.id, _script[-1], self.params
        #     ))

        
        



