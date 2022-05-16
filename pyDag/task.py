from logging import exception
from enums import TaskState
from engine import Engine

class Task(Engine):

    def __init__(self, id, params, script):
        self.id = id
        self.params = params
        self.script = script
        self.state = TaskState.none
        Engine.__init__(self, id, params, script)

