from logging import exception
from enums import TaskState
from engine import Engine

class Task(Engine):
    def __init__(self, id, params, script, dag, logger):
        self.id = id
        self.params = params
        self.script = script
        self.state = TaskState.none
        self.start_time = None
        self.end_time = None
        self.logger = logger
        Engine.__init__(self, id, params, script, dag, logger)