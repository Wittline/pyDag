from enum import Enum

class DagState(Enum):
    none = 0
    running = 1
    success  = 2
    failed  = 3

class TaskState(Enum):
    none = 0
    scheduled = 1
    waiting = 2
    running = 3
    success = 4
    failed = 5
    restarting = 6

class TypeScript(Enum):
    pyScripts  = 1
    sqlScripts = 0

class ScriptStorage(Enum):
    local = 0
    gcp = 1
    aws = 2
    database = 3    

