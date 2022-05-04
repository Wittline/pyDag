from enum import Enum

class TaskState(Enum):
    none = 0
    waiting = 1
    running = 2
    success = 3
    failed = 4
    restarting = 5

