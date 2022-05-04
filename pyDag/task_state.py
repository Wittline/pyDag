from enum import Enum

class TaskState(Enum):
    none = 0
    scheduled = 1
    waiting = 2
    running = 3
    success = 4
    failed = 5
    restarting = 6

