from enum import Enum

class DagState(Enum):
    none = 0
    running = 1
    success  = 2
    failed  = 3
