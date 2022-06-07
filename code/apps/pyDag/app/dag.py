from error import *
from enums import DagState

class DAG(object):

    def __init__(self, logger):
        self.__gph = {}
        self.__gph_inverse = {}
        self.state = DagState.none
        self.dag_data = {'dag_id':'','script_cache':False, 'expire_cache':0}
        self.logger = logger
                    

    def tasks(self):
        return set(self.__gph.keys())                  

    def add_tasks(self, *tasks):

        for task in tasks:
            if task not in self.__gph:
                self.__gph[task] = set()
                self.__gph_inverse[task] = set()


    def add_edge(self, task_from, *task_tos):
        
        self.__validate(task_from, *task_tos)

        for task_to in task_tos:
            if self.__path_to(task_to, task_from):
                params = [task_from, task_to]
                self.logger.info(7, params, True, CycleError)                
            self.__gph[task_from].add(task_to)
            self.__gph_inverse[task_to].add(task_from)


    def successors(self, task):
        
        self.__validate(task)
        return self.__gph[task]

    def predecessors(self, task):

        self.__validate(task)
        return self.__gph_inverse[task]


    def __validate(self, *tasks):
        for task in tasks:
            if task not in self.tasks():
                self.logger.info(8, [task], True, TaskNotFoundError)

    def __path_to(self, task_from, task_to):
        if task_from == task_to:
            return True
        for task in self.__gph[task_from]:
            if self.__path_to(task, task_to):
                return True
        return False

    def n_predecessors(self, task):        
        return len(self.predecessors(task))

    def n_successors(self, task):     
        return len(self.successors(task))

    def __initials(self, callback):
        initials = set()
        for task in self.tasks():
            if callback(task) == 0:
                initials.add(task)
        return initials

    def initial_tasks(self):        
        return self.__initials(self.n_predecessors)