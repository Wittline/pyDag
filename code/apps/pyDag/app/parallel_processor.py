from multiprocessing.pool import ThreadPool as Pool
from error import ExecutionError

class ParallelProcessor:
    def __init__(self, cpus):   
        self.cpus = cpus
        self.pool = None        
    
    def __execute_task(self, task, func):

        item = (task, func(task))

        if isinstance(item[1], Exception):
            raise ExecutionError('Task "{0}" execution error: {1}'.format(item[0], item[1]))

        return item
        
    def process_tasks(self, tasks, func):
        
        self.pool = Pool(processes = self.cpus)
        results = [self.pool.apply_async(self.__execute_task, args=(task, func)) for task in tasks]
        self.pool.close()
        self.pool.join()

        outputs = [r.get() for r in results]

        return outputs

    def __terminate_tasks(self):
        self.pool.terminate()

    def set_state(self, state):
        if state.value == 5:
            self.__terminate_tasks()




