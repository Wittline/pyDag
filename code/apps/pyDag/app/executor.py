import timeit as ti
from enums import TaskState

class Executor:

    def set_state(self, state, tasks):

        if state.value == 1:
            self.__scheduling(tasks, state)
        elif state.value == 2:
            self.__waiting(tasks, state)
        elif state.value == 3:
            self.__running(tasks, state)
        elif state.value == 4:
            self.__success(tasks, state)
        elif state.value == 5:
            self.__failed(tasks, state)
        else:
            pass    

    def __running(self, task, state):
        task.logger.info(2, [task.id], False, None)     
        task.state = state        
        task.start_time = ti.default_timer()
        result = task.run()
        task.end_time = ti.default_timer()

        if result:
            self.__success(task, TaskState.success)

        return result

    def __scheduling(self, tasks, state):          
        for task in tasks:
            task.state = state
            task.logger.info(3, [task.id], False, None)          

    def __waiting(self, tasks, state):
        for task in tasks:
            task.state = state
            task.logger.info(4, [task.id], False, None)

    def __success(self, task, state):
        task.state = state
        task.logger.info(5, [task.id], False, None)
        task.logger.close()
    
    def __failed(self, tasks, state):
        for task in tasks:
            task.state = state
            task.logger.info(6, [task.id], False, None)
            task.logger.close()


