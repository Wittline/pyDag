from error import *
from parallel_processor import ParallelProcessor
from executor import Executor
from log_handler import LogHandler
from dag import DAG
from enums import TaskState, DagState, TypeEngine, TypeStorage
import time
from task import Task


class pyDag:

    def __init__(self, cpus, name = None):
        self.name = name
        self.cpus = cpus      
        self.run_time = '{}'.format(time.time_ns()) + '.log'
        file_log = '{0}/{1}/{2}'.format(self.name, 'pyDag', self.run_time)
        self.log = LogHandler(self.name, file_log)
        self.dag = DAG(self.log)
        self.processor = ParallelProcessor(self.cpus, self.log)
        self.executor =  Executor()  

    def __validateTasks(self, dag):

        _ids = {}

        if len(dag['dag'].keys()) <= 0:            
            return {'error': True, 'task_id':0, 'prop':None, 'msg':'There are no keys in dag file'}
        
        if  not "tasks" in dag['dag'].keys():
            return {'error': True, 'task_id':0, 'prop':None, 'msg':'There are no key "tasks" in the dag file'}

        if  len(dag['dag']['tasks']) < 1:
            return {'error': True, 'task_id':0, 'prop':None, 'msg':'There are no tasks in the dag file'}
        
        for task in dag['dag']['tasks']:

            if task['task_id'] in _ids:
                return {'error': True, 'task_id':task['task_id'], 'prop':'task_id', 'msg':'task_id already exist'}
            else:
                _ids[task['task_id']] = 1

            _path = task['script'].split(".")

            if len(_path) != 5:
                return {'error': True, 'task_id':task['task_id'], 'prop':'script', 'msg':'Path incomplete in script'}
                                
            try:
                types = TypeStorage[_path[0]]
            except Exception as ex:
                return {'error': True, 'task_id':task['task_id'], 'prop':'script', 'msg':'TypeStorage does not exist,' + str(ex)}
            try:
                typee = TypeEngine[_path[-2]]
            except Exception as ex:
                return {'error': True, 'task_id':task['task_id'], 'prop':'script', 'msg': 'TypeEngine does not exist,' + str(ex)}


        for task in dag['dag']['tasks']:
            if len(set(task['dependencies'])) != len(task['dependencies']):
                return {'error': True, 'task_id':task['task_id'], 'prop':'dependencies', 'msg':"There are repeated dependencies"}
            for d in task['dependencies']:
                if not d in _ids:
                    return {'error': True, 'task_id':task['task_id'], 'prop':'dependencies', 'msg':"Dependency '{}' does not exist as a task_id".format(d)}

        return {'error':False}
    

    def __to_dag(self, obj):

        t_dict = {}

        for k in self.dag.dag_data.keys():
            self.dag.dag_data[k] = obj['dag'][k]              

        for task in obj['dag']['tasks']:
            file_log = '{0}/{1}/{2}'.format(self.dag.dag_data['dag_id'], task['task_id'], self.run_time)
            t_dict[task['task_id']] = Task(task['task_id'], task['params'], task['script'], self.dag.dag_data, LogHandler(task['task_id'], file_log))
            self.dag.add_tasks(t_dict[task['task_id']])
            for d in task['dependencies']:
                self.dag.add_edge(t_dict[d], t_dict[task['task_id']])    

    
    def addDag(self, objDag):

        result = self.__validateTasks(objDag)
        if  result['error']:
            params = [result['task_id'], result['prop'], result['msg']] 
            self.log.info(0, params, True, TasksPropertiesError)
        else:
            self.__to_dag(objDag)           
            
    
    def __invoke(self, instance, state, *args, **kwargs):
        try:
            func = getattr(instance, 'set_state')
        except AttributeError:
            return None

        if not callable(func):
            return None

        return func(state, *args, **kwargs)

    def __process(self, to_run, running, processor, executor):

        def execute(task):
            return self.__invoke(executor, TaskState.running, task)
        try:
            return processor.process_tasks(to_run, execute)
        except ExecutionError:
            self.__invoke(executor, TaskState.failed, running)
            self.__invoke(processor, TaskState.failed)           
            raise

    def run(self):

        self.dag.state = DagState.running
        finished = []
        running = set()
        no_indegree = self.dag.initial_tasks()
        indegree_dict = {}

        for task in self.dag.tasks():
            indegree_dict[task] = self.dag.n_predecessors(task)

        while no_indegree:
            
            to_run = list(no_indegree-running)[:max(0, self.cpus-len(running))]     

            self.__invoke(self.executor, TaskState.scheduled, to_run)

            running |= set(to_run)
            self.__invoke(self.executor, TaskState.waiting, running)

            results = self.__process(
                to_run, running, self.processor, self.executor)


            processed = [r[0] for r in results]
            running -= set(processed)

            finished += processed
            no_indegree -= set(processed)

            for task, result in results:
                for to_run in self.dag.successors(task):
                    indegree_dict[to_run] -= 1
                    if indegree_dict[to_run] == 0:
                        no_indegree.add(to_run)
                
        if len(finished) == len(self.dag.tasks()):
            for task in finished:
                if task.state.value != 4:
                    self.dag.state = DagState.failed
                    break
            self.dag.state = DagState.success
        else:
            self.dag.state = DagState.failed
                
        result =  {
            "dag_id": self.dag.dag_data['dag_id'],
            "tasks_times": [(task.id, format(task.end_time, '.8f'), format(task.start_time, '.8f'), format(task.end_time-task.start_time, '.8f')) for task in finished if task.state.value == 4],
        }

        self.log.info(18, [result])
        self.log.close()
        
