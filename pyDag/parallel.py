import multiprocessing as mp
from threading import Thread
import timeit as tiempo
import time

class Parallel:

    def __init__(self, cpus):        
        self.cpus = cpus
    
    def execute(self, task, func):
        return func(task)

        
    def process(self, tasks, func):

        pool = mp.Pool(processes=self.cpus)
        
        results = [pool.apply_async(self.execute, args=(task, func)) for task in tasks]
        pool.close()
        pool.join()
        
        outputs = [p.get() for p in results]

        return outputs