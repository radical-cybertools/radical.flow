# dask_backend.py

import os
import typeguard
import subprocess
from typing import Dict, Callable
from dask.distributed import Client, Future, as_completed

from .base import Session, BaseExecutionBackend


class DaskExecutionBackend(BaseExecutionBackend):
    @typeguard.typechecked
    def __init__(self, resources: Dict):
        self._session = Session()
        self.task_manager = TaskManager(resources)
        print('Dask-Parallel execution backend started successfully')

    def state(self):
        return "RUNNING"  # you can expand this if needed

    def task_state_cb(self):
        pass  # you can wire this in via register_callback

    def submit_tasks(self, tasks):
        return self.task_manager.submit_tasks(tasks)

    def shutdown(self) -> None:
        self.task_manager.shutdown()
        print('Shutdown is triggered, terminating the resources gracefully')


class TaskManager:
    def __init__(self, resources: Dict):
        self.client = Client(**resources)  # e.g., {'n_workers': 4, 'threads_per_worker': 1}
        self._callback_func: Callable[[dict, str], None] = lambda task, state: None

    def submit_tasks(self, tasks: list):
        futures = []
        for task in tasks:
            fut = self.client.submit(self._task_wrapper, task)
            fut.add_done_callback(lambda f, task=task: self._callback_func(*f.result()))
            futures.append(fut)
        return futures

    def register_callback(self, func: Callable[[dict, str], None]):
        self._callback_func = func

    def shutdown(self):
        self.client.shutdown()

    @staticmethod
    def _task_wrapper(task):
        exec_list = [task['executable']]
        exec_list.extend(task.get('arguments', []))

        result = subprocess.run(exec_list, capture_output=True, text=True, shell=True)

        task['stdout'] = result.stdout
        task['stderr'] = result.stderr
        task['exit_code'] = result.returncode

        state = 'DONE' if result.returncode == 0 else 'FAILED'
        return task, state
