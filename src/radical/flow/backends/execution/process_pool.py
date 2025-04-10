import os
import typeguard
import subprocess
import radical.utils as ru

from typing import Dict, Callable
from concurrent.futures import ProcessPoolExecutor, Future

from .base import Session, BaseExecutionBackend

# Needs to be re-imported if using multiprocessing
def _task_wrapper(task):
    import subprocess  
    exec_list = [task['executable']]
    exec_list.extend(task.get('arguments', []))

    result = subprocess.run(exec_list, capture_output=True, text=True, shell=True)

    task['stdout'] = result.stdout
    task['stderr'] = result.stderr
    task['exit_code'] = result.returncode

    state = 'DONE' if result.returncode == 0 else 'FAILED'
    return task, state

class ProcessExecutionBackend(BaseExecutionBackend):
    @typeguard.typechecked
    def __init__(self, resources: Dict):
        self._session = Session()
        self.task_manager = TaskManager()
        print('ProcessPool execution backend started successfully')

    def state(self):
        pass

    def task_state_cb(self):
        pass

    def submit_tasks(self, tasks):
        return self.task_manager.submit_tasks(tasks)

    def shutdown(self) -> None:
        self.task_manager.shutdown(cancel_futures=True)
        print('Shutdown is triggered, terminating the resources gracefully')


class TaskManager(ProcessPoolExecutor):
    def __init__(self):
        super().__init__()
        self._callback_func: Callable[[dict, str], None] = lambda task, state: None  # Must accept two arguments

    def submit_tasks(self, tasks: list):
        for task in tasks:
            fut = self.submit(_task_wrapper, task)  # NOTE: must use top-level function
            fut.add_done_callback(lambda f, task=task: self._callback_func(*f.result()))

    def register_callback(self, func: Callable[[dict, str], None]):
        self._callback_func = func
