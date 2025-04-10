import os
import subprocess
import typeguard
from typing import Dict, Callable
from concurrent.futures import ThreadPoolExecutor, Future
import radical.pilot as rp
import radical.utils as ru

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

class Session():
    def __init__(self):
        self.path = os.getcwd()

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


class ResourceEngine:

    @typeguard.typechecked
    def __init__(self, resources: Dict) -> None:
        self._session = Session()
        self.task_manager = TaskManager()

    def state(self):
        raise NotImplementedError

    def task_state_cb(self, task, state):
        raise NotImplementedError

    def shutdown(self) -> None:
        self.task_manager.shutdown(cancel_futures=True)
        print('Shutdown is triggered, terminating the resources gracefully')
