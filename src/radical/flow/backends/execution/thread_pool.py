import os
import typeguard
import subprocess
import radical.utils as ru
from typing import Dict, Callable
from concurrent.futures import ThreadPoolExecutor, Future

from .base import Session, BaseExecutionBackend


class ThreadExecutionBackend(BaseExecutionBackend):
    @typeguard.typechecked
    def __init__(self, resources: Dict):
        self.tasks = {}
        self.session = Session()
        self.executor = ThreadPoolExecutor()
        self._callback_func: Callable[[Future], None] = lambda f: None
        print('ThreadPool execution backend started successfully')

    def state(self):
        pass

    def task_state_cb(self):
        pass

    def register_callback(self, func: Callable):
        self._callback_func = func

    def register_task(self, uid, task_desc, task_specific_kwargs):
        self.tasks[uid] = task_desc
    
    def link_explicit_data_deps(self, task_id, file_name=None):
        pass

    def link_implicit_data_deps(self, src_task):
        pass

    def _task_wrapper(self, task):
        if task['function']:
            try:
                # Execute the function with optional args/kwargs
                func = task['function']
                args = task.get('args', [])
                kwargs = task.get('kwargs', {})

                return_value = func(*args, **kwargs)

                task['return_value'] = return_value
                task['stdout'] = str(return_value)
                task['stderr'] = ''
                task['exit_code'] = 0
                state = 'DONE'
            except Exception as e:
                task['return_value'] = None
                task['stdout'] = ''
                task['stderr'] = str(e)
                task['exit_code'] = 1
                state = 'FAILED'
        else:
            exec_list = [task['executable']]
            exec_list.extend(task.get('arguments', []))

            result = subprocess.run(exec_list, capture_output=True,
                                    text=True, shell=True)

            task['stdout'] = result.stdout
            task['stderr'] = result.stderr
            task['exit_code'] = result.returncode
            state = 'DONE' if result.returncode == 0 else 'FAILED'

        return task, state

    def submit_tasks(self, task_uids: list):
        for task_uid in task_uids:
            task = self.tasks[task_uid]
            # Submit task to thread pool
            fut = self.executor.submit(self._task_wrapper, task)
            fut.add_done_callback(lambda f, task=task: self._callback_func(*f.result()))

    def shutdown(self) -> None:
        self.executor.shutdown(cancel_futures=True)
        print('Shutdown is triggered, terminating the resources gracefully')
