import os
from typing import Dict, Callable

from .base import Session, BaseExecutionBackend


class NoopExecutionBackend(BaseExecutionBackend):
    def __init__(self):
        self.session = Session()
        self.task_manager = TaskManager()
        print('Noop execution backend started successfully')

    def state(self):
        return 'IDLE'

    def task_state_cb(self):
        pass

    def register_callback(self, func: Callable):
        self.task_manager.register_callback(func)

    def submit_tasks(self, tasks):
        return self.task_manager.submit_tasks(tasks)
    
    def link_explicit_data_deps(self, task_id, file_name=None, file_path=None):
        # No-op: No explicit data dependencies in the Noop backend
        pass

    def link_implicit_data_deps(self, task):
        # No-op: No implicit data dependencies in the Noop backend
        pass

    def register_task(self, uid, task_desc, task_specific_kwargs):
        # No-op: No task registration in the Noop backend
        pass

    def shutdown(self) -> None:
        print('Dummy shutdown: Nothing to cleanup.')


class TaskManager:
    def __init__(self):
        self._callback_func: Callable = lambda task, state: None  # default no-op

    def submit_tasks(self, tasks: list):
        for task in tasks:
            task['stdout'] = "Dummy output"
            task['stderr'] = ""
            task['exit_code'] = 0
            state = "DONE"
            self._callback_func(task, state)

    def register_callback(self, func: Callable):
        self._callback_func = func
