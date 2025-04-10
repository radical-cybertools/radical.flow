import os
from typing import Dict, Callable

from .base import Session, BaseExecutionBackend


class NoopExecutionBackend(BaseExecutionBackend):
    def __init__(self):
        self._session = Session()
        self.task_manager = TaskManager()
        print('Noop execution backend started successfully')

    def state(self):
        return 'IDLE'

    def task_state_cb(self):
        pass

    def submit_tasks(self, tasks):
        return self.task_manager.submit_tasks(tasks)

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
