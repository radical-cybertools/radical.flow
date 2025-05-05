import os

from abc import ABC, abstractmethod
from typing import List

class BaseExecutionBackend(ABC):

    @abstractmethod
    def submit_tasks(self, tasks: List[dict]) -> None:
        pass

    @abstractmethod
    def shutdown(self) -> None:
        pass

    @abstractmethod
    def state(self) -> str:
        pass

    @abstractmethod
    def task_state_cb(self, task: dict, state: str) -> None:
        pass

    @abstractmethod
    def register_callback(cls, func) -> None:
        pass


class Session():
    def __init__(self):
        self.path = os.getcwd()
