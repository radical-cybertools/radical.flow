import asyncio
from typing import List, Dict, Any, Union, Optional, Callable
import dask
import typeguard
from functools import wraps
from dask.distributed import Client, Future as DaskFuture
from concurrent.futures import Future as ConcurrentFuture
from radical.flow.backends.execution.base import BaseExecutionBackend, Session


class DaskExecutionBackend(BaseExecutionBackend):
    """
    A robust Dask execution backend supporting both synchronous and asynchronous functions.
    Handles task submission, dependency management, and proper event loop handling.
    """

    @typeguard.typechecked
    def __init__(self, resources: Optional[Dict] = None):
        """
        Initialize the Dask execution backend.
        
        Args:
            resources: Dictionary of resource requirements for tasks
        """
        self.tasks = {}
        self._client = None
        self._callback = None
        self.session = Session()
        self.initialize(resources)

    def initialize(self, resources) -> None:
        """Initialize the Dask client and set up worker environments."""
        try:
            self._client = Client(**resources)
            # Ensure workers can handle async functions
            #self._client.run(_setup_worker_event_loop)
            print(f"Dask backend initialized with dashboard at {self._client.dashboard_link}")
        except Exception as e:
            print(f"Failed to initialize Dask client: {str(e)}")
            raise

    def register_callback(self, callback: Callable) -> None:
        """Register a callback for task state changes."""
        self._callback = callback

    def shutdown(self) -> None:
        """Shutdown the Dask client and clean up resources."""
        if self._client is not None:
            try:
                # Close the client
                self._client.close()
                print("Dask client shutdown complete")
            except Exception as e:
                print(f"Error during shutdown: {str(e)}")
            finally:
                self._client = None
                self.tasks.clear()

    def submit_tasks(self, tasks: List[Dict[str, Any]]) -> None:
        """
        Submit tasks to Dask cluster, handling both sync and async functions.

        Args:
            tasks: List of task dictionaries containing:
                - uid: Unique task identifier
                - function: Callable to execute
                - args: Positional arguments
                - kwargs: Keyword arguments
                - async: Boolean indicating if function is async
        """
        for task in tasks:
            if not task['function'] and task['executable']:
                raise RuntimeError('DaskBackend is optimized for task functions only')

            self.tasks[task['uid']] = task
            
            # make sure we do not pass future object to Dask as it is not picklable
            task['args'] = tuple(arg for arg in task['args'] if not isinstance(arg,
                                               (ConcurrentFuture, asyncio.Future)))

            try:
                if task['async']:
                    self._submit_async_function(task)
                    print(f"Successfully submitted async task {task['uid']}")
                else:
                    self._submit_sync_function(task)
                    print(f"Successfully submitted sync task {task['uid']}")
            except Exception as e:
                print(f"Failed to submit task {task['uid']}: {str(e)}")
                raise

    def _submit_to_dask(self, task: Dict[str, Any], fn: Callable, *args) -> None:
        """Submit function to Dask and register completion callback."""
        def on_done(f: DaskFuture):
            try:
                result = f.result()
                task['return_value'] = result
                self._callback(task, 'DONE')
            except Exception as e:
                task['exception'] = str(e)
                self._callback(task, 'FAILED')

        dask_future = self._client.submit(fn, *args, pure=False)
        dask_future.add_done_callback(on_done)

    def _submit_async_function(self, task: Dict[str, Any]) -> None:
        """Submit async function to Dask."""
        
        # in dask dashboard we want the real task name not "async_wrapper"
        @wraps(task['function'])
        async def async_wrapper():
            return await task['function'](*task['args'], **task['kwargs'])

        self._submit_to_dask(task, async_wrapper)

    def _submit_sync_function(self, task: Dict[str, Any]) -> None:
        """Submit sync function to Dask."""

        # in dask dashboard we want the real task name not "sync_wrapper"
        @wraps(task['function'])
        def sync_wrapper(fn, args, kwargs):
            return fn(*args, **kwargs)

        self._submit_to_dask(task, sync_wrapper, task['function'], task['args'], task['kwargs'])

    def link_explicit_data_deps(self, source: str, target: str) -> Dict[str, str]:
        """Handle explicit data dependencies between tasks."""
        pass

    def link_implicit_data_deps(self, task_desc: Dict[str, Any]) -> None:
        """Handle implicit data dependencies for a task."""
        pass

    def state(self) -> str:
        pass

    def task_state_cb(self, task: dict, state: str) -> None:
        pass

    def build_task(self, task: dict) -> None:
        pass
