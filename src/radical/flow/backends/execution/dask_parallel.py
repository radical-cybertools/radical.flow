import asyncio
from typing import List, Dict, Any, Union, Optional, Callable
import dask
import typeguard
from dask.distributed import Client, Future as DaskFuture
from concurrent.futures import Future as ConcurrentFuture
from radical.flow.backends.execution.base import BaseExecutionBackend, Session

def _setup_worker_event_loop():
    """Initialize event loop on each worker."""
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

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
        self._client = None
        self.session = Session()
        self.tasks = {}  # Maps task UIDs to their futures
        self._callback = None  # State change callback
        self.initialize(resources)

    def initialize(self, resources) -> None:
        """Initialize the Dask client and set up worker environments."""
        try:
            self._client = Client(**resources)
            # Ensure workers can handle async functions
            self._client.run(_setup_worker_event_loop)
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
            self.tasks[task['uid']] = task
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

    def _submit_async_function(self, task: Dict[str, Any]) -> None:
        """Submit async function to Dask properly without managing event loops."""

        async def async_wrapper():
            return await task['function'](*task['args'], **task['kwargs'])

        # Submit the coroutine directly — Dask will await it correctly
        dask_future = self._client.submit(async_wrapper, pure=False)

        def on_dask_done(f: DaskFuture) -> None:
            try:
                result = f.result()
                task['return_value'] = result
                self._callback(task, 'DONE')
            except Exception as e:
                task['exception'] = str(e)
                self._callback(task, 'FAILED')

        dask_future.add_done_callback(on_dask_done)


    def _submit_sync_function(self, task: Dict[str, Any]) -> None:
        """Submit async function to Dask properly without managing event loops."""

        def sync_wrapper():
            return task['function'](*task['args'], **task['kwargs'])

        # Submit the coroutine directly — Dask will await it correctly
        dask_future = self._client.submit(sync_wrapper, pure=False)

        def on_dask_done(f: DaskFuture) -> None:
            try:
                result = f.result()
                task['return_value'] = result
                self._callback(task, 'DONE')
            except Exception as e:
                task['exception'] = str(e)
                self._callback(task, 'FAILED')

        dask_future.add_done_callback(on_dask_done)


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
