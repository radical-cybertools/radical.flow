# flake8: noqa
import os
import asyncio
import logging
import threading
from typing import Callable, Dict, Optional

import radical.utils as ru
import radical.pilot as rp

from functools import wraps
from asyncio import Future as AsyncFuture
from concurrent.futures import Future as SyncFuture

import typeguard
from .task import Task as Block
from .data import InputFile, OutputFile

from radical.flow.backends.execution.noop import NoopExecutionBackend
from radical.flow.backends.execution.base import BaseExecutionBackend

TASK = 'task'
BLOCK = 'block'

class WorkflowEngine:
    """
    WorkflowEngine is an asynchronous workflow manager that uses asyncio event loops 
    and coroutines to manage and execute workflow components (blocks and/or tasks) 
    within Directed Acyclic Graph (DAG) or Chain Graph (CG) structures. It provides 
    support for async/await operations and handles task dependencies, input/output 
    data staging, and execution.

    Inputs:
        engine (BaseExecutionBackend): The backend used for task management/execution.
        dry_run : Enable dry run execution mode with no computational resources.
        jupyter_async: enable the execution of async tasks within Jupyter.
    Methods:
        __call__(func: Callable):
            Decorator to register workflow tasks.
        block(func: Callable):
            Decorator to register workflow blocks.
        _start_async_tasks():
            Starts asynchronous tasks in both synchronous and asynchronous contexts.
        shutdown_on_failure(func: Callable):
            Decorator that calls `shutdown` if an exception occurs in the decorated function.
        link_explicit_data_deps(task_id: str, file_name: Optional[str] = None) -> dict:
            Creates a dictionary linking explicit data dependencies between tasks.
        link_implicit_data_deps(src_task: Task) -> list:
            Generates commands to link implicit data dependencies for a source task.
        _detect_dependencies(possible_dependencies: list) -> tuple:
            Detects and categorizes possible dependencies into blocks/tasks, input files,
            and output files.
        _clear():
            Clears workflow components and their dependencies.
        async run():
            Manages the execution of workflow components by resolving dependencies and 
            submitting them for execution once they are resolved.
        async submit():
            Submits blocks or tasks from the queue for execution.
        async _submit_blocks(blocks: list):
            Submits blocks for execution.
        async execute_block(block_fut, func, *args, **kwargs):
            Executes a block function and updates its asyncio future.
        task_callbacks(task, state):
            Callback function to handle task state changes using asyncio.Future.
        _assign_uid(prefix: str) -> str:
            Generates a unique identifier (UID) for a flow component.
        _is_in_jupyter() -> bool:
            Detects if the code is running in a Jupyter Notebook environment.
        _set_loop():
            Detects and sets the asyncio event loop based on the execution context.
        _register_component(comp_fut, comp_type: str, func_obj: dict, comp_descriptions:
            dict, is_async: bool):
            Shared logic for registering tasks or blocks as workflow components.
    """

    @typeguard.typechecked
    def __init__(self, engine: Optional[BaseExecutionBackend] = None,
                 dry_run: bool = False, jupyter_async=None) -> None:

        self.loop = None
        self.running = []
        self.components = {}
        self.engine = engine
        self.resolved = set()
        self.dependencies = {}
        self.dry_run = dry_run
        self.unresolved = set()
        self.queue = asyncio.Queue()

        if self.engine is None:
            if self.dry_run:
                self.engine = NoopExecutionBackend()
            else:
                raise RuntimeError('An execution backend must be specified'
                                   ' when not in "dry_run" mode.')
        else:
            if self.dry_run and not isinstance(self.engine, NoopExecutionBackend):
                raise RuntimeError('Dry-run only supports the "NoopExecutionBackend".')

        self.jupyter_async = jupyter_async if jupyter_async is not None else \
                             os.environ.get('FLOW_JUPYTER_ASYNC', None)

        self.task_manager = self.engine.task_manager
        self._set_loop() # detect and set the event-loop 
        self._start_async_tasks() # start the solver and submitter

        self.task_manager.register_callback(self.task_callbacks)

        self._profiler = ru.Profiler(name='workflow_manager',
                                     ns='radical.flow', path=self.engine._session.path)

        logging.basicConfig(filename=f'{self.engine._session.path}/workflow_manager.log',
                            level=logging.DEBUG, format='%(asctime)s - %(levelname)s - %(message)s')

    def _is_in_jupyter(self):
        return "JPY_PARENT_PID" in os.environ

    def _set_loop(self):
        """
        Configure and set the asyncio event loop for the current context.

        This method determines the appropriate asyncio event loop to use based on
        the execution environment (e.g., Jupyter, IPython, or standard Python).
        It handles both synchronous and asynchronous execution modes and ensures
        that a valid event loop is set.

        Raises:
            ValueError: If running in a Jupyter environment and the `jupyter_async`
                        parameter or the `FLOW_JUPYTER_ASYNC` environment variable
                        is not set.
            RuntimeError: If no event loop could be obtained or created.

        Notes:
            - In Jupyter, the behavior depends on the `jupyter_async` parameter:
              - If `True`, the existing loop is reused for asynchronous execution.
              - If `False`, a new loop is created for synchronous execution.
            - In IPython, the existing loop is reused.
            - In standard Python, a new loop is created if none exists.
        """
        try:
            # get current loop if running
            loop = asyncio.get_running_loop()

            if loop and self._is_in_jupyter():
                # We can not detect if the user wants to execute
                # **sync/async** function unless we are instructed to, so we fail.
                if self.jupyter_async is None:
                    exception_msg = ('Jupyter requires async/sync mode to be '
                                     ' set via the "jupyter_async" parameter or '
                                     'the "FLOW_JUPYTER_ASYNC" environment variable.')
                    raise ValueError(exception_msg)

                elif isinstance(self.jupyter_async, str):
                    self.jupyter_async = True if self.jupyter_async == 'TRUE' else False

                if self.jupyter_async:
                    # Jupyter async context and runs **async** functions
                    self.loop = loop
                    logging.debug('Running within Async Jupyter and loop is found/re-used')
                else:
                    # Jupyter async context and runs **sync** functions
                    self.loop = asyncio.new_event_loop()
                    logging.debug('Running within Sync Jupyter and new loop is created')
            else:
                # IPython async context and runs **async/sync** functions
                self.loop = loop
                logging.debug('Running within IPython loop is found/re-used')

        except RuntimeError:
            # Python sync context and runs **async/sync** functions
            self.loop = asyncio.new_event_loop()    # create a new loop if none exists
            logging.debug('No loop was found, new loop is created/set')

        if not self.loop:
            raise RuntimeError('Failed to obtain or create a new event-loop for unknown reason')

        asyncio.set_event_loop(self.loop)
        logging.debug('Event-Loop is set successfully')

    def _start_async_tasks(self):
        """Starts async tasks in both sync and async contexts."""
        def _start():
            self.loop.create_task(self.submit())  # let's schedule submit() in loop
            self.loop.create_task(self.run())     # let's schedule run() in loop
            if not self.loop.is_running():
                self.loop.run_forever()  # keep the event loop alive

        # case1: called from an async namespace
        if self.loop.is_running():
            asyncio.create_task(self.submit())
            asyncio.create_task(self.run())
        else:
            # case2: called from a sync namespace
            thread = threading.Thread(target=_start, daemon=True)
            thread.start()

    @typeguard.typechecked
    def __call__(self, func: Callable):
        """Decorator to register workflow tasks."""
        return self._handle_flow_component_registration(func, comp_type=TASK)

    @typeguard.typechecked
    def block(self, func: Callable):
        """Decorator to register workflow blocks."""
        return self._handle_flow_component_registration(func, comp_type=BLOCK)

    def _handle_flow_component_registration(self, func: Callable, comp_type: str):
        """Universal decorator logic for both tasks and blocks."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            is_async = asyncio.iscoroutinefunction(func)

            func_obj = {'func': func, 'args': args, 'kwargs': kwargs}    
            if is_async:
                comp_fut = AsyncFuture()
                async def async_wrapper():
                    comp_desc = await func(*args, **kwargs) if comp_type == TASK else Block()
                    return self._register_component(comp_fut, comp_type, func_obj,
                                                    comp_desc, is_async=True)
                asyncio.create_task(async_wrapper())
                return comp_fut
            else:
                comp_fut = SyncFuture()
                comp_desc = func(*args, **kwargs) if comp_type == TASK else Block()
                self._register_component(comp_fut, comp_type, func_obj,
                                         comp_desc, is_async=False)
                return comp_fut

        return wrapper

    def _register_component(self, comp_fut, comp_type: str, func_obj: dict,
                            comp_descriptions: dict, is_async: bool):
        """Shared task/block registration logic."""
        comp_descriptions['name'] = func_obj['func'].__name__

        if comp_type == BLOCK:
            comp_descriptions['args'] = func_obj['args']
            comp_descriptions['kwargs'] = func_obj['kwargs']
            comp_descriptions['function'] = func_obj['func']

        comp_descriptions['uid'] = self._assign_uid(prefix=comp_type)

        comp_deps, input_files_deps, output_files_deps = self._detect_dependencies(func_obj['args'])

        comp_descriptions['metadata'] = {'dependencies': comp_deps,
                                         'input_files' : input_files_deps,
                                         'output_files': output_files_deps}

        comp_fut.id = comp_descriptions['uid'].split(f'{comp_type}.')[1]
        setattr(comp_fut, comp_type, comp_descriptions)

        self.components[comp_descriptions['uid']] = {'future': comp_fut,
                                                     'description': comp_descriptions}

        self.dependencies[comp_descriptions['uid']] = comp_deps

        logging.debug(f"Registered {comp_type}: '{comp_descriptions['name']}' with id of {comp_descriptions['uid']}")

        return comp_fut

    @staticmethod
    def shutdown_on_failure(func: Callable):
        """
        Decorator that calls `shutdown` if an exception occurs in the decorated function.
        """
        def wrapper(self, *args, **kwargs):
            try:
                return func(self, *args, **kwargs)
            except Exception as e:
                logging.exception('Internal failure is detected, shutting down the execution backend')
                self.engine.shutdown()  # Call shutdown on exception
                raise e
        return wrapper

    def _assign_uid(self, prefix):
        """
        Generates a unique identifier (UID) for a flow component.

        This method generates a custom flow component UID based on the format
        `task.%(item_counter)06d` and assigns it a session-specific namespace
        using the engine session UID.

        Returns:
            str: The generated unique identifier for the flow component.
        """
        uid = ru.generate_id(prefix, ru.ID_SIMPLE)

        return uid

    def link_explicit_data_deps(self, task_id, file_name=None):
        """
        Creates a dictionary linking explicit data dependencies between tasks.

        This method defines the source and target for data transfer between tasks, 
        using the provided task ID and optional file name. If no file name is provided, 
        the task ID is used as the file name.

        Args:
            task_id (str): The task ID to link data dependencies from.
            file_name (str, optional): The file name to be used in the data dependency. 
                                        Defaults to None, in which case the task_id is used.

        Returns:
            dict: A dictionary containing the data dependencies, including source, 
                target, and transfer action.
        """
        if not file_name:
            file_name = task_id

        data_deps = {'source': f"pilot:///{task_id}/{file_name}",
                     'target': f"task:///{file_name}", 'action': rp.LINK}

        return data_deps

    def link_implicit_data_deps(self, src_task):
        """
        Generates commands to link implicit data dependencies for a source task.

        This method creates shell commands to copy files from the source task's sandbox
        to the current task's sandbox, excluding task-related files. The commands are 
        returned as a list of Python shell commands to execute in the task environment.

        Args:
            src_task (Task): The source task whose files are to be copied.

        Returns:
            list: A list of shell commands to link implicit data dependencies between tasks.
        """
        cmd1 = f'export SRC_TASK_ID={src_task.uid}'
        cmd2 = f'export SRC_TASK_SANDBOX="$RP_PILOT_SANDBOX/$SRC_TASK_ID"'

        cmd3 = '''files=$(cd "$SRC_TASK_SANDBOX" && ls | grep -ve "^$SRC_TASK_ID")
                  for f in $files
                  do 
                     ln -sf "$SRC_TASK_SANDBOX/$f" "$RP_TASK_SANDBOX"
                  done'''

        commands = [cmd1, cmd2, cmd3]

        return commands

    def _detect_dependencies(self, possible_dependencies):
        """
        Detects and categorizes possible dependencies into blocks/tasks, input files,
        and output files.

        This method iterates over a list of possible dependencies and classifies
        them into three categories:
        - Blocks/Tasks that are instances of `Future` with a `task` or `block` attribute
          (task dependencies).
        - Input files that are instances of `InputFile` (files required by the task only).
        - Output files that are instances of `OutputFile` (files produced by the task only).

        Args:
            possible_dependencies (list): A list of possible dependencies, which can include
            blocks, tasks, input files, and output files.

        Returns:
            tuple: A tuple containing three lists:
                - `dependencies`: A list of flow components that need to be completed.
                - `input_files`: A list of input file names that need to be fetched.
                - `output_files`: A list of output file names that need to be retrieved
                   from the task folder.
        """
        dependencies = []
        input_files = []
        output_files = []

        for possible_dep in possible_dependencies:
            # it is a flow component deps
            if isinstance(possible_dep, SyncFuture) or isinstance(possible_dep, AsyncFuture):
                if hasattr(possible_dep, TASK):
                    possible_dep = possible_dep.task
                elif hasattr(possible_dep, BLOCK):
                    possible_dep = possible_dep.block
                dependencies.append(possible_dep)
            # it is input file needs to be obtained from somewhere
            elif isinstance(possible_dep, InputFile):
                input_files.append(possible_dep.filename)
            # it is output file needs to be obtained from the task folder
            elif isinstance(possible_dep, OutputFile):
                output_files.append(possible_dep.filename)

        return dependencies, input_files, output_files

    def _clear(self):
        """
        clear workflow component and their deps
        """

        self.components.clear()
        self.dependencies.clear()

    async def run(self):
        """
        Async method to manage the execution of workflow components by resolving
        dependencies and submitting then for execution once they are resolved.

        This method continuously checks for unresolved components, evaluates their
        dependencies, and prepares them for submission if all dependencies are resolved.
        It also handles input and output data staging for the components.

        Workflow:
        - Identifies unresolved components.
        - Checks if their dependencies are resolved and their associated tasks are completed.
        - Prepares components for execution by setting up pre-execution commands and
          input staging based on dependencies.
        - Submits components that are ready for execution to the queue.

        Attributes:
            unresolved (set): A set of component UIDs that have unresolved dependencies.
            resolved (set): A set of component UIDs whose dependencies are resolved.
            running (list): A list of component UIDs that are currently running.
            dependencies (dict): A mapping of component UIDs to their dependency information.
            components (dict): A mapping of component UIDs to their descriptions and futures.
            queue (asyncio.Queue): A queue to submit components ready for execution.

        Raises:
            asyncio.CancelledError: If the coroutine is cancelled during execution.

        Notes:
            - This method runs indefinitely until cancelled.
            - It uses a sleep interval to avoid busy-waiting.
        """

        while True:
            self.unresolved = set(self.dependencies.keys())

            if not self.unresolved:
                await asyncio.sleep(0.1)
                continue

            to_submit = []

            for comp_uid in list(self.unresolved):
                if self.components[comp_uid]['future'].done():
                    self.resolved.add(comp_uid)
                    self.unresolved.remove(comp_uid)
                    continue

                if comp_uid in self.running:
                    continue

                dependencies = self.dependencies[comp_uid]
                if all(dep['uid'] in self.resolved and self.components[dep['uid']]['future'].done() for dep in dependencies):
                    comp_desc = self.components[comp_uid]['description']

                    pre_exec = []
                    input_staging = []

                    for dep in dependencies:
                        dep_desc = self.components[dep['uid']]['description']

                        if not dep_desc.metadata.get('output_files'):
                            pre_exec.extend(self.link_implicit_data_deps(dep_desc))

                        for output_file in dep_desc.metadata['output_files']:
                            if output_file in comp_desc.metadata['input_files']:
                                input_staging.append(self.link_explicit_data_deps(dep['uid'], output_file))

                    for input_file in comp_desc.metadata['input_files']:
                        _data_target = [item['target'].split('/')[-1] for item in input_staging]
                        if input_file not in _data_target:
                            input_staging.append({'source': input_file,
                                                 'target': f"task:///{input_file}",
                                                 'action': rp.TRANSFER})

                    comp_desc.pre_exec = pre_exec
                    comp_desc.input_staging = input_staging
                    to_submit.append(comp_desc)

                    msg = f"Ready to submit: {comp_desc.name}"
                    msg += f" with resolved dependencies: {[dep['name'] for dep in dependencies]}"
                    logging.debug(msg)

            if to_submit:
                await self.queue.put(to_submit)
                for t in to_submit:
                    self.running.append(t.uid)
                    self.resolved.add(t.uid)
                    self.unresolved.remove(t.uid)

            await asyncio.sleep(0.5)

    async def submit(self):
        """Async method to submit blocks or tasks from the queue for execution."""
        while True:
            try:
                objects = await asyncio.wait_for(self.queue.get(), timeout=1)

                tasks = [t for t in objects if t and BLOCK not in t['uid']]
                blocks = [b for b in objects if b and TASK not in b['uid']]

                logging.debug(f'Submitting {[b.name for b in objects]} for execution')

                if tasks:
                    self.task_manager.submit_tasks(tasks)
                if blocks:
                    await self._submit_blocks(blocks)

            except asyncio.TimeoutError:
                await asyncio.sleep(0.5)
            except Exception as e:
                logging.exception(f"Error in submit: {e}")
                raise

    async def _submit_blocks(self, blocks: list):
        """Async method to submit blocks for execution."""
        for block in blocks:
            args = block['args']
            kwargs = block['kwargs']
            func = block['function']
            block_fut = self.components[block['uid']]['future']

            # Execute the block function as a coroutine
            asyncio.create_task(self.execute_block(block_fut, func, *args, **kwargs))

    async def execute_block(self, block_fut, func, *args, **kwargs):
        """Async method to execute block function and update asyncio future."""
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                # If function is not async, run it in executor
                result = await self.loop.run_in_executor(None, func, *args, **kwargs)

            if not block_fut.done():
                block_fut.set_result(result)
        except Exception as e:
            if not block_fut.done():
                block_fut.set_exception(e)

    def task_callbacks(self, task, state):
        """
        Callback function to handle task state changes using asyncio.Future.
        """
        task_fut = self.components[task.uid]['future']

        if state == rp.DONE:
            logging.debug(f'{task.uid} is DONE')
            if not task_fut.done():
                task_fut.set_result(task.stdout)
            self.running.remove(task.uid)
        elif state in [rp.FAILED, rp.CANCELED]:
            logging.debug(f'{task.uid} is FAILED')
            if not task_fut.done():
                task_fut.set_exception(Exception(task.stderr))
            self.running.remove(task.uid)
