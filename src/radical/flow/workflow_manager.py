# flake8: noqa
import asyncio
import inspect
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

from radical.flow.backends.execution.radical_pilot import ResourceEngine

TASK = 'task'
BLOCK = 'block'

class WorkflowEngine:
    """
    Asynchronous WorkflowEngine using asyncio event loop and coroutines.
    Manages and executes tasks within DAG/CG structures with async/await support.
    """
    
    @typeguard.typechecked
    def __init__(self, engine: ResourceEngine) -> None:
        self.tasks = {}
        self.running = []
        self.engine = engine
        self.resolved = set()
        self.dependencies = {}
        self.unresolved = set()
        self.queue = asyncio.Queue()
        self.task_manager = self.engine.task_manager

        try:
            self.loop = asyncio.get_running_loop()  # get current loop if running
        except RuntimeError:
            self.loop = asyncio.new_event_loop()    # create a new loop if none exists
            asyncio.set_event_loop(self.loop)       # set as the default loop
        
        self.start_async_tasks()

        self.task_manager.register_callback(self.task_callbacks)

    
    def start_async_tasks(self):
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
        return self._handle_flow_component_registration(func, TASK)

    @typeguard.typechecked
    def block(self, func: Callable):
        """Decorator to register workflow blocks."""
        return self._handle_flow_component_registration(func, BLOCK)

    def _handle_flow_component_registration(self, func: Callable, comp_type: str):
        """Universal decorator logic for both tasks and blocks."""
        @wraps(func)
        def wrapper(*args, **kwargs):
            is_async = asyncio.iscoroutinefunction(func) or \
                         inspect.iscoroutinefunction(wrapper)
            func_obj = {'func': func, 'args': args, 'kwargs': kwargs}

            if is_async:
                async def async_wrapper():
                    comp_desc = await func(*args, **kwargs) if comp_type == TASK else Block()
                    return self._register_component(comp_type, func_obj,
                                                    comp_desc, is_async=True)
                return async_wrapper()
            else:
                comp_desc = func(*args, **kwargs) if comp_type == TASK else Block()
                return self._register_component(comp_type, func_obj,
                                                comp_desc, is_async=False)

        return wrapper


    def _register_component(self, comp_type: str, func_obj: dict,
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

        # Create appropriate future type
        comp_fut = AsyncFuture() if is_async else SyncFuture()
        comp_fut.id = comp_descriptions['uid'].split(f'{comp_type}.')[1]
        setattr(comp_fut, comp_type, comp_descriptions)

        self.tasks[comp_descriptions['uid']] = {'future': comp_fut,
                                                'description': comp_descriptions}

        self.dependencies[comp_descriptions['uid']] = comp_deps

        print(f"Registered {comp_type}: '{comp_descriptions['name']}' with id of {comp_descriptions['uid']}")

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
                print('Internal failure is detected, shutting down the resource engine')
                self.engine.shutdown()  # Call shutdown on exception
                raise e
        return wrapper

    def _assign_uid(self, prefix):
        """
        Generates a unique identifier (UID) for a task.

        This method generates a custom task UID based on the format `task.%(item_counter)06d`
        and assigns it a session-specific namespace using the engine session UID.

        Returns:
            str: The generated unique identifier for the task.
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
        Detects and categorizes possible dependencies into tasks, input files, and output files.

        This method iterates over a list of possible dependencies and classifies them into three
          categories:
        - Tasks that are instances of `Future` with a `task` attribute (task dependencies).
        - Input files that are instances of `InputFile` (files required by the task).
        - Output files that are instances of `OutputFile` (files produced by the task).

        Args:
            possible_dependencies (list): A list of possible dependencies, which can include
            tasks, input files, and output files.

        Returns:
            tuple: A tuple containing three lists:
                - `dependencies`: A list of tasks that need to be completed.
                - `input_files`: A list of input file names that need to be fetched.
                - `output_files`: A list of output file names that need to be retrieved
                   from the task folder.
        """
        dependencies = []
        input_files = []
        output_files = []

        for possible_dep in possible_dependencies:
            # it is a task deps
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
        clear workflow tasks and their deps
        """

        self.tasks.clear()
        self.dependencies.clear()

    async def run(self):
        """Async method to resolve dependencies and submit tasks."""
        while True:
            self.unresolved = set(self.dependencies.keys())

            if not self.unresolved:
                await asyncio.sleep(0.1)
                continue

            to_submit = []

            for task_uid in list(self.unresolved):
                if self.tasks[task_uid]['future'].done():
                    self.resolved.add(task_uid)
                    self.unresolved.remove(task_uid)
                    continue

                if task_uid in self.running:
                    continue

                dependencies = self.dependencies[task_uid]
                if all(dep['uid'] in self.resolved and self.tasks[dep['uid']]['future'].done() for dep in dependencies):
                    task_desc = self.tasks[task_uid]['description']

                    pre_exec = []
                    input_staging = []

                    for dep in dependencies:
                        dep_desc = self.tasks[dep['uid']]['description']

                        if not dep_desc.metadata.get('output_files'):
                            pre_exec.extend(self.link_implicit_data_deps(dep_desc))

                        for output_file in dep_desc.metadata['output_files']:
                            if output_file in task_desc.metadata['input_files']:
                                input_staging.append(self.link_explicit_data_deps(dep['uid'], output_file))

                    for input_file in task_desc.metadata['input_files']:
                        _data_target = [item['target'].split('/')[-1] for item in input_staging]
                        if input_file not in _data_target:
                            input_staging.append({'source': input_file,
                                                 'target': f"task:///{input_file}",
                                                 'action': rp.TRANSFER})

                    task_desc.pre_exec = pre_exec
                    task_desc.input_staging = input_staging
                    to_submit.append(task_desc)

                    msg = f"Ready to submit: {task_desc.name}"
                    msg += f" with resolved dependencies: {[dep['name'] for dep in dependencies]}"
                    print(msg)

            if to_submit:
                await self.queue.put(to_submit)
                for t in to_submit:
                    self.running.append(t.uid)
                    self.resolved.add(t.uid)
                    self.unresolved.remove(t.uid)

            await asyncio.sleep(0.5)

    async def submit(self):
        """Async method to submit tasks from the queue for execution."""
        while True:
            try:
                objects = await asyncio.wait_for(self.queue.get(), timeout=1)

                tasks = [t for t in objects if t and BLOCK not in t['uid']]
                blocks = [t for t in objects if t and TASK not in t['uid']]

                print(f'Submitting {[b.name for b in objects]} for execution')

                if tasks:
                    self.task_manager.submit_tasks(tasks)
                if blocks:
                    await self._submit_blocks(blocks)

            except asyncio.TimeoutError:
                await asyncio.sleep(0.5)
            except Exception as e:
                print(f"Error in submit: {e}")
                raise


    async def _submit_blocks(self, blocks: list):
        """Async method to submit blocks for execution."""
        for block in blocks:
            args = block['args']
            kwargs = block['kwargs']
            func = block['function']
            block_fut = self.tasks[block['uid']]['future']

            # Execute the block function as a coroutine
            asyncio.create_task(self.execute_block(block_fut, func, *args, **kwargs))

    async def execute_block(self, block_fut, func, *args, **kwargs):
        """Async method to execute block function and update asyncio future."""
        try:
            if asyncio.iscoroutinefunction(func):
                result = await func(*args, **kwargs)
            else:
                # If function is not async, run it in executor
                print(func)
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
        task_fut = self.tasks[task.uid]['future']

        if state == rp.DONE:
            print(f'{task.uid} is DONE')
            if not task_fut.done():
                task_fut.set_result(task.stdout)
            self.running.remove(task.uid)
        elif state in [rp.FAILED, rp.CANCELED]:
            print(f'{task.uid} is FAILED')
            if not task_fut.done():
                task_fut.set_exception(Exception(task.stderr))
            self.running.remove(task.uid)
