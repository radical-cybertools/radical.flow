RADICAL Flow (RF) is a synchronous and asynchronous workflow management layer. RF supports the management and execution of tasks, set of tasks with dependencies (workflows), sets of workflows with dependencies on other workflows (blocks). RF flows
the best practice of Python by enabling asynchronous behavior on the task, workflow, and blocks with adaptive execution behavior.
RF supports different execution backends such is `Radical.Pilot`,
`Dask.Parallel` and more. RF is agnostic to these execution backends meaning the user can easily extend it with their own custom execution mechanism.


## Basic Usage (sync)
```python
from radical.flow import Task
from radical.flow import WorkflowManager
from radical.flow import RadicalExecutionBackend

radical_backend = RadicalExecutionBackend({'resource': 'local.localhost'})
flow = WorkflowManager(backend=radical_backend)

@flow.executable_task
def task1(*args):
    return 'python task1.py'

@flow.function_task
def task2(*args):
    return 2 * 2


# create the workflow

t1_future = task1()
t2_future = task2(t2_future) # t2 depends on t1 (waits for it)

t2_result = t2.result()
print(t2_result)

radical_backend.shutdown()
```