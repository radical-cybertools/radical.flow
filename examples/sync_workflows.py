import time

from radical.flow import Task
from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionEngine

engine = RadicalExecutionEngine({'resource': 'local.localhost'})
flow = WorkflowEngine(engine=engine)

@flow
def task1(*args):
    return Task(executable='/bin/echo $RP_TASK_NAME')

@flow
def task2(*args):
    return Task(executable='/bin/echo $RP_TASK_NAME')

@flow
def task3(*args):
    return Task(executable='/bin/echo $RP_TASK_NAME')


def run_wf(wf_id):

    print(f'Starting workflow {wf_id} at {time.time()}')
    
    t3 = task3(task1(), task2())
    print(t3.result())

    print(f'Workflow {wf_id} completed at {time.time()}')

for i in range(5):
    run_wf(i)

engine.shutdown()
