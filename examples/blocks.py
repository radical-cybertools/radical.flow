from radical.flow import WorkflowEngine, ResourceEngine, Task
import time

engine = ResourceEngine({'resource': 'local.localhost'})
flow = WorkflowEngine(engine=engine)


@flow
def task1(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date') 

@flow
def task2(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date')


@flow.block
def block1(*args):
    t1 = task1()
    t2 = task2(t1)
    t2.result()
    print(f'block1 done at {time.time()}')

@flow.block
def block2(*args):
    t3 = task1()
    t4 = task2(t3)
    t4.result()
    print(f'block2 done at {time.time()}')


b1 = block1()
b2 = block2(b1)

b2.result()

engine.shutdown()
