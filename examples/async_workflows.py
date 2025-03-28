import time
from radical.flow import WorkflowEngine, ResourceEngine, Task


engine = ResourceEngine({'resource': 'local.localhost'})
flow = WorkflowEngine(engine=engine)

@flow
def task1(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date') 

@flow
def task2(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date')

@flow
def task3(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date')

@flow
def task4(*args):
    return Task(executable='echo "$(( RANDOM % 100 ))"')

@flow
def task5(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date')

@flow
def task6(*args):
    return Task(executable='/bin/echo "I got executed at" && /bin/date')

workflows = []

@flow.as_async
def run_wf(wf_id):
    t1 = task1()
    t2 = task2(t1) 
    t3 = task3(t2, t1)
    t4 = task4(t1, t3)
    t4_res = t4.result() # <-- this is a blocking call yet the workflow is non-blocking

    if eval(t4_res) % 2 == 0: # <-- this is decision making step
        print('Got even number submitting task 5 instead of 6')
        t5 = task5(t4, t2)
    else:
        print('Got odd number submitting task 6 instead of 5')
        t6 = task6(t4, t2)

    return (f'I am workflow {wf_id} and I am done at {time.time()}')

# run 5 blocking workflows in parallel instead of sequentially
for i in range(5):
    workflows.append(run_wf(i))

print([wf.result() for wf in workflows])

engine.shutdown()
