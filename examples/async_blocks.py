from radical.flow import WorkflowEngine, ResourceEngine, Task
import time
import asyncio

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
    print(f'block1 done at {time.time()}')

@flow.block
def block2(*args):
    t3 = task1()
    t4 = task2(t3)
    print(f'block2 done at {time.time()}')


async def run_blocks():
    b1_fut = block1()
    b2_fut = block2(b1_fut)

async def main():
    tasks = [run_blocks() for _ in range(1)]  # Launch 10 concurrent workflows
    await asyncio.gather(*tasks)  # Run them concurrently
    engine.shutdown()

asyncio.run(main())  # Run the event loop
