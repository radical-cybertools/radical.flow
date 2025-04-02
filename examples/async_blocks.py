import asyncio
from radical.flow import WorkflowEngine, ResourceEngine, Task
import time

async def main():
    # Create engine and workflow
    engine = ResourceEngine({'resource': 'local.localhost'})
    flow = WorkflowEngine(engine=engine)

    @flow
    async def task1(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 

    @flow
    async def task2(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 

    @flow
    async def task3(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 

    @flow
    async def task4(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 

    @flow
    async def task5(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 


    @flow.block
    async def block1(wf_id, *args):
        print(f'\nStarting workflow {wf_id}')
        t1 = await task1()
        tt = await task1()
        t2 = await task2(t1)
        t3 = await task3(t1, t2)
        t4 = await task4(t3)

        if await t4:
            t5 = await task5() 
            await t5

        return f'Workflow {wf_id} completed'


    @flow.block
    async def block2(wf_id, *args):
        print(f'\nStarting workflow {wf_id}')
        t1 = await task1()
        t2 = await task2(t1)
        t3 = await task3(t1, t2)
        t4 = await task4(t3)

        if await t4:
            t5 = await task5()
            await t5

        return f'Workflow {wf_id} completed'


    async def run_blocks(wf_id):

        b1 = await block1(wf_id)
        b2 = await block2(wf_id, b1)
        await b2


    # Run workflows concurrently
    results = await asyncio.gather(*[run_blocks(i) for i in range(1)])

    for result in results:
        print(result)
    
    engine.shutdown()

if __name__ == '__main__':
    asyncio.run(main())
