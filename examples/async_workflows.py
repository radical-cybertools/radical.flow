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

    async def run_wf(wf_id):
        print(f'\nStarting workflow {wf_id}')
        t1 = await task1()
        t2 = await task2(t1)
        t3 = await task3(t1, t2)
        t4 = await task4(t3)

        if await t4:
            t5 = await task5() 
            await t5

        return f'Workflow {wf_id} completed'

    # Run workflows concurrently
    results = await asyncio.gather(*[run_wf(i) for i in range(1024)])
    
    for result in results:
        print(result)
    
    engine.shutdown()

if __name__ == '__main__':
    asyncio.run(main())