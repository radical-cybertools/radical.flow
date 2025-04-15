import time
import asyncio

from radical.flow import Task
from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend


async def main():

    backend = RadicalExecutionBackend({'resource': 'local.localhost'})
    flow = WorkflowEngine(backend=backend)

    @flow
    async def task1(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date') 

    @flow
    async def task2(*args):
        return Task(executable='/bin/echo "I got executed at" && /bin/date')


    @flow.block
    async def block1(*args):
        t1 = task1()
        t2 = task2(t1)
        await t2
        print(f'block1 done at {time.time()}')

    @flow.block
    async def block2(*args):
        t3 = task1()
        t4 = task2(t3)
        await t4
        print(f'block2 done at {time.time()}')


    @flow.block
    async def block1_of_blocks(*args):
        b1 = block1()
        b2 = block2(b1)
        await b2
        print(f'block_of_blocks done at {time.time()}')


    @flow.block
    async def block2_of_blocks(*args):
        b1 = block1()
        b2 = block2(b1)
        await b2
        print(f'block_of_blocks done at {time.time()}')

    async def run_block_of_blocks(i):
        bob1 = block1_of_blocks()
        bob2 = block2_of_blocks(bob1)
        await bob2
        print(f'Block of block-{i} is finished')

    results = await asyncio.gather(*[run_block_of_blocks(i) for i in range(2)])

    backend.shutdown()


if __name__ == '__main__':
    asyncio.run(main())
