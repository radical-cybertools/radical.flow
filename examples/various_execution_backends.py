from radical.flow import Task
from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionEngine, DaskExecutionBackend
from radical.flow import ThreadExecutionBackend, ProcessExecutionBackend

backends= {ThreadExecutionBackend: {},
           ProcessExecutionBackend: {},
           RadicalExecutionEngine: {'resource': 'local.localhost'},
           DaskExecutionBackend: {'n_workers': 2, 'threads_per_worker': 1}}


print('Running 1-layer funnel DAG workflow with each backend\n')
print("""
         task1      task2 <---- running in parallel 1st
             \\       /
               task3      <---- running last\n""")

def main():
    for backend, resource in backends.items():
        engine = backend(resource)
        flow = WorkflowEngine(engine=engine)

        @flow
        def task1(*args):
            return Task(executable='/bin/date')

        @flow
        def task2(*args):
            return Task(executable='/bin/date')

        @flow
        def task3(*args):
            return Task(executable='/bin/date')

        t3 = task3(task1(), task2())

        print(t3.result())

        engine.shutdown()

if __name__ == "__main__":
    main()
