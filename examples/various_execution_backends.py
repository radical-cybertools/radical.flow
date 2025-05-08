from radical.flow import WorkflowEngine
from radical.flow import RadicalExecutionBackend
from radical.flow import DaskExecutionBackend
from radical.flow import ThreadExecutionBackend

backends= {ThreadExecutionBackend : {'max_workers': 4},
           RadicalExecutionBackend: {'resource': 'local.localhost'},
           DaskExecutionBackend   : {'n_workers': 2, 'threads_per_worker': 1}}


print('Running 1-layer funnel DAG workflow with each backend\n')
print("""
         task1      task2 <---- running in parallel 1st
             \\       /
               task3      <---- running last\n""")

def main():
    for backend, resource in backends.items():
        backend = backend(resource)
        flow = WorkflowEngine(backend=backend)

        @flow.executable_task
        def task1(*args):
            return '/bin/date'

        @flow.executable_task
        def task2(*args):
            return '/bin/date'

        @flow.executable_task
        def task3(*args):
            return '/bin/date'

        t3 = task3(task1(), task2())

        print(t3.result())

        backend.shutdown()

if __name__ == "__main__":
    main()
