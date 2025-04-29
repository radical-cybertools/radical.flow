import typeguard
from typing import Dict
import radical.utils as ru
import radical.pilot as rp

from .base import BaseExecutionBackend

class RadicalExecutionBackend(BaseExecutionBackend):
    """
    The RadicalExecutionBackend class is responsible for managing computing resources
    and creating sessions for executing tasks on a large scale. It interfaces with
    different resource management systems such SLURM and FLUX on diverse HPC machines.
    This backend is capable of initialize sessions, manage task execution, and submit
    resources required for the workflow.

    Attributes:
        session (rp.Session): A session instance used to manage and track task execution,
            uniquely identified by a generated ID. This session serves as the primary context for
            all task and resource management within the workflow.

        task_manager (rp.TaskManager): Manages the lifecycle of tasks, handling their submission,
            tracking, and completion within the session.

        pilot_manager (rp.PilotManager): Manages computing resources, known as "pilots," which
            are dynamically allocated based on the provided resources. The pilot manager
            coordinates these resources to support task execution.

        resource_pilot (rp.Pilot): Represents the submitted computing resources as a pilot.
            This pilot is described by the `resources` parameter provided during initialization,
            specifying details such as CPU, GPU, and memory requirements.

    Parameters:
        resources (Dict): A dictionary specifying the resource requirements for the pilot,
            including details like the number of CPUs, GPUs, and memory. This dictionary
            configures the pilot to match the needs of the tasks that will be executed.

    Raises:
        Exception: If session creation, pilot submission, or task manager setup fails,
            the RadicalExecutionBackend will raise an exception, ensuring the resources are correctly
            allocated and managed.

    Example:
        ```python
        resources = {"cpu": 4, "gpu": 1, "memory": "8GB"}
        backend = RadicalExecutionBackend(resources)
        ```
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict, raptor_mode=False, raptor_config={}) -> None:
        try:
            self.session = rp.Session(uid=ru.generate_id('flow.session',
                                                          mode=ru.ID_PRIVATE))
            self.task_manager = rp.TaskManager(self.session)
            self.pilot_manager = rp.PilotManager(self.session)
            self.resource_pilot = self.pilot_manager.submit_pilots(rp.PilotDescription(resources))
            self.task_manager.add_pilots(self.resource_pilot)

            if raptor_mode:
                self.raptor_mode = True
                print('Enabling Raptor mode for RadicalExecutionBackend')
                if raptor_config:
                    self.setup_raptor_mode(raptor_config)
                else:
                    raise RuntimeError('"raptor_config" is required for RAPTOR mode')

            print('RadicalPilot execution backend started successfully\n')

        except Exception:
            print('RadicalPilot execution backend Failed to start, terminating\n')
            raise

        except (KeyboardInterrupt, SystemExit) as e:
            # the callback called sys.exit(), and we can here catch the
            # corresponding KeyboardInterrupt exception for shutdown.  We also catch
            # SystemExit (which gets raised if the main threads exits for some other
            # reason).
            exception_msg = f'Radical execution backend failed'
            exception_msg += f' internally, please check {self.session.path}'
            
            raise SystemExit(exception_msg) from e

    def setup_raptor_mode(self, raptor_config):
        """
        raptor_config = {
            'masters': [
                {
                    'executable': '/bin/bash',
                    'arguments': ['-c', 'echo Master running'],
                    'cpu_processes': 1,
                    'workers': [{'executable': '/bin/bash',
                                 'arguments': ['-c', 'echo Worker running'],
                                 'cpu_processes': 1},
                                {'executable': '/bin/bash',
                                 'arguments': ['-c', 'echo Another worker running'],
                                 'cpu_processes': 1,}]}]}
        """

        self.masters = []
        self.workers = []
        self.master_selector = self.select_master()

        cfg = copy.copy(raptor_config)
        masters = cfg['masters']

        for master_description in masters:
            workers = master_description['workers']
            master_description.pop('workers')

            md = rp.TaskDescription(master_description)
            md.uid = ru.generate_id('flow.master.%(item_counter)06d', ru.ID_CUSTOM,
                                     ns=self.session.uid)
            md.mode = rp.RAPTOR_MASTER
            master = self.resource_pilot.submit_raptors(md)[0]
            self.masters.append(master)

            for worker_description in workers:
                wd = rp.TaskDescription(worker_description)
                wd.uid = ru.generate_id('flow.worker.%(item_counter)06d', ru.ID_CUSTOM,
                                        ns=self.session.uid)
                wd.raptor_id = md.uid
                wd.mode = rp.RAPTOR_WORKER
                worker = master.submit_workers(wd)
                self.workers.append(worker)

    def select_master(self):
        """
        Balance tasks submission across N masters and N workers
        """
        current_master = 0
        masters_uids = [m.uid for m in self.masters]

        while True:
            yield masters_uids[current_master]
            current_master = (current_master + 1) % len(self.masters)

    def submit_tasks(self, tasks):
        if self.raptor_mode:
            for t in tasks:
                t.raptor_id = next(self.master_selector)
        return self.task_manager.submit_tasks(tasks)

    def state(self):
        """
        Retrieve the current state of the resource pilot.

        Returns:
            The current state of the resource pilot.
        """
        raise NotImplementedError

    def task_state_cb(self, task, state):
        """
        Callback function for handling task state changes.

        Args:
            task: The task object whose state has changed.
            state: The new state of the task.
        
        Note:
            This method is intended to be overridden or extended
            to perform specific actions when a task's state changes.
        """
        raise NotImplementedError

    def shutdown(self) -> None:
        """
        Gracefully shuts down the session, downloading any necessary data.

        This method ensures that the session is properly closed and any
        required data is downloaded before finalizing the shutdown.

        Returns:
            None
        """
        print('Shutdown is triggered, terminating the resources gracefully')
        self.session.close(download=True)
