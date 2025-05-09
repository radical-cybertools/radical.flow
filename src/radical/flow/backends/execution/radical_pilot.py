import copy
import typeguard
from typing import Dict, Optional
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
            the RadicalExecutionBackend will raise an exception, ensuring the resources
            are correctly allocated and managed.

    Example:
        ```python
        resources = {"cpu": 4, "gpu": 1, "memory": "8GB"}
        backend = RadicalExecutionBackend(resources)
        ```
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict, raptor_config: Optional[Dict] = None) -> None:
        """
        Initialize the RadicalExecutionBackend with the given resources and optional
        Raptor configuration.
        Args:
            resources (Dict): A dictionary specifying the resource configuration
                for the Radical Pilot session.
            raptor_config (Optional[Dict]): An optional dictionary containing
                configuration for enabling Raptor mode. Defaults to None.
        Raises:
            Exception: If the RadicalPilot execution backend fails to start.
            SystemExit: If a KeyboardInterrupt or SystemExit is encountered during
                initialization, providing a message with the session path for debugging.
        Notes:
            - Initializes a Radical Pilot session, task manager, and pilot manager.
            - Submits pilots based on the provided resource configuration.
            - Adds the pilots to the task manager.
            - Enables Raptor mode if a configuration is provided.
        """
        raptor_config = raptor_config or {}
        try:
            self.raptor_mode = False
            self.session = rp.Session(uid=ru.generate_id('flow.session',
                                                          mode=ru.ID_PRIVATE))
            self.task_manager = rp.TaskManager(self.session)
            self.pilot_manager = rp.PilotManager(self.session)
            self.resource_pilot = self.pilot_manager.submit_pilots(rp.PilotDescription(resources))
            self.task_manager.add_pilots(self.resource_pilot)

            if raptor_config:
                self.raptor_mode = True
                print('Enabling Raptor mode for RadicalExecutionBackend')
                self.setup_raptor_mode(raptor_config)

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
        Sets up the Raptor mode by configuring and submitting master and worker tasks.

        This method initializes the Raptor mode by creating and submitting master tasks
        and their associated worker tasks to the resource pilot. The configuration for
        the masters and workers is provided through the `raptor_config` dictionary.

        Args:
            raptor_config (dict): A dictionary containing the configuration for the
                Raptor mode. The structure of the dictionary is as follows:
                            'executable': str,  # Path to the master executable
                            'arguments': list,  # List of arguments for the master
                            'ranks': int,  # Number of ranks (CPU processes) for the master
                            'workers': [  # List of worker configurations
                                    'executable': str,  # Path to the worker executable
                                    'arguments': list,  # List of arguments for the worker
                                    'ranks': int  # Number of ranks (CPU processes) for the worker
                                },
                                ...
                            ]
                        },
                        ...
                    ]
                }

        Attributes:
            masters (list): A list of master tasks created and submitted.
            workers (list): A list of worker tasks created and submitted.
            master_selector (callable): A callable used to select a master.

        Steps:
            1. Deep copies the `raptor_config` to avoid modifying the original.
            2. Iterates through the master configurations in `raptor_config['masters']`.
            3. Extracts and removes the worker configurations from each master configuration.
            4. Creates and submits a master task using the `rp.TaskDescription`.
            5. Iterates through the worker configurations and creates worker tasks
               associated with the corresponding master.
            6. Submits the worker tasks to the master and stores them in the `workers` list.

        Raises:
            Any exceptions raised by the `rp.TaskDescription` or submission methods
            will propagate to the caller.
        """

        self.masters = []
        self.workers = []
        self.master_selector = self.select_master()

        cfg = copy.deepcopy(raptor_config)
        masters = cfg['masters']

        for master_description in masters:
            workers = master_description.pop('workers')

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
        if not self.raptor_mode or not self.masters:
            raise RuntimeError('Raptor mode is not enabled or no masters available')

        current_master = 0
        masters_uids = [m.uid for m in self.masters]

        while True:
            yield masters_uids[current_master]
            current_master = (current_master + 1) % len(self.masters)

    def register_callback(self, func):
        return self.task_manager.register_callback(func)

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
