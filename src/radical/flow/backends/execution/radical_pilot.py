import typeguard
from typing import Dict
import radical.utils as ru
import radical.pilot as rp

from .base import BaseExecutionBackend

class RadicalExecutionEngine(BaseExecutionBackend):
    """
    The ResourceEngine class is responsible for managing computing resources and creating
    sessions for executing tasks. It interfaces with a resource management framework to
    initialize sessions, manage task execution, and submit resources required for the workflow.

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
            the ResourceEngine will raise an exception, ensuring the resources are correctly
            allocated and managed.

    Example:
        ```python
        resources = {"cpu": 4, "gpu": 1, "memory": "8GB"}
        engine = ResourceEngine(resources)
        ```
    """

    @typeguard.typechecked
    def __init__(self, resources: Dict) -> None:
        try:
            self._session = rp.Session(uid=ru.generate_id('flow.session',
                                                          mode=ru.ID_PRIVATE))
            self.task_manager = rp.TaskManager(self._session)
            self.pilot_manager = rp.PilotManager(self._session)
            self.resource_pilot = self.pilot_manager.submit_pilots(rp.PilotDescription(resources))
            self.task_manager.add_pilots(self.resource_pilot)

            print('RadicalPilot execution backend started successfully\n')

        except Exception:
            print('RadicalPilot execution backend Failed to start, terminating\n')
            raise

        except (KeyboardInterrupt, SystemExit) as e:
            # the callback called sys.exit(), and we can here catch the
            # corresponding KeyboardInterrupt exception for shutdown.  We also catch
            # SystemExit (which gets raised if the main threads exits for some other
            # reason).
            excp_msg = f'Resource engine failed internally, please check {self._session.path}'
            raise SystemExit(excp_msg) from e

    def submit_tasks(self, tasks):
        return self.task_manager.submit_tasks(tasks)
    
    def get_nodelist(self):
        pass

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
        self._session.close(download=True)
