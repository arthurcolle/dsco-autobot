from concurrent.futures import Future
import asyncio

class PrioritizedFuture(Future):
    """
    A Future subclass with additional attributes for priority, maximum duration, dependencies, and resource requirements.

    This class extends the standard Future to include information about the task's priority,
    the function to be executed, its arguments, maximum allowed duration, any dependencies
    it may have on other tasks, and its resource requirements.

    Attributes:
        priority (int): The priority of the task. Lower numbers indicate higher priority.
        func (callable): The function to be executed.
        args (tuple): Positional arguments to be passed to the function.
        kwargs (dict): Keyword arguments to be passed to the function.
        dependencies (set): A set of task names that must be completed before this task can start.
        max_duration (float): The maximum duration in seconds that the task is allowed to run.
        resource_requirements (dict): A dictionary specifying the resources required by this task.

    """

    def __init__(self, priority, func, *args, max_duration=None, dependencies=None, resource_requirements=None, **kwargs):
        """
        Initialize a PrioritizedFuture.

        Args:
            priority (int): The priority of the task. Lower numbers indicate higher priority.
            func (callable): The function to be executed.
            *args: Positional arguments to be passed to the function.
            max_duration (float, optional): The maximum duration in seconds that the task is
                allowed to run. If None, the task can run indefinitely.
            dependencies (set, optional): A set of task names that must be completed before
                this task can start.
            resource_requirements (dict, optional): A dictionary specifying the resources
                required by this task.
            **kwargs: Keyword arguments to be passed to the function.
        """
        super().__init__()
        self.priority = priority
        self.func = func
        self.args = args
        self.kwargs = kwargs
        self.dependencies = set(dependencies) if dependencies else set()
        self.max_duration = max_duration
        self.resource_requirements = resource_requirements or {}

    async def run(self):
        """
        Run the function and capture the result.

        This method executes the stored function with its arguments and sets the result
        or exception on the future. It's designed to be called by the executor.

        Returns:
            The result of the function call, if successful.

        Raises:
            Any exception raised by the function will be captured and set on the future.
        """
        if not self.set_running_or_notify_cancel():
            return
        try:
            result = await self.func(*self.args, **self.kwargs) if asyncio.iscoroutinefunction(self.func) else self.func(*self.args, **self.kwargs)
            self.set_result(result)
        except Exception as e:
            self.set_exception(e)

    def __lt__(self, other):
        """
        Compare this future with another based on priority.

        This method allows PrioritizedFuture objects to be sorted by priority,
        which is essential for use in a priority queue.

        Args:
            other (PrioritizedFuture): Another PrioritizedFuture to compare with.

        Returns:
            bool: True if this future has higher priority (lower priority number)
                  than the other, False otherwise.
        """
        return self.priority < other.priority
