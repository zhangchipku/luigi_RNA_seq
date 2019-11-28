from luigi.local_target import LocalTarget


class Requires:
    """Composition to replace :meth:`luigi.task.Task.requires`

    Example::

        class MyTask(Task):
            # Replace task.requires()
            requires = Requires()
            other = Requirement(OtherTask)

            def run(self):
                # Convenient access here...
                with self.other.output().open('r') as f:
                    ...

         MyTask().requires()
        {'other': OtherTask()}

    """

    def __get__(self, task, cls):
        if task is None:
            return self

        # Bind self/task in a closure
        return lambda: self(task)

    def __call__(self, task):
        """Returns the requirements of a task

        Assumes the task class has :class:`.Requirement` descriptors, which
        can clone the appropriate dependences from the task instance.

        :returns: requirements compatible with `task.requires()`
        :rtype: dict
        """
        # Search task.__class__ for Requirement instances
        # return
        task_keys = [k for k, v in task.__class__.__dict__.items() if isinstance(v, Requirement)]
        return {k: getattr(task, k) for k in task_keys}


class Requirement:
    def __init__(self, task_class, **params):
        self.task_class = task_class
        self.params = params

    def __get__(self, task, cls):
        if task is None:
            return self

        return task.clone(
            self.task_class,
            **self.params)


class TargetOutput:
    def __init__(self, file_pattern='{task.__class__.__name__}',
                 ext='.txt', target_class=LocalTarget, **target_kwargs):
        self.file_pattern = file_pattern
        self.ext = ext
        self.target_class = target_class
        self.target_kwargs = target_kwargs

    def __get__(self, task, cls):
        if task is None:
            return self
        return lambda: self(task)

    def __call__(self, task):
        # Determine the path etc here
        filename = self.file_pattern + self.ext

        return self.target_class(filename.format(task=task), **self.target_kwargs)
