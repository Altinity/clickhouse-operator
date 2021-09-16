import testflows.settings as settings

from testflows.core import *

from multiprocessing.dummy import Pool
from multiprocessing import TimeoutError as PoolTaskTimeoutError

def join(tasks, timeout=None, polling=5):
    """Join all parallel tests.
    """
    exc = None

    for task in tasks:
        task._join_timeout = timeout

    while tasks:
        try:
            try:
                tasks[0].get(timeout=polling)
                tasks.pop(0)

            except PoolTaskTimeoutError as e:
                task = tasks.pop(0)
                if task._join_timeout is not None:
                    task._join_timeout -= polling
                    if task._join_timeout <= 0:
                        raise
                tasks.append(task)
                continue

        except KeyboardInterrupt as e:
            top().terminating = True
            raise

        except Exception as e:
            tasks.pop(0)
            if exc is None:
                exc = e
            top().terminating = True

    if exc is not None:
        raise exc

def start(pool, tasks, scenario, kwargs=None):
    """Start parallel test.
    """
    if kwargs is None:
        kwargs = {}

    task = pool.apply_async(scenario, [], kwargs)
    tasks.append(task)

    return task

def run_scenario(pool, tasks, scenario, kwargs=None):
    if kwargs is None:
        kwargs = {}

    _top = top()
    def _scenario_wrapper(**kwargs):
        if _top.terminating:
            return
        return scenario(**kwargs)

    if current().context.parallel:
        start(pool, tasks, _scenario_wrapper, kwargs)
    else:
        scenario(**kwargs)
