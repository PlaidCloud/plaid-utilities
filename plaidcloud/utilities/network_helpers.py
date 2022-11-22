#!/usr/bin/env python
# coding=utf-8

import random
import time

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

def retry_random_exp(fn, retries=5, current_exp=1):
    """Runs a function, retrying with a random exponential backoff.

    This is useful for trying to slow down to match a rate limit. The random
    element is to avoid clumping of parallel requests.

    Args:
        fn (function): A 0 argument function to run.
        retries (int): Number of times to retry.
        current_exp (int): Used on internal recursion. Tracks the current
        exponential factor for wait.

    Returns:
        The return value of function. If retries are exhausted, raises the
        final Exception.

    Examples:

        >>> from minimock import mock
        >>> mock('time.sleep', tracker=None)
        >>> def good(): return 0/1.0
        >>> retry_random_exp(good)
        0.0
        >>> def bad(): return 0/0.0
        >>> retry_random_exp(bad)
        Traceback (most recent call last):
            ...
        ZeroDivisionError: float division by zero
        >>> import itertools
        >>> count = itertools.count()  # Uses a closure to create a stateful function
        >>> def badthengood(): return 0/0.0 if next(count) < 2 else 0/1.0  # Fails the first 2 times, succeeds the third time.
        >>> retry_random_exp(badthengood)
        0.0
    """
    try:
        return fn()
    except:
        if retries == 0:
            raise
        else:
            random_factor = random.uniform(1.0, 2.0)  # Measured in seconds
            wait = current_exp * random_factor  # Also measured in seconds
            time.sleep(wait)
            return retry_random_exp(fn, retries-1, current_exp*2)
