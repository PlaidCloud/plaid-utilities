# coding=utf-8
"""Tests for plaidcloud.utilities.network_helpers."""

import unittest
from unittest import mock

from plaidcloud.utilities import network_helpers


class TestRetryRandomExp(unittest.TestCase):

    def test_returns_value_from_successful_function(self):
        result = network_helpers.retry_random_exp(lambda: 42)
        self.assertEqual(result, 42)

    def test_reraises_exception_after_exhausted_retries(self):
        call_count = {'n': 0}

        def always_fails():
            call_count['n'] += 1
            raise ValueError('boom')

        with mock.patch.object(network_helpers.time, 'sleep'):
            with self.assertRaises(ValueError):
                network_helpers.retry_random_exp(always_fails, retries=3)

        # Initial call + 3 retries
        self.assertEqual(call_count['n'], 4)

    def test_recovers_after_transient_failures(self):
        call_count = {'n': 0}

        def flaky():
            call_count['n'] += 1
            if call_count['n'] < 3:
                raise RuntimeError('not yet')
            return 'ok'

        with mock.patch.object(network_helpers.time, 'sleep'):
            result = network_helpers.retry_random_exp(flaky, retries=5)

        self.assertEqual(result, 'ok')
        self.assertEqual(call_count['n'], 3)

    def test_exponential_backoff_grows(self):
        waits = []

        def always_fails():
            raise RuntimeError('boom')

        def fake_sleep(seconds):
            waits.append(seconds)

        # Force the random factor to be deterministic.
        with mock.patch.object(network_helpers.random, 'uniform', return_value=1.5), \
             mock.patch.object(network_helpers.time, 'sleep', side_effect=fake_sleep):
            with self.assertRaises(RuntimeError):
                network_helpers.retry_random_exp(always_fails, retries=3)

        # current_exp doubles on every retry: 1, 2, 4 -> 1.5, 3.0, 6.0
        self.assertEqual(waits, [1.5, 3.0, 6.0])


if __name__ == '__main__':
    unittest.main()
