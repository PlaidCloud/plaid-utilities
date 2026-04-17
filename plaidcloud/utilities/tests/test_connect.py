# coding=utf-8
"""Tests for testable pieces of plaidcloud.utilities.connect.

The RPC/xlwings orchestration methods of PlaidConnection are pragma'd since
they require a live PlaidCloud RPC connection. The utility-loader methods
only touch self.rpc and a few module-level imports, so they're exercised
here against a PlaidConnection instance built without calling __init__.
"""

import sys
import unittest
from unittest import mock

from plaidcloud.utilities import connect
from plaidcloud.utilities.udf_utility_loader import DEFAULT_NAMESPACE


def _bare_connection():
    """Build a PlaidConnection without running the RPC-dependent __init__."""
    return connect.PlaidConnection.__new__(connect.PlaidConnection)


class TestLoadPlaidcloudUtilityScripts(unittest.TestCase):

    def setUp(self):
        self._loaded = []

    def tearDown(self):
        for name in self._loaded:
            sys.modules.pop(f'{DEFAULT_NAMESPACE}.{name}', None)

    def test_fetches_and_loads_only_utility_udfs(self):
        conn = _bare_connection()
        conn._project_id = 'proj-id'
        conn.rpc = mock.MagicMock()
        conn.rpc.analyze.udf.udfs.return_value = [
            {'id': '1', 'name': 'helper_a', 'kind': 'utility'},
            {'id': '2', 'name': 'non_utility', 'kind': 'transform'},
            {'id': '3', 'name': 'helper_b', 'kind': 'utility'},
        ]
        conn.rpc.analyze.udf.get_code.side_effect = [
            'A = 1\n',  # helper_a
            'B = 2\n',  # helper_b
        ]
        self._loaded.extend(['helper_a', 'helper_b'])

        conn.load_plaidcloud_utility_scripts()

        # helper_a + helper_b loaded; non_utility filtered out
        import plaidcloud.utilities.udf_helpers as helpers
        self.assertEqual(helpers.helper_a.A, 1)
        self.assertEqual(helpers.helper_b.B, 2)

    def test_project_id_passed_through(self):
        conn = _bare_connection()
        conn._project_id = 'proj-xyz'
        conn.rpc = mock.MagicMock()
        conn.rpc.analyze.udf.udfs.return_value = []

        conn.load_plaidcloud_utility_scripts(reload=False)

        conn.rpc.analyze.udf.udfs.assert_called_once_with(project_id='proj-xyz')


class TestLoadRemoteUtilityScripts(unittest.TestCase):

    def setUp(self):
        self._loaded = []

    def tearDown(self):
        for name in self._loaded:
            sys.modules.pop(f'{DEFAULT_NAMESPACE}.{name}', None)

    def test_downloads_and_loads_each_script(self):
        conn = _bare_connection()

        response = mock.MagicMock()
        response.text = 'X = 42\n'
        response.raise_for_status.return_value = None

        self._loaded.append('remote_a')
        with mock.patch.object(connect.requests, 'get', return_value=response):
            conn.load_remote_utility_scripts({'remote_a': 'https://x/y.py'})

        import plaidcloud.utilities.udf_helpers as helpers
        self.assertEqual(helpers.remote_a.X, 42)

    def test_http_error_propagates(self):
        conn = _bare_connection()

        response = mock.MagicMock()
        response.raise_for_status.side_effect = RuntimeError('404')

        with mock.patch.object(connect.requests, 'get', return_value=response):
            with self.assertRaises(RuntimeError):
                conn.load_remote_utility_scripts({'x': 'https://x/y.py'})

    def test_invalid_script_raises_with_module_name(self):
        conn = _bare_connection()

        response = mock.MagicMock()
        response.text = 'print("hi")\n'  # not allowed at top level
        response.raise_for_status.return_value = None

        with mock.patch.object(connect.requests, 'get', return_value=response):
            with self.assertRaisesRegex(
                ValueError, 'Invalid utility script: bad_one',
            ):
                conn.load_remote_utility_scripts({'bad_one': 'https://x/y.py'})


if __name__ == '__main__':
    unittest.main()
