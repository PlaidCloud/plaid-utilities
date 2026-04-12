# coding=utf-8
"""Tests for plaidcloud.utilities.udf using mock PlaidConnection objects."""

import os
import tempfile
import unittest
from pathlib import Path
from unittest import mock

from plaidcloud.utilities import udf


def _mock_conn(code='def foo():\n    return 1\n',
               project='proj1',
               udf_name='my_udf',
               udf_ext='py',
               udf_paths=('/',)):
    conn = mock.MagicMock()
    conn.analyze.udf.get_code.return_value = code
    conn.analyze.udf.set_code.return_value = None
    conn.analyze.project.project.return_value = {'name': project}
    conn.analyze.udf.udf.return_value = {
        'name': udf_name, 'extension': udf_ext, 'paths': list(udf_paths),
    }
    return conn


class TestDownloadUdf(unittest.TestCase):

    def test_download_with_local_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, 'nested', 'x.py')
            conn = _mock_conn(code='CONST = 1\n')

            udf.download_udf(conn, 'proj-id', 'udf-id', local_path=target)

            with open(target) as f:
                self.assertEqual(f.read(), 'CONST = 1\n')

    def test_download_uses_local_root_when_no_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = _mock_conn(
                code='X = 2\n',
                project='myproj',
                udf_name='widget',
                udf_paths=['/scripts/'],
            )

            udf.download_udf(conn, 'proj', 'u', local_root=tmp)

            expected = os.path.join(tmp, 'myproj', 'scripts', 'widget.py')
            self.assertTrue(os.path.isfile(expected))
            with open(expected) as f:
                self.assertEqual(f.read(), 'X = 2\n')

    def test_download_uses_find_workspace_root_fallback(self):
        with tempfile.TemporaryDirectory() as tmp:
            conn = _mock_conn(
                code='Y = 3\n',
                project='p',
                udf_name='w',
                udf_paths=['/sub/'],
            )

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp),
            ):
                udf.download_udf(conn, 'p', 'u')

            expected = os.path.join(tmp, 'p', 'sub', 'w.py')
            self.assertTrue(os.path.isfile(expected))


class TestUploadUdf(unittest.TestCase):

    def _setup_workspace(self, tmp, project='proj', udf_name='widget'):
        project_dir = os.path.join(tmp, project)
        os.makedirs(project_dir)
        file_path = os.path.join(project_dir, f'{udf_name}.py')
        with open(file_path, 'w') as f:
            f.write('def foo():\n    pass\n')
        return file_path

    def test_upload_updates_existing_udf(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp)
            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [
                {'name': 'proj', 'id': 'proj-id'},
                {'name': 'other', 'id': 'other-id'},
            ]
            conn.analyze.udf.udfs.return_value = [
                {'name': 'widget', 'id': 'udf-id'},
            ]

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                udf.upload_udf(file_path, conn)

            conn.analyze.udf.set_code.assert_called_once()
            kwargs = conn.analyze.udf.set_code.call_args.kwargs
            self.assertEqual(kwargs['project_id'], 'proj-id')
            self.assertEqual(kwargs['udf_id'], 'udf-id')

    def test_upload_creates_new_udf_when_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp)
            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [
                {'name': 'proj', 'id': 'proj-id'},
            ]
            conn.analyze.udf.udfs.return_value = []
            conn.analyze.udf.create.return_value = {'id': 'new-udf-id'}

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                udf.upload_udf(file_path, conn)

            conn.analyze.udf.create.assert_called_once()

    def test_upload_raises_when_project_missing(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp, project='unknown')
            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [
                {'name': 'proj', 'id': 'proj-id'},
            ]

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                with self.assertRaisesRegex(Exception, 'Project .* does not exist'):
                    udf.upload_udf(file_path, conn)

    def test_upload_raises_when_udf_missing_and_create_false(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp)
            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [
                {'name': 'proj', 'id': 'proj-id'},
            ]
            conn.analyze.udf.udfs.return_value = []

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                with self.assertRaisesRegex(Exception, 'udf .* does not exist'):
                    udf.upload_udf(file_path, conn, create=False)

    def test_upload_raises_when_file_outside_workspace_root(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp)
            other_root = tempfile.mkdtemp()
            try:
                conn = mock.MagicMock()
                with mock.patch.object(udf, 'getpass') as gp:
                    gp.getuser.return_value = 'mike'
                    with self.assertRaisesRegex(Exception, 'is not under'):
                        udf.upload_udf(file_path, conn, local_root=other_root)
            finally:
                os.rmdir(other_root)

    def test_upload_as_plaid_user_noops(self):
        # When running as the 'plaid' system user, upload_udf short-circuits.
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp)
            conn = mock.MagicMock()
            with mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'plaid'
                udf.upload_udf(file_path, conn)
            conn.analyze.udf.set_code.assert_not_called()
            conn.analyze.udf.create.assert_not_called()

    def test_upload_with_explicit_project_and_udf_name_overrides(self):
        with tempfile.TemporaryDirectory() as tmp:
            file_path = self._setup_workspace(tmp, project='real', udf_name='actual')
            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [
                {'name': 'override-proj', 'id': 'override-id'},
            ]
            conn.analyze.udf.udfs.return_value = [
                {'name': 'override-name', 'id': 'override-udf-id'},
            ]

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                udf.upload_udf(
                    file_path, conn,
                    project_name='override-proj',
                    name='override-name',
                    udf_path='custom.py',
                    parent_path='/nested',
                )

            kwargs = conn.analyze.udf.set_code.call_args.kwargs
            self.assertEqual(kwargs['project_id'], 'override-id')
            self.assertEqual(kwargs['udf_id'], 'override-udf-id')

    def test_upload_creates_under_root_when_no_parent_path(self):
        with tempfile.TemporaryDirectory() as tmp:
            # File sits directly under the project dir, so parent_path is empty.
            project_dir = os.path.join(tmp, 'proj')
            os.makedirs(project_dir)
            file_path = os.path.join(project_dir, 'widget.py')
            with open(file_path, 'w') as f:
                f.write('X = 1\n')

            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [{'name': 'proj', 'id': 'pid'}]
            conn.analyze.udf.udfs.return_value = []
            conn.analyze.udf.create.return_value = {'id': 'new-id'}

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                udf.upload_udf(file_path, conn)

            create_call = conn.analyze.udf.create.call_args.kwargs
            self.assertEqual(create_call['path'], '/')

    def test_upload_non_py_extension_keeps_full_name(self):
        with tempfile.TemporaryDirectory() as tmp:
            project_dir = os.path.join(tmp, 'proj')
            os.makedirs(project_dir)
            file_path = os.path.join(project_dir, 'query.sql')
            with open(file_path, 'w') as f:
                f.write('SELECT 1\n')

            conn = mock.MagicMock()
            conn.analyze.project.projects.return_value = [{'name': 'proj', 'id': 'pid'}]
            conn.analyze.udf.udfs.return_value = [{'name': 'query.sql', 'id': 'uid'}]

            with mock.patch.object(
                udf, 'find_workspace_root', return_value=Path(tmp).resolve(),
            ), mock.patch.object(udf, 'getpass') as gp:
                gp.getuser.return_value = 'mike'
                udf.upload_udf(file_path, conn)

            conn.analyze.udf.set_code.assert_called_once()


if __name__ == '__main__':
    unittest.main()
