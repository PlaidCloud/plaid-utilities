#!/usr/bin/env python
# coding=utf-8

import getpass
from pathlib import Path
from plaidcloud.rpc.config import find_workspace_root

__author__ = 'Adams Tower'
__copyright__ = 'Copyright 2010-2021, Tartan Solutions, Inc'
__credits__ = ['Adams Tower']
__license__ = 'Apache 2.0'
__maintainer__ = 'Adams Tower'
__email__ = 'adams.tower@tartansolutions.com'


def download_udf(conn, project_id, udf_id, local_root=None, local_path=None):
    """
    Downloads a udf from plaid and puts it into a local file, the location of
    which reflects the plaid udf hierarchy

    Args:
        conn: a PlaidConnection object
        project_id: the project to download from
        udf_id: the udf to download
        local_root (str, optional)
        local_path (str, optional)

    Returns:
        None
    """
    code = conn.analyze.udf.get_code(project_id=project_id, udf_id=udf_id)
    if local_path:
        path = Path(local_path).resolve()
    else:
        if local_root:
            root = Path(local_root).resolve()
        else:
            root = find_workspace_root()

        project = conn.analyze.project.project(project_id=project_id)
        udf = conn.analyze.udf.udf(project_id=project_id, udf_id=udf_id)

        path = root.joinpath(
            project['name'],
            udf['paths'][0].lstrip('/'),
            udf['name']
        ).with_suffix(f".{udf['extension']}")

    path.parent.mkdir(parents=True, exist_ok=True)

    with open(path, 'w') as f:
        f.write(code)


def upload_udf(local_path, conn, create=True, local_root=None, project_name=None, udf_path=None, parent_path=None, name=None,
               view_manager=False, view_explorer=False, memo=None):
    """
    Uploads a local file as a udf. Determines which project to upload to based
    on the name of the directory containing the file. Determines which udf to upload
    to based on the name of the file.

    To use it to upload the current file, but only if that file is on a local Windows or mac dev:

        import platform
        from plaidcloud.utilities.connect import PlaidConnection
        from plaidcloud.utilities.udf import upload_udf

        conn = PlaidConnection()

        if platform.system() == "Windows" or platform.system() == "Darwin":
            upload_udf(__file__, conn)

    Args:
        local_path: the path to the file to be uploaded
        conn (plaidcloud.utilities.connect.PlaidConnection): a connection object to use to upload the file
        create (bool, optional)
        local_root (str, optional)
        project_name (str, optional)
        udf_path (str, optional)
        parent_path (str, optional)
        name (str, optional)
        view_manager (bool, optional)
        view_explorer (bool, optional)
        memo (str, optional)

    Returns:
        None
    """
    if getpass.getuser() != 'plaid':
        path = Path(local_path).resolve()
        if local_root:
            root = Path(local_root).resolve()
        else:
            root = find_workspace_root(path)

        try:
            parts = path.relative_to(root).parts
        except ValueError:
            raise Exception(f'{str(path)} is not under {str(root)}')

        intuited_project_name = parts[0]
        intuited_parent_path = '/'.join(parts[1:-1])
        intuited_udf_path = parts[-1]
        if not project_name:
            project_name = intuited_project_name
        if not udf_path:
            udf_path = intuited_udf_path
        if not parent_path:
            parent_path = intuited_parent_path
        if not name:
            if udf_path.endswith('.py'):
                name = udf_path[:-3]
            else:
                name = udf_path

        projects = conn.analyze.project.projects()
        for project in projects:
            if project['name'].lower() == project_name.lower():
                project_id = project['id']
                break
        else:
            raise Exception('Project {} does not exist!'.format(project_name))
        udfs = conn.analyze.udf.udfs(project_id=project_id)
        for udf in udfs:
            if udf['name'].lower() == name.lower():
                udf_id = udf['id']
                break
        else:
            if create:
                if not parent_path:
                    parent_path = '/'
                udf = conn.analyze.udf.create(
                    project_id=project_id, path=parent_path,
                    name=name, file_path=udf_path, view_manager=view_manager,
                    view_explorer=view_explorer, memo=memo,
                )
                udf_id = udf['id']
            else:
                raise Exception('udf {} does not exist!'.format(udf_path))

        with open(local_path, 'r') as f:
            code = f.read()

        conn.analyze.udf.set_code(project_id=project_id, udf_id=udf_id, code=code)
