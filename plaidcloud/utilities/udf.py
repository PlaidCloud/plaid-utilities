import os

def download_udf(conn, project_id, udf_id, local_root):
    project = conn.analyze.project.project(project_id=project_id)
    udf = conn.analyze.udf.udf(project_id=project_id, udf_id=udf_id)
    code = conn.analyze.udf.get_code(project_id=project_id, udf_id=udf_id)

    local_path = os.path.join(
        local_root,
        project['name'],
        udf['paths'][0].lstrip('/'),
        udf['file_path'],
    )
    os.makedirs(os.path.dirname(local_path), exist_ok=True)
    with open(local_path, 'w') as f:
        f.write(code)

def upload_udf(local_path, conn, create=False, project_name=None, udf_path=None, parent_path=None, name=None, branch='master', view_manager=False, view_explorer=False, memo=None):
    '''
    Uploads a local file as a udf. Determines which project to upload to based
    on the name of the directory containing the file. Determines which udf to upload
    to based on th name of the file.

    To use it to upload the current file, but only if that file is on a local Windows or mac dev:

        import os
        import platform
        from plaidcloud.utilities.connect import PlaidConnection
        from plaidcloud.utilities.udf import upload_udf

        conn = PlaidConnection()

        if platform.system() == "Windows" or platform.system() == "Darwin":
            upload_udf(os.path.abspath(__file__), conn)

    Args:
        local_path: the path to the file to be uploaded
        conn (plaidcloud.utilities.connect.PlaidConnection): a connection object to use to upload the file

    Returns:
        None
    '''
    def parts_from_downloaded_udfs(path):
        head, tail = os.path.split(path)
        if tail == 'downloaded_udfs':
            return []
        else:
            return parts_from_downloaded_udfs(head) + [tail]
    parts = parts_from_downloaded_udfs(local_path)
    intuited_project_name = parts[0]
    intuited_parent_path = '/'.join(parts[1:-1])
    intuited_udf_path = parts[-1]
    if not project_name:
        project_name = intuited_project_name
    if not udf_path:
        udf_path=intuited_udf_path
    if not parent_path:
        parent_path = intuited_parent_path
    projects = conn.analyze.project.projects()
    for project in projects:
        if project['name'].lower() == project_name.lower():
            project_id = project['id']
            break
    else:
        raise Exception('Project {} does not exist!'.format(project_name))
    udfs = conn.analyze.udf.udfs(project_id=project_id)
    for udf in udfs:
        if udf['file_path'].lower() == udf_path.lower():
            udf_id = udf['id']
            break
    else:
        if create:
            if not parent_path:
                parent_path = '/'
            if not name:
                if udf_path.endswith('.py'):
                    name = udf_path[:-3]
                else:
                    name = udf_path
            udf = conn.analyze.udf.create(
                project_id=project_id, branch=branch, path=parent_path,
                name=name, file_path=udf_path, view_manager=view_manager,
                view_explorer=view_explorer, memo=memo,
            )
            udf_id = udf['id']
        else:
            raise Exception('udf {} does not exist!'.format(udf_path))

    with open(local_path, 'r') as f:
        code = f.read()
    conn.analyze.udf.set_code(project_id=project_id, udf_id=udf_id, code=code)
