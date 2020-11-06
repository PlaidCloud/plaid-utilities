import os

def upload_udf(local_path, conn):
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
    dir, udf_path = os.path.split(local_path)
    _, project_name = os.path.split(dir)
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
        raise Exception('udf {} does not exist!'.format(udf_path))

    with open(local_path, 'r') as f:
        code = f.read()
    conn.analyze.udf.set_code(project_id=project_id, udf_id=udf_id, code=code)
