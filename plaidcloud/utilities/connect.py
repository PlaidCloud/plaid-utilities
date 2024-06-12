#!/usr/bin/env python
# coding=utf-8

"""Basic class that allows for a handler that wraps the plaid RPC API.
   Gracefully handles oauth token generation.

   Primarily used in UDFs
   """

import os
import shutil

from plaidcloud.rpc.logger import Logger
from plaidcloud.rpc.rpc_connect import Connect, PlaidXLConnect
from plaidcloud.utilities.query import Connection, Table
import plaidcloud.utilities.data_helpers as dh
import plaidcloud.utilities.frame_manager as fm

try:
    import xlwings as xw  # pylint: disable=import-error
except:
    xw = None

__author__ = 'Pat Buxton'
__maintainer__ = 'Pat Buxton <patrick.buxton@tartansolutions.com>'
__copyright__ = '© Copyright 2020, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'

def create_connection(*args, **kwargs):
    """
    This function enables UDFs to autocomplete rpc methods from IDEs.
    """
    conn = PlaidConnection(*args, **kwargs)
    if False:
        from plaid import rpc_v1  # pylint: disable=import-error
        conn = rpc_v1

    conn._logger.debug('Workspace ID (UUID):   {0} ({1})'.format(conn.workspace_id, conn.workspace_uuid))
    # TODO: display the workspace name once it is feasible to do so.
    #conn._logger.debug('Workspace Name: {0}'.format(conn.analyze.something_unknown__what_goes_here))

    conn._logger.debug('Project ID:   {0}'.format(conn.project_id))
    conn._logger.debug('Project Name: {0}'.format(conn.analyze.project.project(project_id=conn.project_id)['name']))
    return conn


class PlaidConnection(Connect, Connection):
    """
    Establish connection.
    """
    def __init__(self, *args, **kwargs):
        Connect.__init__(self)
        Connection.__init__(self, rpc=self)
        self._logger = Logger(rpc=self)
        self._logger.debug('Connected to host "{0}"'.format(self.hostname))
        if xw and kwargs.get('xl_path') and self.is_local and self.debug:
            self._wb = self.get_workbook(kwargs.get('xl_path'))
        else:
            self._wb = None
        if isinstance(self._project_id, str) and self._project_id != '':
            self._logger.debug('Project ID: {0}'.format(self.project_id))
        if isinstance(self._workflow_id, str) and self._workflow_id != '':
            self._logger.debug('Workflow ID: {0}'.format(self.workflow_id))

    def get_table(self, table_name):
        return Table(self, table_name)

    def get_workbook(self, xl_path):
        xl_path = self.path(xl_path)
        if not os.path.exists(xl_path):
            template_path = self.path('TEMPLATE') + '/template.xlsm'
            if not os.path.exists(template_path):
                raise Exception('Excel template does not exist at {}'.format(template_path))
            shutil.copyfile(template_path, xl_path)
        self._logger.debug('Workbook: {0}'.format(xl_path))
        return xw.Book(xl_path)

    def save_xl(self, wb=None):
        if not xw:
            return
        if wb:
            wb.save()
        elif self._wb:
            self._wb.save()

    def to_xl(self, df, sheet, book=False, wb=None, autofit=True, show_index=False, silent=False, check_debug=False, check_local='', save=False):
        if xw and self.is_local is True and (not check_debug or self.debug is True) and (not check_local or self.local[check_local] is True):
            dh.to_xl(
                df,
                sheet=sheet,
                book=None,
                wb=wb or self._wb,
                autofit=autofit,
                show_index=show_index,
                silent=silent
            )
        if save:
            self.save_xl(wb=wb)

    def to_xl_old(self, table_sheet_tuples=None, df_sheet_tuples=None, save=False):
        if xw and self.is_local is True and self.debug is True and self.local['xl_out'] is True:
            if table_sheet_tuples:
                for tbl, sheet in table_sheet_tuples:
                    dh.to_xl(self.get_dataframe(tbl, clean=True), sheet=sheet, wb=self._wb)
            if df_sheet_tuples:
                for df, sheet in df_sheet_tuples:
                    dh.to_xl(df, sheet=sheet, wb=self._wb)
            if save and self._wb:
                self._wb.save()


    def save(self, df, name, append=False):
        if self.is_local is False or self.write_from_local is True:
            fm.save(df, name, self, append=append)

    @property
    def logger(self):
        return self._logger


class PlaidXLConnection(PlaidXLConnect, Connection):

    def __init__(self, *, rpc_uri: str, auth_token: str, workspace_id: str = '', project_id: str = ''):
        PlaidXLConnect.__init__(self, rpc_uri=rpc_uri, auth_token=auth_token, workspace_id=workspace_id, project_id=project_id)
        Connection.__init__(self, rpc=self)
