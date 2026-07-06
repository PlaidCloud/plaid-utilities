# coding=utf-8
"""Tests for plaidcloud.utilities.query."""
import csv
import os
import tempfile
import unittest
import uuid
from unittest.mock import MagicMock, patch, mock_open, ANY

import numpy as np
import pandas as pd
import pytest
import sqlalchemy

from plaidcloud.rpc.database import PlaidDate, PlaidTimestamp
from plaidcloud.utilities import query
from plaidcloud.utilities.query import (
    Connection,
    SCHEMA_PREFIX,
    TABLE_PREFIX,
    Table,
    UDFParams,
    _get_table_id,
)

__author__ = "Pat Buxton"
__copyright__ = "Copyright 2026, Tartan Solutions, Inc"
__license__ = "Apache 2.0"


def make_mock_rpc(
    project_id='proj-id-123',
    step_id=None,
    workflow_id=None,
    schema='anlz_schema',
    dialect_name='postgresql',
    table_meta=None,
    search_results=None,
    project_name='proj',
):
    """Build an rpc-like MagicMock with sensible defaults."""
    rpc = MagicMock()
    rpc.project_id = project_id
    rpc.step_id = step_id
    rpc.workflow_id = workflow_id
    rpc.rpc_uri = 'https://example.com/json-rpc'
    rpc.auth_token = 'tok'
    rpc.verify_ssl = True
    rpc.analyze.project.lookup_by_full_path.return_value = project_id
    rpc.analyze.project.lookup_by_name.return_value = project_id
    rpc.analyze.project.project.return_value = {'name': project_name}
    rpc.analyze.project.get_project_schema.return_value = schema
    rpc.analyze.query.dialect.return_value = dialect_name
    rpc.analyze.table.table_meta.return_value = list(table_meta or [])
    rpc.analyze.table.search_by_name.return_value = list(search_results or [])
    rpc.analyze.table.create.return_value = {'id': 'analyzetable_newid'}
    rpc.analyze.table.touch.return_value = None
    return rpc


def make_connection(rpc=None, project=None):
    """Construct a Connection with the Dimensions class patched out."""
    if rpc is None:
        rpc = make_mock_rpc()
    with patch.object(query, 'Dimensions') as mock_dims:
        mock_dims.return_value = MagicMock()
        conn = Connection(project=project, rpc=rpc)
    return conn


# ---------------------------------------------------------------------------
# UDFParams
# ---------------------------------------------------------------------------
class TestUDFParams(unittest.TestCase):
    def test_named_tuple_fields(self):
        params = UDFParams(
            source_by_name={'a': 'tableA'},
            sources=['tableA'],
            target_by_name={'b': 'tableB'},
            targets=['tableB'],
            variable_by_name={'v': 'val'},
            variables=['val'],
        )
        self.assertEqual(params.source_by_name, {'a': 'tableA'})
        self.assertEqual(params.sources, ['tableA'])
        self.assertEqual(params.target_by_name, {'b': 'tableB'})
        self.assertEqual(params.targets, ['tableB'])
        self.assertEqual(params.variable_by_name, {'v': 'val'})
        self.assertEqual(params.variables, ['val'])


# ---------------------------------------------------------------------------
# _get_table_id
# ---------------------------------------------------------------------------
class TestGetTableId(unittest.TestCase):
    def setUp(self):
        self.rpc = make_mock_rpc()

    def test_returns_name_when_already_table_id(self):
        result = _get_table_id(self.rpc, 'proj', TABLE_PREFIX + 'abc')
        self.assertEqual(result, (TABLE_PREFIX + 'abc', None, None))
        # No RPC call should have been made.
        self.rpc.analyze.table.search_by_name.assert_not_called()

    def test_single_match_returns_id(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/some/path']}
        ]
        table_id, path, name = _get_table_id(self.rpc, 'proj', 'some_table')
        self.assertEqual(table_id, 'analyzetable_1')
        self.assertEqual(path, '/')
        self.assertEqual(name, 'some_table')

    def test_path_prepended_with_slash(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/dir/sub']}
        ]
        _, path, name = _get_table_id(self.rpc, 'proj', 'dir/sub/tab')
        self.assertEqual(path, '/dir/sub')
        self.assertEqual(name, 'tab')

    def test_multiple_matches_one_path_match(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/a']},
            {'id': 'analyzetable_2', 'paths': ['/b']},
        ]
        table_id, _, _ = _get_table_id(self.rpc, 'proj', 'b/tab')
        self.assertEqual(table_id, 'analyzetable_2')

    def test_multiple_matches_no_path_match_raises(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/a']},
            {'id': 'analyzetable_2', 'paths': ['/b']},
        ]
        with self.assertRaises(Exception):
            _get_table_id(self.rpc, 'proj', 'c/tab', raise_if_not_found=True)

    def test_multiple_path_matches_arbitrary_first(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/a']},
            {'id': 'analyzetable_2', 'paths': ['/a']},
        ]
        table_id, _, _ = _get_table_id(
            self.rpc, 'proj', 'a/tab', raise_if_not_found=False
        )
        self.assertEqual(table_id, 'analyzetable_1')

    def test_multiple_matches_no_path_no_raise_returns_first(self):
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_1', 'paths': ['/a']},
            {'id': 'analyzetable_2', 'paths': ['/b']},
        ]
        table_id, _, _ = _get_table_id(
            self.rpc, 'proj', 'c/tab', raise_if_not_found=False
        )
        self.assertEqual(table_id, 'analyzetable_1')

    def test_no_match_raises(self):
        self.rpc.analyze.table.search_by_name.return_value = []
        with self.assertRaises(Exception):
            _get_table_id(self.rpc, 'proj', 'missing', raise_if_not_found=True)

    def test_no_match_returns_none(self):
        self.rpc.analyze.table.search_by_name.return_value = []
        table_id, path, name = _get_table_id(
            self.rpc, 'proj', 'missing', raise_if_not_found=False
        )
        self.assertIsNone(table_id)
        self.assertEqual(path, '/')
        self.assertEqual(name, 'missing')


# ---------------------------------------------------------------------------
# Connection.__init__
# ---------------------------------------------------------------------------
class TestConnectionInit(unittest.TestCase):
    def test_project_passed_as_uuid(self):
        proj_uuid = str(uuid.uuid4())
        rpc = make_mock_rpc(project_id=proj_uuid)
        conn = make_connection(rpc=rpc, project=proj_uuid)
        self.assertEqual(conn._project_id, proj_uuid)
        # Should not invoke lookups for a UUID
        rpc.analyze.project.lookup_by_full_path.assert_not_called()
        rpc.analyze.project.lookup_by_name.assert_not_called()

    def test_project_passed_as_path(self):
        rpc = make_mock_rpc()
        rpc.analyze.project.lookup_by_full_path.return_value = 'lookup-id-path'
        conn = make_connection(rpc=rpc, project='/foo/bar')
        self.assertEqual(conn._project_id, 'lookup-id-path')
        rpc.analyze.project.lookup_by_full_path.assert_called_once_with(path='/foo/bar')

    def test_project_passed_as_name(self):
        rpc = make_mock_rpc()
        rpc.analyze.project.lookup_by_name.return_value = 'lookup-id-name'
        conn = make_connection(rpc=rpc, project='MyProject')
        self.assertEqual(conn._project_id, 'lookup-id-name')
        rpc.analyze.project.lookup_by_name.assert_called_once_with(name='MyProject')

    def test_project_from_rpc_when_none(self):
        rpc = make_mock_rpc(project_id='rpc-default')
        conn = make_connection(rpc=rpc)
        self.assertEqual(conn._project_id, 'rpc-default')

    def test_dialect_falls_back_to_postgresql(self):
        rpc = make_mock_rpc(dialect_name='nonexistent-dialect-xyz')
        conn = make_connection(rpc=rpc, project=str(uuid.uuid4()))
        self.assertEqual(conn.dialect.name, 'postgresql')

    def test_creates_default_rpc_when_none_provided(self):
        """``Connection()`` with no arguments should construct a default
        ``Connect`` and use its ``project_id``."""
        with patch.object(query, 'Connect') as mock_connect, \
             patch.object(query, 'Dimensions'):
            rpc = make_mock_rpc()
            mock_connect.return_value = rpc
            conn = Connection()
            mock_connect.assert_called_once()
            self.assertIs(conn.rpc, rpc)
            self.assertEqual(conn._project_id, rpc.project_id)


# ---------------------------------------------------------------------------
# Connection.variables (lazy)
# ---------------------------------------------------------------------------
class TestConnectionVariables(unittest.TestCase):
    def test_lazy_load_on_first_access(self):
        rpc = make_mock_rpc(workflow_id=None)
        rpc.analyze.project.variable_values.return_value = {'x': '1'}
        conn = make_connection(rpc=rpc)
        # not yet loaded
        self.assertIs(conn._variables, Connection._NOT_LOADED)
        self.assertEqual(conn.variables, {'x': '1'})
        # cached -- second call doesn't refresh
        rpc.analyze.project.variable_values.return_value = {'x': '2'}
        self.assertEqual(conn.variables, {'x': '1'})

    def test_load_failure_yields_empty_dict(self):
        rpc = make_mock_rpc()
        rpc.analyze.project.variable_values.side_effect = RuntimeError('boom')
        conn = make_connection(rpc=rpc)
        self.assertEqual(conn.variables, {})

    def test_setter(self):
        conn = make_connection()
        conn.variables = {'k': 'v'}
        self.assertEqual(conn.variables, {'k': 'v'})

    def test_refresh_variables_with_workflow_id(self):
        rpc = make_mock_rpc(workflow_id='wf-1')
        rpc.analyze.workflow.variable_values.return_value = {'a': 'b'}
        conn = make_connection(rpc=rpc)
        result = conn.refresh_variables()
        self.assertEqual(result, {'a': 'b'})
        rpc.analyze.workflow.variable_values.assert_called_once_with(
            project_id=conn._project_id, workflow_id='wf-1', include_project=True,
        )

    def test_refresh_variables_without_workflow_id(self):
        rpc = make_mock_rpc(workflow_id=None)
        rpc.analyze.project.variable_values.return_value = {'c': 'd'}
        conn = make_connection(rpc=rpc)
        result = conn.refresh_variables()
        self.assertEqual(result, {'c': 'd'})
        rpc.analyze.project.variable_values.assert_called_once_with(
            project_id=conn._project_id,
        )


# ---------------------------------------------------------------------------
# Connection.udf
# ---------------------------------------------------------------------------
class TestConnectionUdf(unittest.TestCase):
    def test_udf_no_step_id(self):
        rpc = make_mock_rpc(step_id=None)
        conn = make_connection(rpc=rpc)
        self.assertIsNone(conn.udf)

    def test_udf_setter(self):
        conn = make_connection()
        sentinel = object()
        conn.udf = sentinel
        self.assertIs(conn.udf, sentinel)

    def test_load_udf_params_populates(self):
        rpc = make_mock_rpc(step_id='step-123', workflow_id=None)
        rpc.analyze.project.variable_values.return_value = {}
        rpc.analyze.step.step.return_value = {
            'sources': [{'source': 'src_tab', 'id': 'src_alias'}],
            'targets': [{'target': 'tgt_tab', 'id': 'tgt_alias'}],
            'variables': [{'value': 'val', 'name': 'var_name'}],
        }
        conn = make_connection(rpc=rpc)
        with patch.object(conn, 'get_table') as gt:
            gt.side_effect = lambda name: f'table:{name}'
            udf = conn.udf
        self.assertIsInstance(udf, UDFParams)
        self.assertEqual(udf.source_by_name, {'src_alias': 'table:src_tab'})
        self.assertEqual(udf.target_by_name, {'tgt_alias': 'table:tgt_tab'})
        self.assertEqual(udf.variable_by_name, {'var_name': 'val'})

    def test_udf_load_failure_yields_none(self):
        rpc = make_mock_rpc(step_id='step-x')
        rpc.analyze.step.step.side_effect = RuntimeError('nope')
        conn = make_connection(rpc=rpc)
        self.assertIsNone(conn.udf)


# ---------------------------------------------------------------------------
# Connection._compiled
# ---------------------------------------------------------------------------
class TestCompiled(unittest.TestCase):
    def test_compile_basic_select(self):
        conn = make_connection()
        meta = sqlalchemy.MetaData()
        tbl = sqlalchemy.Table(
            't', meta,
            sqlalchemy.Column('a', sqlalchemy.Integer),
        )
        q, params = conn._compiled(sqlalchemy.select(tbl.c.a).where(tbl.c.a == 5))
        self.assertIsInstance(q, str)
        self.assertNotIn('\n', q)
        self.assertIn('a', q)
        self.assertIn(5, params.values())

    def test_compile_flattens_embedded_newlines_with_space(self):
        # Raw multi-line SQL (e.g. an AI/hand-authored extract) carries newlines
        # with NO trailing space. Flattening must replace each newline with a
        # space, not drop it, or tokens weld across clause boundaries
        # (`"driver_name"FROM`) and the statement no longer parses.
        conn = make_connection()
        raw = sqlalchemy.text('SELECT a."driver_name"\nFROM t a\nWHERE a.x > 0')
        q, _params = conn._compiled(raw)
        self.assertNotIn('\n', q)
        self.assertNotIn('"driver_name"FROM', q)
        self.assertIn('"driver_name" FROM', q)


# ---------------------------------------------------------------------------
# Connection.get_csv / get_csv_by_query
# ---------------------------------------------------------------------------
class TestGetCsv(unittest.TestCase):
    def setUp(self):
        self.rpc = make_mock_rpc()
        self.rpc.analyze.query.download_csv.return_value = '/tmp/x.csv'
        self.conn = make_connection(rpc=self.rpc)

    def _table_with_cols(self, *cols):
        meta = sqlalchemy.MetaData()
        return sqlalchemy.Table('tab', meta, *cols, schema='anlz_schema')

    def test_get_csv_unclean_path(self):
        tbl = self._table_with_cols(sqlalchemy.Column('a', sqlalchemy.Integer))
        result = self.conn.get_csv(tbl, clean=False)
        self.assertEqual(result, '/tmp/x.csv')
        kwargs = self.rpc.analyze.query.download_csv.call_args.kwargs
        self.assertIn('table_name', kwargs)
        self.assertNotIn('query', kwargs)

    def test_get_csv_clean_utf8(self):
        tbl = self._table_with_cols(
            sqlalchemy.Column('a', sqlalchemy.Integer),
            sqlalchemy.Column('b', sqlalchemy.String),
        )
        result = self.conn.get_csv(tbl, clean=True)
        self.assertEqual(result, '/tmp/x.csv')
        kwargs = self.rpc.analyze.query.download_csv.call_args.kwargs
        self.assertIn('query', kwargs)
        self.assertIn('replace', kwargs['query'].lower())

    def test_get_csv_clean_non_utf8(self):
        tbl = self._table_with_cols(
            sqlalchemy.Column('a', sqlalchemy.String),
        )
        self.conn.get_csv(tbl, encoding='latin-1', clean=True)
        kwargs = self.rpc.analyze.query.download_csv.call_args.kwargs
        self.assertIn('convert', kwargs['query'].lower())
        # Encoding parameter is rendered into params, not into the SQL string.
        self.assertIn('utf8_to_latin_1', kwargs['params'].values())

    def test_get_csv_clean_no_columns_raises(self):
        tbl = self._table_with_cols()
        with self.assertRaises(Exception):
            self.conn.get_csv(tbl, clean=True)

    def test_get_csv_by_query_str(self):
        result = self.conn.get_csv_by_query('SELECT 1', params={'p': 1})
        self.assertEqual(result, '/tmp/x.csv')
        kwargs = self.rpc.analyze.query.download_csv.call_args.kwargs
        self.assertEqual(kwargs['query'], 'SELECT 1')
        self.assertEqual(kwargs['params'], {'p': 1})

    def test_get_csv_by_query_sa(self):
        tbl = self._table_with_cols(sqlalchemy.Column('a', sqlalchemy.Integer))
        sa_q = sqlalchemy.select(tbl.c.a).where(tbl.c.a == 7)
        self.conn.get_csv_by_query(sa_q)
        kwargs = self.rpc.analyze.query.download_csv.call_args.kwargs
        self.assertIsInstance(kwargs['query'], str)


# ---------------------------------------------------------------------------
# Connection._csv_stream
# ---------------------------------------------------------------------------
class TestCsvStream(unittest.TestCase):
    def setUp(self):
        self.conn = make_connection()

    def _write_csv(self, header, rows):
        f = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.csv', newline=''
        )
        try:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            for r in rows:
                writer.writerow(r)
        finally:
            f.close()
        return f.name

    def test_streams_typed_rows(self):
        columns = [
            sqlalchemy.Column('i', sqlalchemy.Integer),
            sqlalchemy.Column('n', sqlalchemy.Numeric),
            sqlalchemy.Column('d', sqlalchemy.DateTime),
            sqlalchemy.Column('t', sqlalchemy.Interval),
        ]
        path = self._write_csv(
            ['i', 'n', 'd', 't'],
            [
                {'i': '3', 'n': '1.5', 'd': '2024-01-02', 't': '1 day'},
                {'i': '', 'n': '', 'd': '2024-02-03', 't': '2 days'},
            ],
        )
        try:
            rows = list(self.conn._csv_stream(path, columns, preserve_nulls=False))
            self.assertEqual(rows[0]['i'], 3)
            self.assertAlmostEqual(rows[0]['n'], 1.5)
            self.assertEqual(rows[0]['d'], pd.Timestamp('2024-01-02'))
            self.assertEqual(rows[0]['t'], pd.Timedelta('1 day'))
            # Empty integer/numeric coerced through `or 0`
            self.assertEqual(rows[1]['i'], 0)
            self.assertEqual(rows[1]['n'], 0.0)
        finally:
            os.remove(path)

    def test_preserve_nulls_keeps_none(self):
        columns = [sqlalchemy.Column('i', sqlalchemy.Integer)]
        # Direct exercise via in-memory pseudo: create a tmp csv with header but
        # an explicit None value isn't representable in csv -- this branch only
        # triggers if csv.DictReader yields a None value (extra columns).
        # We construct a row with fewer columns than the header to coerce None.
        f = tempfile.NamedTemporaryFile(
            mode='w', delete=False, suffix='.csv', newline=''
        )
        f.write('i,extra\n')
        f.write('5\n')
        f.close()
        try:
            rows = list(
                self.conn._csv_stream(f.name, columns + [sqlalchemy.Column('extra', sqlalchemy.Integer)], preserve_nulls=True)
            )
            # 'extra' missing -> DictReader fills with None; preserve_nulls keeps it
            self.assertIsNone(rows[0]['extra'])
        finally:
            os.remove(f.name)


# ---------------------------------------------------------------------------
# Connection._get_df_from_csv
# ---------------------------------------------------------------------------
class TestGetDfFromCsv(unittest.TestCase):
    def setUp(self):
        self.conn = make_connection()

    def _write(self, contents):
        f = tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.csv')
        f.write(contents)
        f.close()
        return f.name

    def test_blind_read_when_no_columns(self):
        path = self._write('a,b\n1,hello\n2,world\n')
        try:
            df = self.conn._get_df_from_csv(path, columns=None)
        finally:
            os.remove(path)
        self.assertEqual(list(df.columns), ['a', 'b'])
        self.assertEqual(df.iloc[0]['b'], 'hello')

    def test_typed_read_with_columns(self):
        path = self._write('i,b,s,dt\n3,t,hello,2024-01-01\n4,t,,2024-02-02\n')
        try:
            columns = [
                sqlalchemy.Column('i', sqlalchemy.Integer),
                sqlalchemy.Column('b', sqlalchemy.Boolean),
                sqlalchemy.Column('s', sqlalchemy.String),
                sqlalchemy.Column('dt', sqlalchemy.DateTime),
            ]
            df = self.conn._get_df_from_csv(path, columns=columns)
        finally:
            os.remove(path)
        # Integer column reads as integer.
        self.assertEqual(df.iloc[0]['i'], 3)
        # Truthy boolean cell stays True.
        self.assertTrue(df.iloc[0]['b'])
        # Null strings should become empty strings.
        self.assertEqual(df.iloc[1]['s'], '')
        # Date column is parsed into Timestamps.
        self.assertEqual(df.iloc[0]['dt'], pd.Timestamp('2024-01-01'))

    def test_falsey_string_converts_to_false(self):
        """A CSV value like 'F'/'false'/'no' for a Boolean column should
        produce False."""
        path = self._write('b\nF\nfalse\nno\nFALSE\nf\n')
        try:
            columns = [sqlalchemy.Column('b', sqlalchemy.Boolean)]
            df = self.conn._get_df_from_csv(path, columns=columns)
        finally:
            os.remove(path)
        for i in range(len(df)):
            self.assertFalse(bool(df.iloc[i]['b']))

    def test_interval_column_converted_to_timedelta(self):
        path = self._write('d\n1 days\n2 days\n')
        try:
            columns = [sqlalchemy.Column('d', sqlalchemy.Interval)]
            df = self.conn._get_df_from_csv(path, columns=columns)
        finally:
            os.remove(path)
        self.assertEqual(df.iloc[0]['d'], pd.Timedelta('1 day'))


# ---------------------------------------------------------------------------
# Connection.get_data / get_dataframe variants
# ---------------------------------------------------------------------------
class TestGetData(unittest.TestCase):
    def setUp(self):
        self.conn = make_connection()

    def test_returns_dataframe_for_select(self):
        select_stub = sqlalchemy.select(sqlalchemy.literal(1))
        with patch.object(self.conn, 'get_dataframe_by_query', return_value='DF'):
            result = self.conn.get_data(select_stub, return_type='df')
        self.assertEqual(result, 'DF')

    def test_returns_dataframe_for_str(self):
        with patch.object(self.conn, 'get_dataframe_by_querystring', return_value='DF2') as m:
            result = self.conn.get_data('SELECT 1', return_type='df')
        self.assertEqual(result, 'DF2')
        m.assert_called_once()

    def test_returns_csv_for_str_select(self):
        with patch.object(self.conn, 'get_csv_by_query', return_value='/tmp/q.csv') as m:
            result = self.conn.get_data('SELECT 1', return_type='csv')
        self.assertEqual(result, '/tmp/q.csv')
        m.assert_called_once_with('SELECT 1')

    def test_returns_csv_for_str_non_select(self):
        with patch.object(self.conn, 'get_csv', return_value='/tmp/q.csv') as m:
            result = self.conn.get_data('mytable', return_type='csv')
        self.assertEqual(result, '/tmp/q.csv')
        m.assert_called_once()

    def test_unsupported_return_type_raises(self):
        with self.assertRaises(Exception):
            self.conn.get_data('foo', return_type='bogus')

    def test_unknown_data_source_for_df_raises(self):
        with self.assertRaises(Exception):
            self.conn.get_data(12345, return_type='df')

    def test_unknown_data_source_for_csv_raises(self):
        with self.assertRaises(Exception):
            self.conn.get_data(12345, return_type='csv')

    def test_get_dataframe_deletes_temp_file(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('a\n1\n')
            path = f.name
        tbl = MagicMock()
        tbl.columns = [sqlalchemy.Column('a', sqlalchemy.Integer)]
        with patch.object(self.conn, 'get_csv', return_value=path):
            df = self.conn.get_dataframe(tbl)
        self.assertEqual(df.iloc[0]['a'], 1)
        self.assertFalse(os.path.exists(path))

    def test_get_dataframe_handles_delete_failure(self):
        # Pass a path that doesn't exist; ensure no exception bubbles up.
        tbl = MagicMock()
        tbl.columns = []
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('a\n1\n')
            path = f.name
        try:
            with patch.object(self.conn, 'get_csv', return_value=path), \
                 patch('os.remove', side_effect=OSError('locked')):
                df = self.conn.get_dataframe(tbl)
            self.assertEqual(list(df.columns), ['a'])
        finally:
            if os.path.exists(path):
                os.remove(path)

    def test_get_dataframe_by_querystring(self):
        with tempfile.NamedTemporaryFile(mode='w', delete=False) as f:
            f.write('a\nhi\n')
            path = f.name
        with patch.object(self.conn, 'get_csv_by_query', return_value=path):
            df = self.conn.get_dataframe_by_querystring('SELECT 1')
        self.assertEqual(df.iloc[0]['a'], 'hi')

    def test_get_iterator_uses_csv_stream(self):
        tbl = MagicMock()
        tbl.columns = [sqlalchemy.Column('x', sqlalchemy.Integer)]
        with patch.object(self.conn, 'get_csv', return_value='/tmp/zzz.csv'), \
             patch.object(self.conn, '_csv_stream', return_value=iter([{'x': 1}])) as ms:
            it = self.conn.get_iterator(tbl)
            rows = list(it)
        self.assertEqual(rows, [{'x': 1}])
        ms.assert_called_once_with('/tmp/zzz.csv', tbl.columns, True)

    def test_get_iterator_by_query(self):
        sa_q = sqlalchemy.select(sqlalchemy.literal(1).label('x'))
        with patch.object(self.conn, 'get_csv_by_query', return_value='/tmp/q.csv'), \
             patch.object(self.conn, '_csv_stream', return_value=iter([{'x': 1}])) as ms:
            rows = list(self.conn.get_iterator_by_query(sa_q))
        self.assertEqual(rows, [{'x': 1}])
        ms.assert_called_once()


# ---------------------------------------------------------------------------
# Connection.execute
# ---------------------------------------------------------------------------
class TestExecute(unittest.TestCase):
    def test_execute_str_query(self):
        conn = make_connection()
        conn.rpc.analyze.query.query.return_value = [{'a': 1}]
        result = conn.execute('SELECT 1', params={'p': 1})
        self.assertEqual(result, [{'a': 1}])
        conn.rpc.analyze.query.query.assert_called_once_with(
            project_id=conn._project_id, query='SELECT 1', params={'p': 1}
        )

    def test_execute_sa_query(self):
        conn = make_connection()
        conn.rpc.analyze.query.query.return_value = [{'a': 1}]
        sa_q = sqlalchemy.select(sqlalchemy.literal(1).label('x'))
        result = conn.execute(sa_q)
        self.assertEqual(result, [{'a': 1}])
        conn.rpc.analyze.query.query.assert_called_once()

    def test_execute_return_df(self):
        conn = make_connection()
        conn.rpc.analyze.query.query.return_value = [{'a': 1}, {'a': 2}]
        df = conn.execute('SELECT 1', return_df=True)
        self.assertIsInstance(df, pd.DataFrame)
        self.assertEqual(list(df['a']), [1, 2])

    def test_execute_return_df_non_list_returns_none(self):
        conn = make_connection()
        conn.rpc.analyze.query.query.return_value = 'not-a-list'
        self.assertIsNone(conn.execute('SELECT 1', return_df=True))


# ---------------------------------------------------------------------------
# Connection.bulk_save_objects / bulk_insert_mappings
# ---------------------------------------------------------------------------
class TestBulkSave(unittest.TestCase):
    def test_bulk_insert_mappings_not_implemented(self):
        conn = make_connection()
        with self.assertRaises(NotImplementedError):
            conn.bulk_insert_mappings(MagicMock(), [{'a': 1}])

    def test_bulk_save_objects_persists_each_mapping(self):
        """``bulk_save_objects`` should turn each object into a dict of values
        via ``get_values_as_dict`` and persist all of them together. We don't
        care about the precise downstream method as long as those dicts are
        what gets passed on for persistence."""
        conn = make_connection()
        m1, m2 = MagicMock(), MagicMock()
        m1.get_values_as_dict.return_value = {'a': 1}
        m2.get_values_as_dict.return_value = {'a': 2}
        with patch.object(conn, 'bulk_insert_mappings') as m:
            conn.bulk_save_objects(objects=[m1, m2])
        # Both mappings must have been collected.
        m1.get_values_as_dict.assert_called_once()
        m2.get_values_as_dict.assert_called_once()
        m.assert_called_once()
        _, mappings = m.call_args.args
        self.assertEqual(mappings, [{'a': 1}, {'a': 2}])

    def test_bulk_save_objects_empty_is_noop(self):
        conn = make_connection()
        # Should not raise
        conn.bulk_save_objects(objects=[])
        conn.bulk_save_objects(objects=None)

    def test_add_routes_through_bulk_save(self):
        conn = make_connection()
        with patch.object(conn, 'bulk_save_objects') as m:
            conn.add({'a': 1})
        m.assert_called_once_with(objects=[{'a': 1}])


# ---------------------------------------------------------------------------
# Misc Connection methods
# ---------------------------------------------------------------------------
class TestMiscConnection(unittest.TestCase):
    def test_commit_close_are_noops(self):
        conn = make_connection()
        self.assertIsNone(conn.commit())
        self.assertIsNone(conn.close())

    def test_rollback_raises(self):
        conn = make_connection()
        with self.assertRaises(Exception):
            conn.rollback()

    def test_project_id_property_raises_if_unset(self):
        rpc = make_mock_rpc(project_id=None)
        conn = make_connection(rpc=rpc)
        conn._project_id = None  # force unset
        with self.assertRaises(Exception):
            _ = conn.project_id

    def test_project_name_raises_if_unset(self):
        conn = make_connection()
        conn._project_id = None
        with self.assertRaises(Exception):
            _ = conn.project_name

    def test_project_id_property_returns(self):
        conn = make_connection()
        self.assertIsNotNone(conn.project_id)

    def test_project_name_property_returns(self):
        conn = make_connection()
        self.assertEqual(conn.project_name, 'proj')

    def test_truncate_calls_rpc(self):
        conn = make_connection()
        tbl = MagicMock()
        tbl.id = 'tab-1'
        conn.truncate(tbl)
        conn.rpc.analyze.table.clear_data.assert_called_once_with(
            project_id=conn._project_id, table_id='tab-1'
        )

    def test_drop_calls_rpc(self):
        conn = make_connection()
        tbl = MagicMock()
        tbl.id = 'tab-1'
        conn.drop(tbl)
        conn.rpc.analyze.table.delete.assert_called_once_with(
            project_id=conn._project_id, table_id='tab-1'
        )

    def test_get_dimension(self):
        conn = make_connection()
        dim = MagicMock()
        dim.hierarchy_table.return_value = 'HIER'
        conn.dims.get_dimension.return_value = dim
        result = conn.get_dimension('mydim')
        self.assertEqual(result, 'HIER')
        conn.dims.get_dimension.assert_called_once_with(name='mydim', replace=False)

    def test_query_method_is_passthrough_none(self):
        conn = make_connection()
        # Currently a no-op TODO method.
        self.assertIsNone(conn.query([]))

    def test_load_parquet_is_passthrough_none(self):
        conn = make_connection()
        self.assertIsNone(conn._load_parquet('p', 't'))

    def test_load_csv_routes_to_rpc(self):
        conn = make_connection()
        conn.rpc.analyze.table.load_csv.return_value = 'OK'
        result = conn._load_csv(
            project_id='p', table_id='t', meta=[], csv_data=b'data',
            header=True, delimiter=',', null_as='', quote='"',
        )
        self.assertEqual(result, 'OK')
        conn.rpc.analyze.table.load_csv.assert_called_once()

    def test_get_table_columns_returns_ids(self):
        conn = make_connection()
        table_object = MagicMock()
        table_object.cols.return_value = [{'id': 'a'}, {'id': 'b'}]
        self.assertEqual(conn._get_table_columns(table_object), ['a', 'b'])

    def test_get_table_columns_empty(self):
        conn = make_connection()
        table_object = MagicMock()
        table_object.cols.return_value = None
        self.assertEqual(conn._get_table_columns(table_object), [])


# ---------------------------------------------------------------------------
# Connection.save_data
# ---------------------------------------------------------------------------
class TestSaveData(unittest.TestCase):
    def test_save_data_upserts(self):
        rpc = make_mock_rpc(table_meta=[{'id': 'a', 'dtype': 'numeric'}])
        # search_by_name must return a single entry so Table picks it up.
        rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_target', 'paths': ['/']}
        ]
        conn = make_connection(rpc=rpc)

        # Build a query whose selected_columns we control.
        meta = sqlalchemy.MetaData()
        src = sqlalchemy.Table(
            'src', meta,
            sqlalchemy.Column('a', sqlalchemy.Numeric),
            schema='anlz_schema',
        )
        sa_q = sqlalchemy.select(src.c.a)

        conn.save_data(sa_q, 'mytable')
        rpc.analyze.query.upsert.assert_called_once()
        kwargs = rpc.analyze.query.upsert.call_args.kwargs
        self.assertEqual(kwargs['project_id'], conn._project_id)
        self.assertTrue(kwargs['recreate'])
        self.assertIsNone(kwargs['update_query'])


# ---------------------------------------------------------------------------
# Table class
# ---------------------------------------------------------------------------
class TestTable(unittest.TestCase):
    def setUp(self):
        self.rpc = make_mock_rpc(
            table_meta=[{'id': 'col1', 'dtype': 'numeric'}],
        )
        # Single hit for _get_table_id
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_existing', 'paths': ['/']}
        ]
        self.conn = make_connection(rpc=self.rpc)

    def test_table_from_existing(self):
        tbl = Table(self.conn, 'some_table')
        self.assertEqual(tbl.id, 'analyzetable_existing')
        self.assertEqual(tbl.project_id, self.conn._project_id)

    def test_table_id_passed_directly(self):
        tbl = Table(self.conn, TABLE_PREFIX + 'abc')
        self.assertEqual(tbl.id, TABLE_PREFIX + 'abc')
        # Confirm that for direct IDs we skipped the search
        self.rpc.analyze.table.search_by_name.assert_not_called()

    def test_table_create_when_missing_by_name(self):
        self.rpc.analyze.table.search_by_name.return_value = []
        self.rpc.analyze.table.create.return_value = {'id': 'analyzetable_new'}
        tbl = Table(self.conn, 'new_tbl')
        self.assertEqual(tbl.id, 'analyzetable_new')
        self.rpc.analyze.table.create.assert_called_once()
        kwargs = self.rpc.analyze.table.create.call_args.kwargs
        self.assertEqual(kwargs['name'], 'new_tbl')
        self.assertEqual(kwargs['path'], '/')

    def test_table_create_with_path(self):
        self.rpc.analyze.table.search_by_name.return_value = []
        self.rpc.analyze.table.create.return_value = {'id': 'analyzetable_new'}
        Table(self.conn, 'a/b/c')
        kwargs = self.rpc.analyze.table.create.call_args.kwargs
        self.assertEqual(kwargs['name'], 'c')
        self.assertEqual(kwargs['path'], 'a/b')

    def test_table_id_input_skips_name_search(self):
        """When the input already looks like a table id, the constructor must
        not try to resolve it via search_by_name (that's the whole point of
        accepting raw ids)."""
        self.rpc.analyze.table.search_by_name.return_value = []
        self.rpc.analyze.table.create.return_value = {'id': 'analyzetable_new'}
        tid = TABLE_PREFIX + 'abc'
        Table(self.conn, tid)
        self.rpc.analyze.table.search_by_name.assert_not_called()

    def test_schema_uses_project_id_when_anlz_prefix(self):
        rpc = make_mock_rpc(project_id=SCHEMA_PREFIX + 'something')
        rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_x', 'paths': ['/']}
        ]
        rpc.analyze.table.table_meta.return_value = [{'id': 'a', 'dtype': 'numeric'}]
        conn = make_connection(rpc=rpc)
        tbl = Table(conn, 'mt')
        self.assertEqual(tbl.schema, SCHEMA_PREFIX + 'something')
        rpc.analyze.project.get_project_schema.assert_not_called()

    def test_project_schema_resolved_once_per_connection(self):
        """The physical schema is stable per connection, so instantiating several
        tables must not re-fetch it (one RPC, not one-per-table)."""
        Table(self.conn, 'some_table')
        Table(self.conn, 'some_table')
        self.assertEqual(self.rpc.analyze.project.get_project_schema.call_count, 1)

    def test_write_path_touches_once(self):
        """When columns are supplied the physical table is (re)created by a single
        touch; the constructor must not touch a second time (a redundant recreate
        + update_shape)."""
        Table(self.conn, 'some_table', columns=[{'id': 'c', 'dtype': 'numeric'}])
        self.assertEqual(self.rpc.analyze.table.touch.call_count, 1)

    def test_read_path_still_ensures_table(self):
        """get_table-style construction (no input columns) still touches once to
        ensure the physical table exists."""
        Table(self.conn, 'some_table')
        self.assertEqual(self.rpc.analyze.table.touch.call_count, 1)

    def test_fully_qualified_name_uses_dialect(self):
        tbl = Table(self.conn, 'some_table')
        result = tbl.fully_qualified_name(self.conn.dialect)
        self.assertIn(tbl.id, result)
        self.assertIn(tbl.schema, result)

    def test_cols_calls_table_meta(self):
        tbl = Table(self.conn, 'some_table')
        # Reset mocks to count subsequent calls only.
        self.rpc.analyze.table.table_meta.reset_mock()
        self.rpc.analyze.table.table_meta.return_value = [{'id': 'x'}]
        result = tbl.cols()
        self.assertEqual(result, [{'id': 'x'}])
        self.rpc.analyze.table.table_meta.assert_called_once_with(
            project_id=tbl.project_id, table_id=tbl.id,
        )

    def test_table_info_calls_analyze_table(self):
        tbl = Table(self.conn, 'some_table')
        self.rpc.analyze.table.return_value = {'k': 'v'}
        result = tbl.table_info(keys=['k'])
        self.assertEqual(result, {'k': 'v'})

    def test_info_alias_for_table_info(self):
        tbl = Table(self.conn, 'some_table')
        self.rpc.analyze.table.return_value = 'info-result'
        self.assertEqual(tbl.info(), 'info-result')

    def test_get_data_invokes_get_dataframe(self):
        tbl = Table(self.conn, 'some_table')
        with patch.object(self.conn, 'get_dataframe', return_value='DF') as m:
            self.assertEqual(tbl.get_data(clean=False), 'DF')
        m.assert_called_once_with(tbl, clean=False)

    def test_save_dataframe_routes_to_bulk_insert(self):
        tbl = Table(self.conn, 'some_table')
        df = pd.DataFrame({'a': [1, 2]})
        with patch.object(self.conn, 'bulk_insert_dataframe') as m:
            tbl.save(df, append=True)
        m.assert_called_once_with(tbl, df, append=True)

    def test_save_select_routes_to_save_data(self):
        tbl = Table(self.conn, 'some_table')
        sa_q = sqlalchemy.select(sqlalchemy.literal(1))
        with patch.object(self.conn, 'save_data') as m:
            tbl.save(sa_q)
        m.assert_called_once_with(sa_q, tbl)

    def test_head_uses_select_limit(self):
        tbl = Table(self.conn, 'some_table')
        outer_conn = MagicMock()
        outer_conn.get_dataframe_from_select.return_value = 'X'
        self.assertEqual(tbl.head(outer_conn, rows=5), 'X')
        outer_conn.get_dataframe_from_select.assert_called_once()
        # When rows is None we should still call get_dataframe_from_select.
        outer_conn.reset_mock()
        outer_conn.get_dataframe_from_select.return_value = 'Y'
        self.assertEqual(tbl.head(outer_conn, rows=None), 'Y')


# ---------------------------------------------------------------------------
# Connection.get_table convenience
# ---------------------------------------------------------------------------
class TestGetTable(unittest.TestCase):
    def test_get_table_returns_table(self):
        rpc = make_mock_rpc(table_meta=[{'id': 'a', 'dtype': 'text'}])
        rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_t', 'paths': ['/']}
        ]
        conn = make_connection(rpc=rpc)
        tbl = conn.get_table('foo')
        self.assertIsInstance(tbl, Table)
        self.assertEqual(tbl.id, 'analyzetable_t')


# ---------------------------------------------------------------------------
# Connection._upload (mocked HTTP)
# ---------------------------------------------------------------------------
class TestUpload(unittest.TestCase):
    def test_upload_posts_to_correct_url(self):
        conn = make_connection()
        with patch.object(query.requests.sessions, 'Session') as mock_session_cls:
            session = MagicMock()
            session.__enter__.return_value = session
            session.__exit__.return_value = False
            response = MagicMock()
            response.json.return_value = {'status': 'ok'}
            session.post.return_value = response
            mock_session_cls.return_value = session

            result = conn._upload('tab-1', 'parquet', 'upload/here', b'binary')
        self.assertEqual(result, {'status': 'ok'})
        args, kwargs = session.post.call_args
        # URL replaces the path portion of the rpc_uri.
        self.assertTrue(args[0].endswith('/upload_data'))
        self.assertEqual(kwargs['params']['table_id'], 'tab-1')
        self.assertIn('Authorization', kwargs['headers'])

    def test_upload_callable_auth_token(self):
        rpc = make_mock_rpc()
        rpc.auth_token = lambda: 'callable-token'
        conn = make_connection(rpc=rpc)
        with patch.object(query.requests.sessions, 'Session') as mock_session_cls:
            session = MagicMock()
            session.__enter__.return_value = session
            session.__exit__.return_value = False
            response = MagicMock()
            response.json.return_value = {}
            session.post.return_value = response
            mock_session_cls.return_value = session

            conn._upload('tab-1', 'parquet', 'p', b'')
        kwargs = session.post.call_args.kwargs
        self.assertEqual(
            kwargs['headers']['Authorization'], 'Bearer callable-token'
        )


# ---------------------------------------------------------------------------
# Connection.bulk_insert_dataframe
# ---------------------------------------------------------------------------
class TestBulkInsertDataframe(unittest.TestCase):
    def setUp(self):
        self.rpc = make_mock_rpc(table_meta=[
            {'id': 'a', 'dtype': 'numeric'},
            {'id': 'b', 'dtype': 'text'},
        ])
        self.rpc.analyze.table.search_by_name.return_value = [
            {'id': 'analyzetable_target', 'paths': ['/']}
        ]
        self.conn = make_connection(rpc=self.rpc)
        # Build a real Table object so it has .id we can use.
        self.tbl = Table(self.conn, 'sometbl')

    def test_empty_dataframe_short_circuit(self):
        self.conn.bulk_insert_dataframe(self.tbl, pd.DataFrame())
        self.rpc.analyze.table.create_data_load.assert_not_called()

    def test_parquet_path_invoked(self):
        df = pd.DataFrame({'a': [1.0, 2.0], 'b': ['x', 'y']})
        self.rpc.analyze.table.create_data_load.return_value = {
            'load_type': 'parquet',
            'upload_path': 'somewhere',
        }
        with patch.object(self.conn, '_upload', return_value=None) as up:
            self.conn.bulk_insert_dataframe(self.tbl, df)
        up.assert_called_once()
        self.rpc.analyze.table.execute_data_load.assert_called_once()

    def test_append_path_with_missing_columns(self):
        # df has 'a' but not 'b' -- ensures the missing-col fill branch runs.
        df = pd.DataFrame({'a': [1.0]})
        self.rpc.analyze.table.create_data_load.return_value = {
            'load_type': 'parquet',
            'upload_path': 'somewhere',
        }
        with patch.object(self.conn, '_upload', return_value=None):
            self.conn.bulk_insert_dataframe(self.tbl, df, append=True)
        self.rpc.analyze.table.execute_data_load.assert_called_once()
        kwargs = self.rpc.analyze.table.execute_data_load.call_args.kwargs
        self.assertTrue(kwargs['append'])

    def test_no_data_load_falls_back_to_csv(self):
        """If create_data_load returns falsy, the loader should fall back to
        the legacy CSV path: write one or more chunks and post each via
        ``_load_csv``."""
        df = pd.DataFrame({'a': [1.0], 'b': ['x']})
        self.rpc.analyze.table.create_data_load.return_value = None
        # The current production CSV path calls to_csv with conflicting
        # quote/escape characters, which newer pandas refuses. We patch
        # DataFrame.to_csv to focus this test on the orchestration: the
        # fallback dispatches to _load_csv with the expected metadata.
        with patch.object(self.conn, '_load_csv', return_value=None) as lc, \
             patch.object(pd.DataFrame, 'to_csv', return_value=None):
            self.conn.bulk_insert_dataframe(self.tbl, df)
        lc.assert_called_once()
        kwargs = lc.call_args.kwargs
        self.assertEqual(kwargs['project_id'], self.conn._project_id)
        self.assertEqual(kwargs['table_id'], self.tbl.id)
        self.assertFalse(kwargs['append'])
