# coding=utf-8
import asyncio
import os
import pickle
import subprocess
import sys
import textwrap
import unittest
import uuid as uuid_mod

import datetime

import sqlalchemy
from sqlalchemy.exc import CompileError
# from toolz.functoolz import curry
# from toolz.functoolz import identity as ident

# from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.rpc.database import GUIDHyphens
from plaidcloud.utilities import sqlalchemy_functions as sf
from plaidcloud.utilities import sql_expression
# from plaidcloud.utilities.analyze_table import compiled


__author__ = "Patrick Buxton"
__copyright__ = "© Copyright 2009-2023, Tartan Solutions, Inc"
__credits__ = ["Patrick Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Patrick Buxton"
__email__ = "patrick.buxton@tartansolutions.com"


class BaseTest(unittest.TestCase):

    dialect = 'greenplum'

    def setUp(self) -> None:
        self.eng = sqlalchemy.create_engine(f'{self.dialect}://127.0.0.1/')

class DatabendTest(BaseTest):

    dialect = 'databend'


class StarrocksTest(BaseTest):

    dialect = 'starrocks'


class SnowflakeTest(BaseTest):

    dialect = 'snowflake'


class TestImportCol(BaseTest):

    def test_import_col_text(self):
        expr = sqlalchemy.func.import_col('Column1', 'text', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('%(import_col_1)s', str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])

    def test_import_col_numeric(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                         'THEN %(param_1)s ELSE CAST(%(import_col_1)s AS NUMERIC) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual(0.0, compiled.params['param_1'])

    def test_import_col_currency_empty_cell_is_zero(self):
        # Money columns mirror numeric's empty-cell semantics (0.0, not NULL)
        # so re-typing a column to currency doesn't change import behavior.
        expr = sqlalchemy.func.import_col('Column1', 'currency', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(0.0, compiled.params['param_1'])

    def test_import_col_numeric_trailing_negatives(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                        'THEN %(param_1)s ELSE to_number(%(import_col_1)s, %(to_number_1)s) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual(0.0, compiled.params['param_1'])
        self.assertEqual('9999999999999999999999999D9999999999999999999999999MI', compiled.params['to_number_1'])

    def test_import_col_interval(self):
        expr = sqlalchemy.func.import_col('Column1', 'interval', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                         'THEN NULL ELSE %(import_col_1)s::interval END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])


class TestImportColDatabend(DatabendTest):

    def test_import_col_text(self):
        expr = sqlalchemy.func.import_col('Column1', 'text', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('%(import_col_1)s', str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])

    def test_import_col_numeric(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, '
                         '%(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE '
                         'CAST(CASE WHEN (to_string(regexp_replace(%(import_col_1)s, '
                         '%(regexp_replace_4)s, %(regexp_replace_5)s)) = %(to_string_1)s) THEN NULL '
                         'ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                         '%(regexp_replace_5)s) END AS DECIMAL(38, 10)) END'), str(compiled))

        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_4'])
        self.assertEqual('', compiled.params['regexp_replace_5'])
        self.assertEqual(0.0, compiled.params['param_1'])
        self.assertEqual('NaN', compiled.params['to_string_1'])

    def test_import_col_numeric_trailing_negatives(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, '
                         '%(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE '
                         'CAST(CASE WHEN (to_string(CASE WHEN '
                         'regexp_like(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                         '%(regexp_replace_5)s), %(regexp_like_1)s) THEN concat(%(concat_1)s, '
                         'replace(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                         '%(regexp_replace_5)s), %(replace_1)s, %(replace_2)s)) ELSE '
                         'regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s) '
                         'END) = %(to_string_1)s) THEN NULL ELSE CASE WHEN '
                         'regexp_like(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                         '%(regexp_replace_5)s), %(regexp_like_1)s) THEN concat(%(concat_1)s, '
                         'replace(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                         '%(regexp_replace_5)s), %(replace_1)s, %(replace_2)s)) ELSE '
                         'regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s) '
                         'END END AS DECIMAL(38, 10)) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_4'])
        self.assertEqual('', compiled.params['regexp_replace_5'])
        self.assertEqual(0.0, compiled.params['param_1'])
        self.assertEqual('NaN', compiled.params['to_string_1'])
        self.assertEqual('^[0-9]*\\.?[0-9]*-$', compiled.params['regexp_like_1'])
        self.assertEqual('-', compiled.params['concat_1'])
        self.assertEqual('-', compiled.params['replace_1'])
        self.assertEqual('', compiled.params['replace_2'])

    def test_import_col_interval(self):
        expr = sqlalchemy.func.import_col('Column1', 'interval', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                          'THEN NULL ELSE to_interval(%(import_col_1)s) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])

    def test_import_cast_currency(self):
        expr = sqlalchemy.func.import_cast('Column1', 'currency', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CAST(CASE WHEN (to_string(regexp_replace(%(import_cast_1)s, '
                          '%(regexp_replace_1)s, %(regexp_replace_2)s)) = %(to_string_1)s) THEN NULL '
                          'ELSE regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s) END AS DECIMAL(18, 4))'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_cast_1'])
        self.assertEqual('NaN', compiled.params['to_string_1'])

    def test_import_cast_currency_trailing_negatives(self):
        # Shares numeric's sign-fixup logic but must still land on DECIMAL(18, 4)
        expr = sqlalchemy.func.import_cast('Column1', 'currency', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CAST(CASE WHEN (to_string(CASE WHEN regexp_like(regexp_replace(%(import_cast_1)s, '
                          '%(regexp_replace_1)s, %(regexp_replace_2)s), %(regexp_like_1)s) THEN '
                          'concat(%(concat_1)s, replace(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(replace_1)s, %(replace_2)s)) ELSE '
                          'regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) END) '
                          '= %(to_string_1)s) THEN NULL ELSE CASE WHEN '
                          'regexp_like(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(regexp_like_1)s) THEN concat(%(concat_1)s, '
                          'replace(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(replace_1)s, %(replace_2)s)) ELSE '
                          'regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) END '
                          'END AS DECIMAL(18, 4))'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_cast_1'])


# class TestLeft(BaseTest):
#     def test_left(self):
#         expr = sqlalchemy.func.left('somestring', 5)
#         compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
#         self.assertEqual(str(compiled), 'CAST(SUBSTRING(CAST(%(left_1)s AS TEXT) FROM %(substring_1)s FOR CAST(%(left_2)s AS INTEGER)) AS TEXT)')
#         self.assertEqual('somestring', compiled.params['left_1'])
#         self.assertEqual(1, compiled.params['substring_1'])
#         self.assertEqual(5, compiled.params['left_2'])

class TestImportColStarrocks(TestImportCol, StarrocksTest):
    def test_import_col_numeric(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                          'THEN %(param_1)s ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual(0.0, compiled.params['param_1'])

    def test_import_col_numeric_trailing_negatives(self):
        # StarRocks has no to_number(); the trailing-negatives import_cast path
        # therefore falls through to the wide-decimal cast (to_number specializes
        # to CAST(... AS DECIMAL(38, 10)) on StarRocks). Trailing-minus handling
        # is lost — bad input yields NULL rather than a hard error.
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) '
                          'THEN %(param_1)s ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual(0.0, compiled.params['param_1'])

    def test_import_cast_currency(self):
        expr = sqlalchemy.func.import_cast('Column1', 'currency', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(import_cast_1)s AS DECIMAL(18, 4))', str(compiled))
        self.assertEqual('Column1', compiled.params['import_cast_1'])

    def test_import_cast_currency_trailing_negatives(self):
        # Same as numeric: to_number specializes to the wide-decimal cast on
        # StarRocks. The (18, 4) target column still governs storage width.
        expr = sqlalchemy.func.import_cast('Column1', 'currency', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(import_cast_1)s AS DECIMAL(38, 10))', str(compiled))


# --- Typed staging compile switch (sc-23281) --------------------------------

#: (sql, params) of import_col('Column1', <dtype>, 'YYYY-MM-DD', False)
#: compiled with render_postcompile, captured on master BEFORE the
#: typed_staging switch existed. Flag-off compilation must stay byte-identical
#: to these: the CSV import path is untouched by the typed-staging work.
#: (time/bigint/smallint were captured on this branch — the flag-off path is
#: purely additive vs master, and one of them, databend/bigint, was hand-
#: compiled against master's copy of the module and matched byte-for-byte.)
#: (hana is the one import_cast dialect variant not covered — sqlalchemy-hana
#: isn't an installable test dependency; it is reachable only through
#: compile_import_col, so the switch covers it by construction.)
IMPORT_COL_SNAPSHOTS = {
    ('greenplum', 'text'): (
        '%(import_col_1)s',
        {'import_col_1': 'Column1'},
    ),
    ('greenplum', 'numeric'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(%(import_col_1)s AS NUMERIC) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0},
    ),
    ('greenplum', 'currency'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE %(import_col_1)s END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0},
    ),
    ('greenplum', 'integer'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS NUMERIC) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('greenplum', 'date'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_date(nullif(trim(CAST(CAST(%(import_col_1)s AS TEXT) AS '
         'TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'nullif_1': '', 'param_1': 'YYYY-MM-DD'},
    ),
    ('greenplum', 'timestamp'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), '
         'CAST(%(to_timestamp_1)s AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'to_timestamp_1': 'YYYY-MM-DD'},
    ),
    ('greenplum', 'boolean'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE %(import_col_1)s::boolean END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('greenplum', 'interval'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE %(import_col_1)s::interval END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('greenplum', 'time'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), '
         'CAST(%(to_timestamp_1)s AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'to_timestamp_1': 'HH24:MI:SS'},
    ),
    ('greenplum', 'bigint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS NUMERIC) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('greenplum', 'smallint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS NUMERIC) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('databend', 'text'): (
        '%(import_col_1)s',
        {'import_col_1': 'Column1'},
    ),
    ('databend', 'numeric'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(CASE WHEN '
         '(to_string(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s)) = '
         '%(to_string_1)s) THEN NULL ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) END AS DECIMAL(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0, 'regexp_replace_4': '\\s*', 'regexp_replace_5': '', 'to_string_1': 'NaN'},
    ),
    ('databend', 'currency'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(CASE WHEN '
         '(to_string(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s)) = '
         '%(to_string_1)s) THEN NULL ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) END AS DECIMAL(18, 4)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0, 'regexp_replace_4': '\\s*', 'regexp_replace_5': '', 'to_string_1': 'NaN'},
    ),
    ('databend', 'integer'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_int32(regexp_replace(%(import_col_1)s, '
         '%(regexp_replace_4)s, %(regexp_replace_5)s)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('databend', 'date'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_date(to_timestamp(nullif(TRIM(CAST(CAST(%(import_col_1)s AS '
         'TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'nullif_1': '', 'param_1': '%Y-%m-%d'},
    ),
    ('databend', 'timestamp'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), CAST(%(param_1)s '
         'AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': '%Y-%m-%d'},
    ),
    ('databend', 'boolean'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_boolean(CAST(CASE WHEN (to_string(%(import_col_1)s) = '
         '%(to_string_1)s) THEN %(param_1)s WHEN (to_string(%(import_col_1)s) = %(to_string_2)s) THEN '
         '%(param_2)s WHEN (to_string(%(import_col_1)s) = %(to_string_3)s) THEN %(param_3)s WHEN '
         '(to_string(%(import_col_1)s) = %(to_string_4)s) THEN %(param_4)s ELSE %(import_col_1)s END AS '
         'VARCHAR)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'to_string_1': 't', 'param_1': 'TRUE', 'to_string_2': '1', 'param_2': 'TRUE', 'to_string_3': 'f', 'param_3': 'FALSE', 'to_string_4': '0', 'param_4': 'FALSE'},
    ),
    ('databend', 'interval'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_interval(%(import_col_1)s) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('databend', 'time'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), CAST(%(param_1)s '
         'AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': '%H:%M:%S'},
    ),
    ('databend', 'bigint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_int64(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s,'
         ' %(regexp_replace_5)s)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('databend', 'smallint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_int16(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s,'
         ' %(regexp_replace_5)s)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('starrocks', 'text'): (
        '%(import_col_1)s',
        {'import_col_1': 'Column1'},
    ),
    ('starrocks', 'numeric'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0},
    ),
    ('starrocks', 'currency'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(%(import_col_1)s AS DECIMAL(18, 4)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0},
    ),
    ('starrocks', 'integer'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('starrocks', 'date'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE str2date(nullif(trim(CAST(CAST(%(import_col_1)s AS CHAR) AS '
         'CHAR)), %(nullif_1)s), CAST(%(param_1)s AS CHAR)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'nullif_1': '', 'param_1': '%Y-%m-%d'},
    ),
    ('starrocks', 'timestamp'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE str_to_date(CAST(%(import_col_1)s AS CHAR), CAST(%(param_1)s '
         'AS CHAR)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': '%Y-%m-%d'},
    ),
    ('starrocks', 'boolean'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS BOOLEAN) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('starrocks', 'interval'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE %(import_col_1)s::interval END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('starrocks', 'time'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE str_to_date(CAST(%(import_col_1)s AS CHAR), CAST(%(param_1)s AS'
         ' CHAR)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': '%H:%M:%S'},
    ),
    ('starrocks', 'bigint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('starrocks', 'smallint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(%(import_col_1)s AS DECIMAL(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('snowflake', 'text'): (
        '%(import_col_1)s',
        {'import_col_1': 'Column1'},
    ),
    ('snowflake', 'numeric'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(CASE WHEN '
         '(to_varchar(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s)) = '
         '%(to_string_1)s) THEN NULL ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) END AS NUMERIC(38, 10)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0, 'regexp_replace_4': '\\s*', 'regexp_replace_5': '', 'to_string_1': 'NaN'},
    ),
    ('snowflake', 'currency'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(CASE WHEN '
         '(to_varchar(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s)) = '
         '%(to_string_1)s) THEN NULL ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) END AS NUMERIC(18, 4)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 0.0, 'regexp_replace_4': '\\s*', 'regexp_replace_5': '', 'to_string_1': 'NaN'},
    ),
    ('snowflake', 'integer'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) AS INTEGER) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('snowflake', 'date'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_date(nullif(trim(CAST(CAST(%(import_col_1)s AS TEXT) AS '
         'TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'nullif_1': '', 'param_1': 'YYYY-MM-DD'},
    ),
    ('snowflake', 'timestamp'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), CAST(%(param_1)s '
         'AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 'YYYY-MM-DD'},
    ),
    ('snowflake', 'boolean'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_boolean(CAST(%(import_col_1)s AS VARCHAR)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': ''},
    ),
    ('snowflake', 'time'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE to_timestamp(CAST(%(import_col_1)s AS TEXT), CAST(%(param_1)s '
         'AS TEXT)) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'param_1': 'HH24:MI:SS'},
    ),
    ('snowflake', 'bigint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) AS BIGINT) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('snowflake', 'smallint'): (
        ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
         '%(regexp_replace_3)s) THEN NULL ELSE CAST(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
         '%(regexp_replace_5)s) AS SMALLINT) END'),
        {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '', 'regexp_replace_3': '', 'regexp_replace_4': '\\s*', 'regexp_replace_5': ''},
    ),
    ('snowflake', 'interval'): CompileError,
}


class TestTypedStagingImportCol(unittest.TestCase):

    @staticmethod
    def _compile(dialect, dtype, trailing_negs=False):
        # Build via the canonical module reference, NOT sqlalchemy.func: this
        # repo's CI pytest invocation (--doctest-modules on a namespace
        # package) imports sqlalchemy_functions a second time as
        # 'utilities.sqlalchemy_functions', and the duplicate's registration
        # wins the sqlalchemy.func registry — func.import_col() would then
        # dispatch to the duplicate's @compiles handler, which reads the
        # duplicate module's typed_staging ContextVar while these tests set
        # sf's. sf.import_col guarantees element class, handler and contextvar
        # are same-module regardless of import order.
        eng = sqlalchemy.create_engine(f'{dialect}://127.0.0.1/')
        expr = sf.import_col('Column1', dtype, 'YYYY-MM-DD', trailing_negs)
        compiled = expr.compile(dialect=eng.dialect, compile_kwargs={"render_postcompile": True})
        return str(compiled), dict(compiled.params)

    def test_flag_off_is_byte_identical_to_pre_switch_sql(self):
        for (dialect, dtype), expected in IMPORT_COL_SNAPSHOTS.items():
            with self.subTest(dialect=dialect, dtype=dtype):
                if expected is CompileError:
                    with self.assertRaises(CompileError):
                        self._compile(dialect, dtype)
                else:
                    self.assertEqual(expected, self._compile(dialect, dtype))

    def test_flag_on_compiles_to_bare_column(self):
        # The converter already parsed/typed/coerced the value, so every dtype
        # passes through — except interval, which stages as TEXT and keeps its
        # projection cast (see test_flag_on_interval_keeps_projection_cast).
        for dialect, dtype in IMPORT_COL_SNAPSHOTS:
            if dtype == 'interval':
                continue
            with self.subTest(dialect=dialect, dtype=dtype):
                with sf.typed_staging_compilation():
                    self.assertEqual(
                        ('%(import_col_1)s', {'import_col_1': 'Column1'}),
                        self._compile(dialect, dtype),
                    )

    def test_flag_on_interval_keeps_projection_cast(self):
        # Interval has no Arrow mapping: the Parquet converter stages it as
        # TEXT, so flag-ON must keep the old else-branch import_cast
        # (col::interval / to_interval) — the verified old text-staging shape
        # — rather than a bare column, which would lean on an unverified
        # engine implicit cast of string staging into an Interval temp column
        # (sc-23281 m-5). No blank CASE: the converter nulls blanks itself.
        expected = {
            'greenplum': ('%(import_col_1)s::interval', {'import_col_1': 'Column1'}),
            'databend': ('to_interval(%(import_col_1)s)', {'import_col_1': 'Column1'}),
            'starrocks': ('%(import_col_1)s::interval', {'import_col_1': 'Column1'}),
            'snowflake': CompileError,  # no GA INTERVAL type — same loud refusal as flag-off
        }
        for dialect, snapshot in expected.items():
            with self.subTest(dialect=dialect):
                with sf.typed_staging_compilation():
                    if snapshot is CompileError:
                        with self.assertRaises(CompileError):
                            self._compile(dialect, 'interval')
                    else:
                        self.assertEqual(snapshot, self._compile(dialect, 'interval'))

    def test_flag_off_snowflake_trailing_negatives_numeric(self):
        # Pins the one flag-off branch the (dialect, dtype) grid can't reach:
        # trailing_negs=True routes numerics through the regexp_like/concat
        # sign-folding CASE on Snowflake.
        sql, params = self._compile('snowflake', 'numeric', trailing_negs=True)
        self.assertEqual(
            ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = '
             '%(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(CASE WHEN (to_varchar(CASE WHEN '
             'regexp_like(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s), '
             '%(regexp_like_1)s) THEN concat(%(concat_1)s, replace(regexp_replace(%(import_col_1)s, '
             '%(regexp_replace_4)s, %(regexp_replace_5)s), %(replace_1)s, %(replace_2)s)) ELSE '
             'regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s) END) = '
             '%(to_string_1)s) THEN NULL ELSE CASE WHEN regexp_like(regexp_replace(%(import_col_1)s, '
             '%(regexp_replace_4)s, %(regexp_replace_5)s), %(regexp_like_1)s) THEN concat(%(concat_1)s, '
             'replace(regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, %(regexp_replace_5)s), '
             '%(replace_1)s, %(replace_2)s)) ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
             '%(regexp_replace_5)s) END END AS NUMERIC(38, 10)) END'),
            sql,
        )
        self.assertEqual(
            {'import_col_1': 'Column1', 'regexp_replace_1': '\\s*', 'regexp_replace_2': '',
             'regexp_replace_3': '', 'param_1': 0.0, 'regexp_replace_4': '\\s*', 'regexp_replace_5': '',
             'regexp_like_1': '^[0-9]*\\.?[0-9]*-$', 'concat_1': '-', 'replace_1': '-', 'replace_2': '',
             'to_string_1': 'NaN'},
            params,
        )

    def test_registry_construction_honours_flag_in_fresh_interpreter(self):
        # Production builds the element through SQLAlchemy's registry, not the
        # module attribute: import_data_query renders each column expression as
        # the string 'func.import_col(...)' (sql_expression.py, ~line 1351) and
        # eval_expression evals it with {'func': sqlalchemy.func}
        # (get_safe_dict). Pin that THAT construction path honours the flag
        # when only the canonical module is loaded — in a fresh interpreter,
        # because under this repo's own pytest invocation the doctest-duplicate
        # module can hijack the func registry (see _compile), which is a test-
        # harness artifact production never has.
        script = textwrap.dedent('''
            import sqlalchemy
            import plaidcloud.utilities.sqlalchemy_functions as sf
            expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
            eng = sqlalchemy.create_engine('greenplum://127.0.0.1/')
            with sf.typed_staging_compilation():
                on = str(expr.compile(dialect=eng.dialect))
            off = str(expr.compile(dialect=eng.dialect))
            assert on == '%(import_col_1)s', f'flag ignored: {on}'
            assert 'CASE WHEN' in off, f'flag leaked: {off}'
        ''')
        repo_root = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..', '..'))
        result = subprocess.run(
            [sys.executable, '-c', script], cwd=repo_root,
            capture_output=True, text=True, check=False,
        )
        self.assertEqual(0, result.returncode, result.stderr)

    def test_same_tree_swaps_modes_without_cache_leakage(self):
        eng = sqlalchemy.create_engine('greenplum://127.0.0.1/')
        expr = sf.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        off_before = str(expr.compile(dialect=eng.dialect))
        with sf.typed_staging_compilation():
            on = str(expr.compile(dialect=eng.dialect))
        off_after = str(expr.compile(dialect=eng.dialect))
        self.assertEqual(off_before, off_after)
        self.assertEqual('%(import_col_1)s', on)
        self.assertNotEqual(off_before, on)
        # inherit_cache=False must keep any statement containing import_col out
        # of SQLAlchemy's compiled-query cache, or one mode's SQL could be
        # replayed under the other.
        self.assertIsNone(sqlalchemy.select(expr)._generate_cache_key())

    def test_pickled_import_data_query_compiles_passthrough(self):
        # Deploy safety: the workflow-runner pickles the extract query built by
        # its own (possibly older) plaidcloud-utilities; the import worker
        # unpickles it and compiles at execution time. The switch must take
        # effect on that unpickled tree with no runner change.
        query = sql_expression.import_data_query(
            '_schema',
            'table_54321',
            [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}],
            [
                {'target': 'NumCol', 'source': 'Column2', 'dtype': 'numeric'},
                {'target': 'TsCol', 'source': 'Column1', 'dtype': 'timestamp'},
            ],
            date_format='YYYY-MM-DD',
            temp_table_id='temp_table',
            config={'project_schema': 'anlz_schema'},
        )
        restored = pickle.loads(pickle.dumps(query))
        eng = sqlalchemy.create_engine('greenplum://127.0.0.1/')
        with sf.typed_staging_compilation():
            passthrough_sql = str(restored.compile(dialect=eng.dialect))
        self.assertNotIn('regexp_replace', passthrough_sql)
        self.assertNotIn('to_timestamp', passthrough_sql)
        self.assertIn('text_import."Column2"', passthrough_sql)
        # and the same unpickled tree still compiles the CSV machinery flag-off
        csv_sql = str(restored.compile(dialect=eng.dialect))
        self.assertIn('regexp_replace', csv_sql)
        self.assertIn('to_timestamp', csv_sql)

    def test_flag_propagates_into_asyncio_to_thread(self):
        # The worker sets the flag around query execution and runs the actual
        # compile/execute on a thread via asyncio.to_thread, which copies the
        # caller's context — the flag must survive that hop.
        eng = sqlalchemy.create_engine('greenplum://127.0.0.1/')
        expr = sf.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)

        async def compile_in_thread():
            with sf.typed_staging_compilation():
                return await asyncio.to_thread(lambda: str(expr.compile(dialect=eng.dialect)))

        self.assertEqual('%(import_col_1)s', asyncio.run(compile_in_thread()))


class TestZfill(BaseTest):
    def test_zfill(self):
        expr = sqlalchemy.func.zfill('foobar', 2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'lpad(CAST(%(zfill_1)s AS TEXT), greatest(CAST(%(zfill_2)s AS INTEGER), length(CAST(%(zfill_1)s AS TEXT))), %(lpad_1)s)',
            str(compiled),
        )
        self.assertEqual('foobar', compiled.params['zfill_1'])
        self.assertEqual(2, compiled.params['zfill_2'])
        self.assertEqual('0', compiled.params['lpad_1'])

    def test_zfill_char(self):
        expr = sqlalchemy.func.zfill('foobar', 2, '#')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'lpad(CAST(%(zfill_1)s AS TEXT), greatest(CAST(%(zfill_2)s AS INTEGER), length(CAST(%(zfill_1)s AS TEXT))), CAST(%(zfill_3)s AS TEXT))',
            str(compiled),
        )
        self.assertEqual('foobar', compiled.params['zfill_1'])
        self.assertEqual(2, compiled.params['zfill_2'])
        self.assertEqual('#', compiled.params['zfill_3'])


class TestNormalizeWhitespace(BaseTest):
    def test_normalize_whitespace(self):
        expr = sqlalchemy.func.normalize_whitespace('foobar')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'regexp_replace(CAST(%(normalize_whitespace_1)s AS TEXT), %(regexp_replace_1)s, %(regexp_replace_2)s, %(regexp_replace_3)s)',
            str(compiled),
        )
        self.assertEqual('foobar', compiled.params['normalize_whitespace_1'])
        ww_re = '[' + ''.join(['\\' + c for c in sf.WEIRD_WHITESPACE_CHARS]) + ']+'
        self.assertEqual(ww_re, compiled.params['regexp_replace_1']) # TODO
        self.assertEqual(' ', compiled.params['regexp_replace_2'])
        self.assertEqual('g', compiled.params['regexp_replace_3'])


class TestLeft(BaseTest):
    def test_left(self):
        expr = sqlalchemy.func.left('somestring', 5)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('left(%(left_1)s, %(left_2)s)', str(compiled))
        self.assertEqual('somestring', compiled.params['left_1'])
        self.assertEqual(5, compiled.params['left_2'])


class TestSliceString(BaseTest):
    def test_slice_string(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', 4)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(5, compiled.params['substring_1'])

    def test_slice_string_none(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', 4, None)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(5, compiled.params['substring_1'])

    def test_slice_string_pos_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', 4, 2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s FOR %(substring_2)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(5, compiled.params['substring_1'])
        self.assertEqual(2, compiled.params['substring_2'])

    def test_slice_string_neg_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', 4, -2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('left(CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT), %(left_1)s)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(5, compiled.params['substring_1'])
        self.assertEqual(-2, compiled.params['left_1'])


    def test_slice_string_neg_start(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', -4)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('right(CAST(%(slice_string_1)s AS TEXT), %(right_1)s)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(4, compiled.params['right_1'])

    def test_slice_string_neg_start_none(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', -4, None)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('right(CAST(%(slice_string_1)s AS TEXT), %(right_1)s)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(4, compiled.params['right_1'])

    def test_slice_string_neg_start_pos_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', -4, 2)
        with self.assertRaises(NotImplementedError):
            expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})

        # self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s FOR %(substring_2)s) AS TEXT)', str(compiled))
        # self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        # self.assertEqual(5, compiled.params['substring_1'])
        # self.assertEqual(2, compiled.params['substring_2'])

    def test_slice_string_neg_start_neg_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', -4, -2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('left(right(CAST(%(slice_string_1)s AS TEXT), %(right_1)s), %(left_1)s)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(4, compiled.params['right_1'])
        self.assertEqual(2, compiled.params['left_1'])


    def test_slice_string_none_start(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(1, compiled.params['substring_1'])


    def test_slice_string_none_start_none(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', None, None)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(1, compiled.params['substring_1'])

    def test_slice_string_none_start_pos_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', None, 2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s FOR %(substring_2)s) AS TEXT)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(1, compiled.params['substring_1'])
        self.assertEqual(2, compiled.params['substring_2'])

    def test_slice_string_none_start_neg_count(self):
        expr = sqlalchemy.func.slice_string('abcdefghijk', None, -2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('left(CAST(SUBSTRING(CAST(%(slice_string_1)s AS TEXT) FROM %(substring_1)s) AS TEXT), %(left_1)s)', str(compiled))
        self.assertEqual('abcdefghijk', compiled.params['slice_string_1'])
        self.assertEqual(1, compiled.params['substring_1'])
        self.assertEqual(-2, compiled.params['left_1'])


class TestDateAdd(BaseTest):
    def test_date_add(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt, years=1, months=2, weeks=3, days=4, hours=5, minutes=6, seconds=7)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'CAST(%(date_add_1)s AS TIMESTAMP WITHOUT TIME ZONE) + make_interval(CAST(%(param_1)s AS INTEGER), CAST(%(param_2)s AS INTEGER), CAST(%(param_3)s AS INTEGER), CAST(%(param_4)s AS INTEGER), CAST(%(param_5)s AS INTEGER), CAST(%(param_6)s AS INTEGER), CAST(%(param_7)s AS INTEGER))',
            str(compiled),
        )
        self.assertEqual(dt, compiled.params['date_add_1'])
        self.assertEqual(1, compiled.params['param_1'])
        self.assertEqual(2, compiled.params['param_2'])
        self.assertEqual(3, compiled.params['param_3'])
        self.assertEqual(4, compiled.params['param_4'])
        self.assertEqual(5, compiled.params['param_5'])
        self.assertEqual(6, compiled.params['param_6'])
        self.assertEqual(7, compiled.params['param_7'])

    def test_date_add_no_params(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'CAST(%(date_add_1)s AS TIMESTAMP WITHOUT TIME ZONE) + make_interval(CAST(%(param_1)s AS INTEGER), CAST(%(param_2)s AS INTEGER), CAST(%(param_3)s AS INTEGER), CAST(%(param_4)s AS INTEGER), CAST(%(param_5)s AS INTEGER), CAST(%(param_6)s AS INTEGER), CAST(%(param_7)s AS INTEGER))',
            str(compiled),
        )
        self.assertEqual(dt, compiled.params['date_add_1'])
        self.assertEqual(0, compiled.params['param_1'])
        self.assertEqual(0, compiled.params['param_2'])
        self.assertEqual(0, compiled.params['param_3'])
        self.assertEqual(0, compiled.params['param_4'])
        self.assertEqual(0, compiled.params['param_5'])
        self.assertEqual(0, compiled.params['param_6'])
        self.assertEqual(0, compiled.params['param_7'])


class TestDateAddStarrocks(StarrocksTest):
    def test_date_add(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt, years=1, months=2, weeks=3, days=4, hours=5, minutes=6, seconds=7)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'seconds_add(minutes_add(hours_add(days_add(weeks_add(months_add(years_add(CAST(%(date_add_1)s AS DATETIME), %(years_add_1)s), %(months_add_1)s), %(weeks_add_1)s), %(days_add_1)s), %(hours_add_1)s), %(minutes_add_1)s), %(seconds_add_1)s)',
            str(compiled),
        )
        self.assertEqual(dt, compiled.params['date_add_1'])
        self.assertEqual(1, compiled.params['years_add_1'])
        self.assertEqual(2, compiled.params['months_add_1'])
        self.assertEqual(3, compiled.params['weeks_add_1'])
        self.assertEqual(4, compiled.params['days_add_1'])
        self.assertEqual(5, compiled.params['hours_add_1'])
        self.assertEqual(6, compiled.params['minutes_add_1'])
        self.assertEqual(7, compiled.params['seconds_add_1'])

    def test_date_add_weeks_and_days(self):
        expr = sqlalchemy.func.date_add(sqlalchemy.func.date_trunc('WEEK', sqlalchemy.func.now()), weeks=6, days=7)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})
        self.assertEqual(
            "days_add(weeks_add(CAST(date_trunc('WEEK', now()) AS DATETIME), 6), 7)",
            str(compiled),
        )

    def test_date_add_no_params(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(date_add_1)s AS DATETIME)', str(compiled))
        self.assertEqual(dt, compiled.params['date_add_1'])


class TestTransactionTimestamp(DatabendTest):
    def test_transaction_timestamp(self):
        expr = sqlalchemy.func.transaction_timestamp()
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('now()', str(compiled))

class TestStrpos(DatabendTest):
    def test_strpos(self):
        expr = sqlalchemy.func.strpos('databend', 'be')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('locate(%(strpos_1)s, %(strpos_2)s)', str(compiled))
        self.assertEqual('be', compiled.params['strpos_1'])
        self.assertEqual('databend', compiled.params['strpos_2'])

class TestStringToArray(DatabendTest):
    def test_string_to_array(self):
        expr = sqlalchemy.func.string_to_array('1,2,3,4', ',')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'split(%(string_to_array_1)s, CASE WHEN (%(string_to_array_2)s = %(param_1)s OR %(string_to_array_2)s IS NULL) THEN %(param_2)s ELSE %(string_to_array_2)s END)',
            str(compiled)
        )
        self.assertEqual('1,2,3,4', compiled.params['string_to_array_1'])
        self.assertEqual(',', compiled.params['string_to_array_2'])
        self.assertEqual('', compiled.params['param_1'])
        self.assertEqual('', compiled.params['param_2'])

class TestToNumber(DatabendTest):
    def test_to_number(self):
        expr = sqlalchemy.func.to_number('12345', '999999')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_int64(%(to_number_1)s)', str(compiled))
        self.assertEqual('12345', compiled.params['to_number_1'])


class TestLTrim(DatabendTest):
    def test_ltrim_plain(self):
        expr = sqlalchemy.func.ltrim('12345', '')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(LEADING \' \' FROM CAST(%(ltrim_1)s AS TEXT))', str(compiled))
        self.assertEqual('12345', compiled.params['ltrim_1'])

    def test_ltrim_specific(self):
        expr = sqlalchemy.func.ltrim('12345', '1')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(LEADING %(ltrim_1)s FROM CAST(%(ltrim_2)s AS TEXT))', str(compiled))
        self.assertEqual('1', compiled.params['ltrim_1'])
        self.assertEqual('12345', compiled.params['ltrim_2'])


class TestRTrim(DatabendTest):
    def test_rtrim_plain(self):
        expr = sqlalchemy.func.rtrim('12345', '')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(TRAILING \' \' FROM CAST(%(rtrim_1)s AS TEXT))', str(compiled))
        self.assertEqual('12345', compiled.params['rtrim_1'])

    def test_rtrim_specific(self):
        expr = sqlalchemy.func.rtrim('12345', '5')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(TRAILING %(rtrim_1)s FROM CAST(%(rtrim_2)s AS TEXT))', str(compiled))
        self.assertEqual('5', compiled.params['rtrim_1'])
        self.assertEqual('12345', compiled.params['rtrim_2'])


class TestTrim(DatabendTest):
    def test_trim_plain(self):
        expr = sqlalchemy.func.trim('12345', '')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(CAST(%(trim_1)s AS TEXT))', str(compiled))
        self.assertEqual('12345', compiled.params['trim_1'])

    def test_trim_specific(self):
        expr = sqlalchemy.func.trim('12345', '5')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('TRIM(BOTH %(trim_1)s FROM CAST(%(trim_2)s AS TEXT))', str(compiled))
        self.assertEqual('5', compiled.params['trim_1'])
        self.assertEqual('12345', compiled.params['trim_2'])


class TestToChar(DatabendTest):
    def test_to_char_number(self):
        expr = sqlalchemy.func.to_char(123456.789, 'LFM999,999,999,999D00')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'to_char(%(to_char_1)s, \'$FM999,999,999,999.00\')',
            str(compiled),
        )
        self.assertEqual(123456.789, compiled.params['to_char_1'])

    def test_to_char_date(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_string(%(to_char_1)s, %(to_string_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['to_char_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['to_string_1'])

    def test_to_char_date_i(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'IYYY-IW')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_string(%(to_char_1)s, %(to_string_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['to_char_1'])
        self.assertEqual('%G-%V', compiled.params['to_string_1'])

    def test_to_char_time(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'HH24:MI:SS')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_string(%(to_char_1)s, %(to_string_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['to_char_1'])
        self.assertEqual('%H:%M:%S', compiled.params['to_string_1'])


class TestSafeToDate(BaseTest):
    def test_to_date(self):
        expr = sqlalchemy.func.to_date('2019-01-05')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier(self):
        expr = sqlalchemy.func.to_date('2019-01-05', 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('YYYY-MM-DD', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier_python(self):
        expr = sqlalchemy.func.to_date('2019-01-05', '%Y-%m-%d')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('YYYY-MM-DD', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

class TestSafeToDateDB(DatabendTest):
    def test_to_date(self):
        expr = sqlalchemy.func.to_date('2019-01-05')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(TRIM(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier(self):
        expr = sqlalchemy.func.to_date('2019-01-05', '%Y-%m-%d')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(to_timestamp(nullif(TRIM(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT)))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier_postgres(self):
        expr = sqlalchemy.func.to_date('2019-01-05', 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(to_timestamp(nullif(TRIM(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT)))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

class TestSafeToDateSR(StarrocksTest):
    def test_to_date(self):
        expr = sqlalchemy.func.to_date('2019-01-05')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS STRING) AS STRING)), %(nullif_1)s))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier(self):
        expr = sqlalchemy.func.to_date('2019-01-05', '%Y-%m-%d')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('str2date(nullif(trim(CAST(CAST(%(to_date_1)s AS STRING) AS STRING)), %(nullif_1)s), CAST(%(param_1)s AS STRING))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier_postgres(self):
        expr = sqlalchemy.func.to_date('2019-01-05', 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('str2date(nullif(trim(CAST(CAST(%(to_date_1)s AS STRING) AS STRING)), %(nullif_1)s), CAST(%(param_1)s AS STRING))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

class TestSafeExtract(BaseTest):
    def test_extract_datetime(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.extract('year', dt)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM %(extract_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['extract_1'])

    def test_extract_sql_datetime(self):
        c = sqlalchemy.column("id", self.eng.dialect.type_descriptor(sqlalchemy.types.DateTime))
        expr = sqlalchemy.func.extract('year', c)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM id)', str(compiled))

    def test_extract_sql_date(self):
        c = sqlalchemy.column("id", self.eng.dialect.type_descriptor(sqlalchemy.types.Date))
        expr = sqlalchemy.func.extract('year', c)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM id)', str(compiled))


class TestSafeExtractDB(DatabendTest):
    def test_extract_datetime(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.extract('year', dt)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM %(extract_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['extract_1'])

    def test_extract_sql_datetime(self):
        c = sqlalchemy.column("id", self.eng.dialect.type_descriptor(sqlalchemy.types.DateTime))
        expr = sqlalchemy.func.extract('year', c)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM id)', str(compiled))

    def test_extract_sql_date(self):
        c = sqlalchemy.column("id", self.eng.dialect.type_descriptor(sqlalchemy.types.Date))
        expr = sqlalchemy.func.extract('year', c)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM id)', str(compiled))


class TestSafeRound(BaseTest):
    def test_round(self):
        expr = sqlalchemy.func.round(123.45)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('round(CAST(%(round_1)s AS NUMERIC(38, 10)))', str(compiled))
        self.assertEqual(123.45, compiled.params['round_1'])

    def test_round_2dp(self):
        expr = sqlalchemy.func.round(123.45, 2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('round(CAST(%(round_2)s AS NUMERIC(38, 10)), %(round_1)s)', str(compiled))
        self.assertEqual(2, compiled.params['round_1'])
        self.assertEqual(123.45, compiled.params['round_2'])


class TestSafeRoundSR(StarrocksTest):
    def test_round_2dp(self):
        expr = sqlalchemy.func.round(123.45, 2)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('round(CAST(%(round_2)s AS DECIMAL(38, 10)), %(round_1)s)', str(compiled))
        self.assertEqual(2, compiled.params['round_1'])
        self.assertEqual(123.45, compiled.params['round_2'])

class TestSafeDivide(BaseTest):
    def test_safe_divide(self):
        expr = sqlalchemy.func.safe_divide(123.45, 987.65, 100)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('coalesce(CAST(%(safe_divide_1)s AS NUMERIC) / CAST(nullif(CAST(%(safe_divide_2)s AS NUMERIC), %(nullif_1)s) AS NUMERIC), %(safe_divide_3)s)', str(compiled))
        self.assertEqual(123.45, compiled.params['safe_divide_1'])
        self.assertEqual(987.65, compiled.params['safe_divide_2'])
        self.assertEqual(100, compiled.params['safe_divide_3'])
        self.assertEqual(0, compiled.params['nullif_1'])


class TestSafeDivideSR(StarrocksTest):
    def test_safe_divide(self):
        expr = sqlalchemy.func.safe_divide(123.45, 987.65, 100)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('coalesce(CAST(%(safe_divide_1)s AS DECIMAL(38, 10)) / nullif(CAST(%(safe_divide_2)s AS DECIMAL(38, 10)), %(nullif_1)s), %(safe_divide_3)s)', str(compiled))
        self.assertEqual(123.45, compiled.params['safe_divide_1'])
        self.assertEqual(987.65, compiled.params['safe_divide_2'])
        self.assertEqual(100, compiled.params['safe_divide_3'])
        self.assertEqual(0, compiled.params['nullif_1'])



# ---------------------------------------------------------------------------
# Postgres → Snowflake date-format token translation (sc-23158 WS-B3)
# ---------------------------------------------------------------------------

#: Postgres token → expected Snowflake format element, one row per supported
#: token in the translator's table.
_SNOWFLAKE_TOKEN_MATRIX = {
    'IYYY': 'UUUU',
    'YYYY': 'YYYY',
    'YY': 'YY',
    'Month': 'MMMM',
    'MONTH': 'MMMM',
    'Mon': 'MON',
    'MON': 'MON',
    'MM': 'MM',
    'DD': 'DD',
    'Dy': 'DY',
    'DY': 'DY',
    'HH24': 'HH24',
    'HH12': 'HH12',
    'HH': 'HH12',   # Postgres HH = 12-hour; Snowflake bare HH = HH24 — must translate
    'MI': 'MI',
    'SS': 'SS',
    'AM': 'AM',
    'PM': 'PM',
    'US': 'FF6',
    'tz': 'TZHTZM',
}

#: Postgres tokens Snowflake has no format element for — must raise, never
#: render (Snowflake would emit them as literal text: silently wrong output).
_SNOWFLAKE_UNSUPPORTED_TOKENS = ('Day', 'DAY', 'D', 'DDD', 'IW', 'TZ')


class TestPostgresToSnowflakeDateFormat(unittest.TestCase):

    def test_each_supported_token(self):
        for pg_token, expected in _SNOWFLAKE_TOKEN_MATRIX.items():
            with self.subTest(token=pg_token):
                self.assertEqual(expected, sf.postgres_to_snowflake_date_format(pg_token))

    def test_each_unsupported_token_raises(self):
        for pg_token in _SNOWFLAKE_UNSUPPORTED_TOKENS:
            with self.subTest(token=pg_token):
                with self.assertRaises(sqlalchemy.exc.CompileError):
                    sf.postgres_to_snowflake_date_format(pg_token)

    def test_every_documented_conversion_table_token_is_handled(self):
        # The WS-B3 contract: every ATOMIC token key of plaid-rpc's
        # Postgres↔Python conversion table is in the translator's vocabulary —
        # mapped or raise-listed. A token added to plaid-rpc later without a
        # _SNOWFLAKE_DATE_FORMAT_TOKENS entry would otherwise pass through as
        # literal text while this suite stayed green. Composite multi-token
        # keys ('YYYY-MM-DD"T"HH24:MI:SS', 'MM/DD/YYYY', 'DD Mon YYYY', …) are
        # excluded from the subset check — they contain separator characters
        # (so are never pure alphanumeric) and decompose into atomic tokens
        # the tokenizer handles per token; the loop below still exercises them
        # end to end.
        from plaidcloud.rpc.type_conversion import _PG_PY_FORMAT_MAPPING
        atomic_tokens = {key for key in _PG_PY_FORMAT_MAPPING if key.isalnum()}
        handled = set(sf._SNOWFLAKE_DATE_FORMAT_TOKENS)  # mapped ∪ raise-listed (None)
        self.assertEqual(set(), atomic_tokens - handled)
        for pg_format in _PG_PY_FORMAT_MAPPING:
            with self.subTest(token=pg_format):
                try:
                    sf.postgres_to_snowflake_date_format(pg_format)
                except sqlalchemy.exc.CompileError:
                    pass  # explicit unsupported — the contract allows this, never silence

    def test_composite_formats(self):
        cases = {
            'YYYY-MM-DD"T"HH24:MI:SS': 'YYYY-MM-DD"T"HH24:MI:SS',
            'YYYY-MM-DD"T"HH:MI:SS': 'YYYY-MM-DD"T"HH12:MI:SS',
            'YYYY-MM-DD': 'YYYY-MM-DD',
            'YYYY-MM-DD HH24:MI:SS': 'YYYY-MM-DD HH24:MI:SS',
            'HH24:MI:SS': 'HH24:MI:SS',
            'MM/DD/YYYY': 'MM/DD/YYYY',
            'DD Mon YYYY': 'DD MON YYYY',
            'DD MON YYYY': 'DD MON YYYY',
            'YYYYMMDD': 'YYYYMMDD',
            'YYYYMM': 'YYYYMM',
            'HH12:MI:SS AM': 'HH12:MI:SS AM',
            'YYYY-MM-DD HH24:MI:SS.US': 'YYYY-MM-DD HH24:MI:SS.FF6',
        }
        for pg_format, expected in cases.items():
            with self.subTest(fmt=pg_format):
                self.assertEqual(expected, sf.postgres_to_snowflake_date_format(pg_format))

    def test_iso_week_composite_raises(self):
        # 'IYYY-IW' (used with to_char today): ISO year maps to UUUU but there
        # is no ISO week element — the whole format must fail loudly.
        with self.assertRaises(sqlalchemy.exc.CompileError):
            sf.postgres_to_snowflake_date_format('IYYY-IW')

    def test_unknown_text_passes_through(self):
        # Non-token text is literal on both engines (unquoted T, separators).
        self.assertEqual('YYYY-MM-DDTHH24:MI:SS',
                         sf.postgres_to_snowflake_date_format('YYYY-MM-DDTHH24:MI:SS'))
        self.assertEqual('', sf.postgres_to_snowflake_date_format(''))

    def test_unterminated_quote_passes_through(self):
        self.assertEqual('YYYY"abc', sf.postgres_to_snowflake_date_format('YYYY"abc'))


# ---------------------------------------------------------------------------
# Alteryx-converter cross-dialect functions
# ---------------------------------------------------------------------------
# The Alteryx expression converter emits Databend names; StarRocks (MySQL-
# protocol) spells/arg-orders several differently. Each function preserves the
# Databend/default spelling and specializes only StarRocks. StarRocks behavior
# was verified live (paul-dev). Databend spelling is asserted unchanged.

class TestGeneratedFunctionPickling(unittest.TestCase):
    def test_generated_functions_are_pickleable(self):
        for name in {n for renames in sf._FUNCTION_RENAMES.values() for n in renames}:
            with self.subTest(name=name):
                self.assertTrue(hasattr(sf, name))
                self.assertEqual(sf.__name__, getattr(sf, name).__module__)
                expr = getattr(sqlalchemy.func, name)('2020-01-01')
                restored = pickle.loads(pickle.dumps(expr))
                self.assertIs(type(expr), type(restored))
                self.assertEqual(str(expr), str(restored))


class TestConverterRenamesDatabend(DatabendTest):
    """Databend keeps the emitted name verbatim (no regression)."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_modulo_preserved(self):
        self.assertEqual('modulo(5, 3)', self._sql(sqlalchemy.func.modulo(5, 3)))

    def test_ord_preserved(self):
        self.assertEqual("ord('A')", self._sql(sqlalchemy.func.ord('A')))

    def test_today_preserved(self):
        self.assertEqual('today()', self._sql(sqlalchemy.func.today()))

    def test_regexp_instr_preserved(self):
        self.assertEqual("regexp_instr('s', 'p')", self._sql(sqlalchemy.func.regexp_instr('s', 'p')))

    def test_to_year_preserved(self):
        self.assertEqual("to_year('2020-01-01')", self._sql(sqlalchemy.func.to_year('2020-01-01')))

    def test_add_days_preserved(self):
        self.assertEqual("add_days('2020-01-01', 5)", self._sql(sqlalchemy.func.add_days('2020-01-01', 5)))

    def test_to_string_preserved(self):
        self.assertEqual('to_string(123)', self._sql(sqlalchemy.func.to_string(123)))

    def test_try_to_float64_preserved(self):
        self.assertEqual("try_to_float64('1.5')", self._sql(sqlalchemy.func.try_to_float64('1.5')))

    def test_regexp_substr_preserved(self):
        self.assertEqual("regexp_substr('s', 'p')", self._sql(sqlalchemy.func.regexp_substr('s', 'p')))

    def test_date_diff_preserved(self):
        self.assertEqual("date_diff('day', 'd2', 'd1')",
                         self._sql(sqlalchemy.func.date_diff('day', 'd2', 'd1')))


class TestConverterRenamesStarrocks(StarrocksTest):
    """StarRocks gets the valid MySQL-protocol spelling/arg-order."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_modulo_becomes_mod(self):
        self.assertEqual('mod(5, 3)', self._sql(sqlalchemy.func.modulo(5, 3)))

    def test_ord_becomes_ascii(self):
        self.assertEqual("ascii('A')", self._sql(sqlalchemy.func.ord('A')))

    def test_today_becomes_current_date(self):
        self.assertEqual('current_date()', self._sql(sqlalchemy.func.today()))

    def test_regexp_instr_becomes_regexp(self):
        self.assertEqual("regexp('s', 'p')", self._sql(sqlalchemy.func.regexp_instr('s', 'p')))

    def test_datetime_extractors_become_mysql_names(self):
        self.assertEqual("year('2020-01-01')", self._sql(sqlalchemy.func.to_year('2020-01-01')))
        self.assertEqual("month('2020-01-01')", self._sql(sqlalchemy.func.to_month('2020-01-01')))
        self.assertEqual("day('2020-01-01')", self._sql(sqlalchemy.func.to_day_of_month('2020-01-01')))
        self.assertEqual("hour('2020-01-01')", self._sql(sqlalchemy.func.to_hour('2020-01-01')))
        self.assertEqual("minute('2020-01-01')", self._sql(sqlalchemy.func.to_minute('2020-01-01')))
        self.assertEqual("second('2020-01-01')", self._sql(sqlalchemy.func.to_second('2020-01-01')))

    def test_add_family_reverses_word_order(self):
        self.assertEqual("days_add('2020-01-01', 5)", self._sql(sqlalchemy.func.add_days('2020-01-01', 5)))
        self.assertEqual("months_add('2020-01-01', 2)", self._sql(sqlalchemy.func.add_months('2020-01-01', 2)))
        self.assertEqual("years_add('2020-01-01', 1)", self._sql(sqlalchemy.func.add_years('2020-01-01', 1)))

    def test_to_string_casts_to_char(self):
        self.assertEqual('CAST(123 AS CHAR)', self._sql(sqlalchemy.func.to_string(123)))

    def test_to_string_with_decimals_rounds_under_literal_binds(self):
        # ToString(number, decimals): must render a real literal (no %(…)s bind
        # placeholder) so it is valid inside view DDL (compiled with literal_binds).
        expr = sqlalchemy.func.to_string(sqlalchemy.column('c'), 2)
        sql = str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))
        self.assertEqual('CAST(round(c, 2) AS CHAR)', sql)

    def test_try_to_float64_casts_to_double(self):
        self.assertEqual("CAST('1.5' AS DOUBLE)", self._sql(sqlalchemy.func.try_to_float64('1.5')))

    def test_regexp_substr_becomes_regexp_extract(self):
        self.assertEqual("regexp_extract('s', 'p', 0)", self._sql(sqlalchemy.func.regexp_substr('s', 'p')))

    def test_date_diff_becomes_unit_diff_with_swapped_args(self):
        # date_diff('day', dt2, dt1) (= dt1 - dt2 on Databend) → days_diff(dt1, dt2).
        self.assertEqual("days_diff('d1', 'd2')",
                         self._sql(sqlalchemy.func.date_diff('day', 'd2', 'd1')))
        self.assertEqual("months_diff('d1', 'd2')",
                         self._sql(sqlalchemy.func.date_diff('month', 'd2', 'd1')))


class TestConverterRenamesSnowflake(SnowflakeTest):
    """Snowflake gets the documented spelling; arg-reorders go through DATEADD."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_modulo_becomes_mod(self):
        self.assertEqual('mod(5, 3)', self._sql(sqlalchemy.func.modulo(5, 3)))

    def test_ord_becomes_ascii(self):
        self.assertEqual("ascii('A')", self._sql(sqlalchemy.func.ord('A')))

    def test_today_becomes_current_date(self):
        self.assertEqual('current_date()', self._sql(sqlalchemy.func.today()))

    def test_regexp_instr_is_native(self):
        # Snowflake ships regexp_instr (1-based position, 0 on no match) — no rename.
        self.assertEqual("regexp_instr('s', 'p')", self._sql(sqlalchemy.func.regexp_instr('s', 'p')))

    def test_datetime_extractors_become_snowflake_names(self):
        self.assertEqual("year('2020-01-01')", self._sql(sqlalchemy.func.to_year('2020-01-01')))
        self.assertEqual("month('2020-01-01')", self._sql(sqlalchemy.func.to_month('2020-01-01')))
        self.assertEqual("day('2020-01-01')", self._sql(sqlalchemy.func.to_day_of_month('2020-01-01')))
        self.assertEqual("hour('2020-01-01')", self._sql(sqlalchemy.func.to_hour('2020-01-01')))
        self.assertEqual("minute('2020-01-01')", self._sql(sqlalchemy.func.to_minute('2020-01-01')))
        self.assertEqual("second('2020-01-01')", self._sql(sqlalchemy.func.to_second('2020-01-01')))

    def test_add_family_becomes_dateadd(self):
        self.assertEqual("dateadd(day, 5, '2020-01-01')", self._sql(sqlalchemy.func.add_days('2020-01-01', 5)))
        self.assertEqual("dateadd(month, 2, '2020-01-01')", self._sql(sqlalchemy.func.add_months('2020-01-01', 2)))
        self.assertEqual("dateadd(year, 1, '2020-01-01')", self._sql(sqlalchemy.func.add_years('2020-01-01', 1)))
        self.assertEqual("dateadd(hour, 3, '2020-01-01')", self._sql(sqlalchemy.func.add_hours('2020-01-01', 3)))
        self.assertEqual("dateadd(minute, 4, '2020-01-01')", self._sql(sqlalchemy.func.add_minutes('2020-01-01', 4)))
        self.assertEqual("dateadd(second, 6, '2020-01-01')", self._sql(sqlalchemy.func.add_seconds('2020-01-01', 6)))


# ---------------------------------------------------------------------------
# StarRocks specializations for the remaining non-spatial converter functions.
# Each keeps the Databend/default form and adds only a StarRocks form (verified
# live on paul-dev). Databend byte-identity is asserted alongside.
# ---------------------------------------------------------------------------

class TestToCharStarrocks(StarrocksTest):
    def test_to_char_date(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('date_format(%(to_char_1)s, %(param_1)s)', str(compiled))
        self.assertEqual(dt, compiled.params['to_char_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])

    def test_to_char_number_casts_to_char(self):
        # StarRocks cannot honor a Postgres numeric mask; value is cast to CHAR.
        expr = sqlalchemy.func.to_char(123456.789, 'LFM999,999,999,999D00')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(to_char_1)s AS CHAR)', str(compiled))
        self.assertEqual(123456.789, compiled.params['to_char_1'])

    def test_to_char_no_format_casts_to_char(self):
        expr = sqlalchemy.func.to_char(sqlalchemy.column('c'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(c AS CHAR)', str(compiled))


class TestToNumberStarrocks(StarrocksTest):
    def test_to_number_casts_to_decimal(self):
        expr = sqlalchemy.func.to_number('12345', '999999')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(to_number_1)s AS DECIMAL(38, 10))', str(compiled))
        self.assertEqual('12345', compiled.params['to_number_1'])


class TestToNumberDatabendUnchanged(DatabendTest):
    def test_to_number_still_to_int64(self):
        expr = sqlalchemy.func.to_number('12345', '999999')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_int64(%(to_number_1)s)', str(compiled))


class TestTransactionTimestampStarrocks(StarrocksTest):
    def test_transaction_timestamp_is_now(self):
        expr = sqlalchemy.func.transaction_timestamp()
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('now()', str(compiled))


class TestStringToArrayStarrocks(StarrocksTest):
    def test_string_to_array_split(self):
        expr = sqlalchemy.func.string_to_array('1,2,3,4', ',')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'split(%(string_to_array_1)s, CASE WHEN (%(string_to_array_2)s = %(param_1)s OR %(string_to_array_2)s IS NULL) THEN %(param_2)s ELSE %(string_to_array_2)s END)',
            str(compiled)
        )
        self.assertEqual('1,2,3,4', compiled.params['string_to_array_1'])
        self.assertEqual(',', compiled.params['string_to_array_2'])


class TestOnlyAsciiStarrocks(StarrocksTest):
    def test_only_ascii_three_arg(self):
        # StarRocks rejects the 4-arg regexp_replace(varchar x4) the default emits.
        expr = sqlalchemy.func.ascii('abc')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'regexp_replace(CAST(%(ascii_1)s AS STRING), %(regexp_replace_1)s, %(regexp_replace_2)s)',
            str(compiled),
        )
        self.assertEqual('abc', compiled.params['ascii_1'])
        self.assertEqual(r'[^[:ascii:]]+', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])


class TestNormalizeWhitespaceStarrocks(StarrocksTest):
    def test_normalize_whitespace_three_arg_x_escapes(self):
        # RE2 rejects `\uXXXX`; the StarRocks regex spells code points `\x{XXXX}`.
        expr = sqlalchemy.func.normalize_whitespace('foobar')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'regexp_replace(CAST(%(normalize_whitespace_1)s AS STRING), %(regexp_replace_1)s, %(regexp_replace_2)s)',
            str(compiled),
        )
        self.assertEqual('foobar', compiled.params['normalize_whitespace_1'])
        self.assertEqual(sf.STARROCKS_WW_RE, compiled.params['regexp_replace_1'])
        self.assertEqual('[\\n\\r\\f\\x{000B}\\x{0085}\\x{2028}\\x{2029}\\x{00A0}]+', compiled.params['regexp_replace_1'])
        self.assertEqual(' ', compiled.params['regexp_replace_2'])


# ---------------------------------------------------------------------------
# Dialect-neutral spatial (geometry) functions
# ---------------------------------------------------------------------------
# Databend renders the current Databend ST_* spelling byte-for-byte; StarRocks
# renders the verified equivalent, or raises CompileError for ops with no
# StarRocks equivalent (so wave-2b degrades them to the shapely executor).

#: neutral name -> Databend spelling (byte-identical) / StarRocks spelling.
_GEOM_TRANSLATABLE = [
    ('geom_from_wkt', "st_geometryfromwkt('POINT(1 2)')", "st_geometryfromtext('POINT(1 2)')",
     lambda: sqlalchemy.func.geom_from_wkt('POINT(1 2)')),
    ('geom_point', 'st_makegeompoint(1, 2)', 'st_point(1, 2)',
     lambda: sqlalchemy.func.geom_point(1, 2)),
    ('geom_as_wkt', 'st_aswkt(g)', 'st_astext(g)',
     lambda: sqlalchemy.func.geom_as_wkt(sqlalchemy.column('g'))),
    ('geom_contains', 'st_contains(a, b)', 'st_contains(a, b)',
     lambda: sqlalchemy.func.geom_contains(sqlalchemy.column('a'), sqlalchemy.column('b'))),
    ('geom_x', 'st_x(p)', 'st_x(p)',
     lambda: sqlalchemy.func.geom_x(sqlalchemy.column('p'))),
    ('geom_y', 'st_y(p)', 'st_y(p)',
     lambda: sqlalchemy.func.geom_y(sqlalchemy.column('p'))),
    # within(a, b): StarRocks has no st_within → st_contains with swapped args.
    ('geom_within', 'st_within(a, b)', 'st_contains(b, a)',
     lambda: sqlalchemy.func.geom_within(sqlalchemy.column('a'), sqlalchemy.column('b'))),
]

#: neutral name -> Databend spelling; StarRocks raises (no equivalent).
_GEOM_UNSUPPORTED = [
    ('geom_area', 'st_area(g)', lambda: sqlalchemy.func.geom_area(sqlalchemy.column('g'))),
    ('geom_length', 'st_length(g)', lambda: sqlalchemy.func.geom_length(sqlalchemy.column('g'))),
    ('geom_intersects', 'st_intersects(a, b)',
     lambda: sqlalchemy.func.geom_intersects(sqlalchemy.column('a'), sqlalchemy.column('b'))),
    ('geom_createline', 'st_createline(a, b)',
     lambda: sqlalchemy.func.geom_createline(sqlalchemy.column('a'), sqlalchemy.column('b'))),
    ('geom_centroid', 'st_centroid(g)', lambda: sqlalchemy.func.geom_centroid(sqlalchemy.column('g'))),
    ('geom_distance', 'st_distance(a, b)',
     lambda: sqlalchemy.func.geom_distance(sqlalchemy.column('a'), sqlalchemy.column('b'))),
]


class TestGeomFunctionsDatabend(DatabendTest):
    """Databend keeps the current ST_* spelling byte-for-byte (no regression)."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_translatable_render_databend_names(self):
        for name, databend_sql, _sr, make in _GEOM_TRANSLATABLE:
            with self.subTest(fn=name):
                self.assertEqual(databend_sql, self._sql(make()))

    def test_unsupported_still_render_on_databend(self):
        for name, databend_sql, make in _GEOM_UNSUPPORTED:
            with self.subTest(fn=name):
                self.assertEqual(databend_sql, self._sql(make()))


class TestGeomFunctionsStarrocks(StarrocksTest):
    """StarRocks renders the verified equivalent, or raises for unsupported ops."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_translatable_render_starrocks_names(self):
        for name, _db, starrocks_sql, make in _GEOM_TRANSLATABLE:
            with self.subTest(fn=name):
                self.assertEqual(starrocks_sql, self._sql(make()))

    def test_unsupported_raise_compile_error(self):
        for name, _db, make in _GEOM_UNSUPPORTED:
            with self.subTest(fn=name):
                with self.assertRaises(sqlalchemy.exc.CompileError):
                    self._sql(make())


class TestUuidCast(BaseTest):
    """sc-23158 WS-A5: the expression namespace's uuid cast keys resolve to
    GUIDHyphens — the same type sqlalchemy_from_dtype('uuid') stores columns
    with — so an inline cast compiles real SQL per engine and binds the
    36-char hyphenated form (sqlalchemy.Uuid bound 32-char unhyphenated hex
    on non-native dialects, and StarRocks silently dropped the cast)."""

    expected_cast = 'CAST(anlz_schema.table_abc123.id AS CHAR(36))'

    def setUp(self) -> None:
        super().setUp()
        self.table = sql_expression.get_table_rep(
            'table_abc123', [{'source': 'id', 'dtype': 'text'}], 'anlz_schema')

    def test_uuid_cast_compiles(self):
        expr = sql_expression.eval_expression('sqlalchemy.cast(table.id, uuid)', {}, [self.table])
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(self.expected_cast, str(compiled))

    def test_all_case_variants_map_to_guidhyphens(self):
        safe_dict = sql_expression.get_safe_dict([])
        for key in ('uuid', 'Uuid', 'UUID'):
            self.assertIs(GUIDHyphens, safe_dict[key])

    def test_bind_is_hyphenated_on_non_native_dialect(self):
        bound = GUIDHyphens().process_bind_param(
            uuid_mod.UUID('12345678-1234-5678-1234-567812345678'), self.eng.dialect)
        self.assertEqual('12345678-1234-5678-1234-567812345678', bound)


class TestUuidCastDatabend(TestUuidCast, DatabendTest):
    pass


class TestUuidCastStarrocks(TestUuidCast, StarrocksTest):
    pass


# ---------------------------------------------------------------------------
# sc-23158 WS-B2: Snowflake expression variants
# ---------------------------------------------------------------------------
# Exact compiled-SQL assertions per function on snowflake, verified against
# the Snowflake function reference, plus regression assertions pinning the
# databend/starrocks/default renderings of every touched class (current
# operations must stay byte-identical).


class TestElapsedSeconds(BaseTest):
    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_elapsed_seconds(self):
        self.assertEqual(
            'EXTRACT(EPOCH FROM COALESCE(CAST(b AS TIMESTAMP WITHOUT TIME ZONE), NOW())'
            '-CAST(a AS TIMESTAMP WITHOUT TIME ZONE))',
            self._sql(sf.elapsed_seconds(sqlalchemy.column('a'), sqlalchemy.column('b'))))


class TestElapsedSecondsDatabend(DatabendTest):
    def test_elapsed_seconds(self):
        expr = sf.elapsed_seconds(sqlalchemy.column('a'), sqlalchemy.column('b'))
        self.assertEqual(
            '(CAST(COALESCE(CAST(b AS DATETIME), NOW()) AS INT64 - CAST(CAST(a AS DATETIME) AS INT64)) / 1000000',
            str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})))


class TestElapsedSecondsStarrocks(StarrocksTest):
    def test_elapsed_seconds(self):
        expr = sf.elapsed_seconds(sqlalchemy.column('a'), sqlalchemy.column('b'))
        self.assertEqual(
            'seconds_diff(CAST(a AS DATETIME), COALESCE(CAST(b AS DATETIME), NOW()))',
            str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})))


class TestElapsedSecondsSnowflake(SnowflakeTest):
    def test_elapsed_seconds(self):
        # No timestamp subtraction and no NOW() on Snowflake; DATEDIFF(second,
        # start, end) = end - start.
        expr = sf.elapsed_seconds(sqlalchemy.column('a'), sqlalchemy.column('b'))
        self.assertEqual(
            'datediff(second, CAST(a AS datetime), COALESCE(CAST(b AS datetime), CURRENT_TIMESTAMP))',
            str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})))


class TestImportColSnowflake(SnowflakeTest):
    def test_import_col_text(self):
        expr = sqlalchemy.func.import_col('Column1', 'text', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('%(import_col_1)s', str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])

    def test_import_col_numeric(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE '
                          'CAST(CASE WHEN (to_varchar(regexp_replace(%(import_col_1)s, '
                          '%(regexp_replace_4)s, %(regexp_replace_5)s)) = %(to_string_1)s) THEN NULL '
                          'ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                          '%(regexp_replace_5)s) END AS NUMERIC(38, 10)) END'), str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual(0.0, compiled.params['param_1'])
        self.assertEqual('NaN', compiled.params['to_string_1'])

    def test_import_cast_numeric_trailing_negatives(self):
        expr = sqlalchemy.func.import_cast('Column1', 'numeric', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CAST(CASE WHEN (to_varchar(CASE WHEN regexp_like(regexp_replace(%(import_cast_1)s, '
                          '%(regexp_replace_1)s, %(regexp_replace_2)s), %(regexp_like_1)s) THEN '
                          'concat(%(concat_1)s, replace(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(replace_1)s, %(replace_2)s)) ELSE '
                          'regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) END) '
                          '= %(to_string_1)s) THEN NULL ELSE CASE WHEN '
                          'regexp_like(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(regexp_like_1)s) THEN concat(%(concat_1)s, '
                          'replace(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s), %(replace_1)s, %(replace_2)s)) ELSE '
                          'regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) END '
                          'END AS NUMERIC(38, 10))'), str(compiled))
        self.assertEqual('^[0-9]*\\.?[0-9]*-$', compiled.params['regexp_like_1'])
        self.assertEqual('-', compiled.params['concat_1'])

    def test_import_cast_currency(self):
        expr = sqlalchemy.func.import_cast('Column1', 'currency', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('CAST(CASE WHEN (to_varchar(regexp_replace(%(import_cast_1)s, '
                          '%(regexp_replace_1)s, %(regexp_replace_2)s)) = %(to_string_1)s) THEN NULL '
                          'ELSE regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, '
                          '%(regexp_replace_2)s) END AS NUMERIC(18, 4))'), str(compiled))

    def test_import_cast_integer(self):
        expr = sqlalchemy.func.import_cast('Column1', 'integer', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'CAST(regexp_replace(%(import_cast_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) AS INTEGER)',
            str(compiled))

    def test_import_cast_boolean(self):
        # TO_BOOLEAN natively accepts true/t/yes/y/on/1 // false/f/no/n/off/0,
        # case-insensitive — no CASE mapping needed.
        expr = sqlalchemy.func.import_cast('Column1', 'boolean', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_boolean(CAST(%(import_cast_1)s AS VARCHAR))', str(compiled))

    def test_import_cast_date(self):
        expr = sqlalchemy.func.import_cast('Column1', 'date', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'to_date(nullif(trim(CAST(CAST(%(import_cast_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))',
            str(compiled))
        self.assertEqual('YYYY-MM-DD', compiled.params['param_1'])

    def test_import_cast_time(self):
        expr = sqlalchemy.func.import_cast('Column1', 'time', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_timestamp(CAST(%(import_cast_1)s AS TEXT), CAST(%(param_1)s AS TEXT))', str(compiled))
        self.assertEqual('HH24:MI:SS', compiled.params['param_1'])

    def test_import_cast_interval_raises(self):
        # Snowflake has interval constants but no INTERVAL data type.
        expr = sqlalchemy.func.import_cast('Column1', 'interval', 'YYYY-MM-DD', False)
        with self.assertRaises(sqlalchemy.exc.CompileError):
            expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})


class TestSafeToDateSnowflake(SnowflakeTest):
    def test_to_date(self):
        expr = sqlalchemy.func.to_date('2019-01-05')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])

    def test_to_date_specifier(self):
        expr = sqlalchemy.func.to_date('2019-01-05', 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))',
            str(compiled))
        self.assertEqual('YYYY-MM-DD', compiled.params['param_1'])

    def test_to_date_specifier_python(self):
        expr = sqlalchemy.func.to_date('2019-01-05', '%Y-%m-%d')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS TEXT) AS TEXT)), %(nullif_1)s), CAST(%(param_1)s AS TEXT))',
            str(compiled))
        self.assertEqual('YYYY-MM-DD', compiled.params['param_1'])

    def test_to_date_unsupported_token_raises(self):
        expr = sqlalchemy.func.to_date('Monday 2019-01-05', 'Day YYYY-MM-DD')
        with self.assertRaises(sqlalchemy.exc.CompileError):
            expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})


class TestSafeToTimestampSnowflake(SnowflakeTest):
    def test_to_timestamp_single_arg(self):
        expr = sqlalchemy.func.to_timestamp(sqlalchemy.column('t'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_timestamp(CAST(t AS TEXT), CAST(%(param_1)s AS TEXT))', str(compiled))
        self.assertEqual('YYYY-MM-DD HH24:MI:SS', compiled.params['param_1'])

    def test_to_timestamp_translates_bare_hh(self):
        # Postgres HH means HH12; bare HH on Snowflake would mean HH24.
        expr = sqlalchemy.func.to_timestamp(sqlalchemy.column('t'), 'YYYY-MM-DD HH:MI:SS')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_timestamp(CAST(t AS TEXT), CAST(%(param_1)s AS TEXT))', str(compiled))
        self.assertEqual('YYYY-MM-DD HH12:MI:SS', compiled.params['param_1'])

    def test_to_timestamp_python_format(self):
        expr = sqlalchemy.func.to_timestamp(sqlalchemy.column('t'), '%Y-%m-%dT%H:%M:%S')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('YYYY-MM-DD"T"HH24:MI:SS', compiled.params['param_1'])


class TestDateAddSnowflake(SnowflakeTest):
    def test_date_add(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt, years=1, months=2, weeks=3, days=4, hours=5, minutes=6, seconds=7)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'dateadd(second, %(dateadd_1)s, dateadd(minute, %(dateadd_2)s, dateadd(hour, %(dateadd_3)s, '
            'dateadd(day, %(dateadd_4)s, dateadd(week, %(dateadd_5)s, dateadd(month, %(dateadd_6)s, '
            'dateadd(year, %(dateadd_7)s, CAST(%(date_add_1)s AS datetime))))))))',
            str(compiled),
        )
        self.assertEqual(dt, compiled.params['date_add_1'])
        self.assertEqual(7, compiled.params['dateadd_1'])
        self.assertEqual(6, compiled.params['dateadd_2'])
        self.assertEqual(5, compiled.params['dateadd_3'])
        self.assertEqual(4, compiled.params['dateadd_4'])
        self.assertEqual(3, compiled.params['dateadd_5'])
        self.assertEqual(2, compiled.params['dateadd_6'])
        self.assertEqual(1, compiled.params['dateadd_7'])

    def test_date_add_weeks_and_days(self):
        expr = sqlalchemy.func.date_add(sqlalchemy.func.date_trunc('WEEK', sqlalchemy.func.now()), weeks=6, days=7)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})
        self.assertEqual(
            "dateadd(day, 7, dateadd(week, 6, CAST(date_trunc('WEEK', CURRENT_TIMESTAMP) AS datetime)))",
            str(compiled),
        )

    def test_date_add_no_params(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.date_add(dt)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CAST(%(date_add_1)s AS datetime)', str(compiled))


class TestDateDiffSnowflake(SnowflakeTest):
    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_date_diff_same_direction_no_swap(self):
        # Snowflake DATEDIFF(part, a, b) = b - a, the same direction as
        # Databend's date_diff — arguments pass through unswapped.
        self.assertEqual("datediff(day, 'd2', 'd1')",
                         self._sql(sqlalchemy.func.date_diff('day', 'd2', 'd1')))
        self.assertEqual("datediff(month, 'd2', 'd1')",
                         self._sql(sqlalchemy.func.date_diff('month', 'd2', 'd1')))
        self.assertEqual("datediff(week, 'd2', 'd1')",
                         self._sql(sqlalchemy.func.date_diff('week', 'd2', 'd1')))
        self.assertEqual("datediff(quarter, 'd2', 'd1')",
                         self._sql(sqlalchemy.func.date_diff('quarter', 'd2', 'd1')))

    def test_date_diff_unknown_unit_falls_through(self):
        # Renders the nonexistent date_diff → loud server error, never a
        # silently mis-scaled count (same fallback contract as StarRocks).
        expr = sqlalchemy.func.date_diff('millennium', 'd2', 'd1')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('date_diff(%(date_diff_1)s, %(date_diff_2)s, %(date_diff_3)s)', str(compiled))


class TestNumericize(BaseTest):
    def test_numericize(self):
        expr = sqlalchemy.func.numericize(sqlalchemy.column('t'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'coalesce(SUBSTRING(trim(CAST(CAST(t AS TEXT) AS TEXT)) FROM %(substring_1)s), '
            'SUBSTRING(trim(CAST(CAST(t AS TEXT) AS TEXT)) FROM %(substring_2)s), '
            'nullif(regexp_replace(trim(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_replace_1)s, '
            '%(regexp_replace_2)s, %(regexp_replace_3)s), %(nullif_1)s))',
            str(compiled))


class TestNumericizeDatabend(DatabendTest):
    def test_numericize(self):
        expr = sqlalchemy.func.numericize(sqlalchemy.column('t'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'coalesce(regexp_substr(TRIM(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_substr_1)s), '
            'regexp_substr(TRIM(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_substr_2)s), '
            'nullif(regexp_replace(TRIM(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_replace_1)s, '
            '%(regexp_replace_2)s, %(regexp_replace_3)s, %(regexp_replace_4)s), %(nullif_1)s))',
            str(compiled))


class TestNumericizeStarrocks(StarrocksTest):
    def test_numericize(self):
        expr = sqlalchemy.func.numericize(sqlalchemy.column('t'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'coalesce(nullif(regexp_extract(trim(CAST(CAST(t AS STRING) AS STRING)), %(regexp_extract_1)s, '
            '%(regexp_extract_2)s), %(nullif_1)s), nullif(regexp_extract(trim(CAST(CAST(t AS STRING) AS STRING)), '
            '%(regexp_extract_3)s, %(regexp_extract_4)s), %(nullif_2)s), '
            'nullif(regexp_replace(trim(CAST(CAST(t AS STRING) AS STRING)), %(regexp_replace_1)s, '
            '%(regexp_replace_2)s), %(nullif_3)s))',
            str(compiled))


class TestNumericizeSnowflake(SnowflakeTest):
    def test_numericize(self):
        # REGEXP_SUBSTR(subject, pattern) = first whole match / NULL; 3-arg
        # REGEXP_REPLACE replaces all occurrences (occurrence defaults to 0).
        expr = sqlalchemy.func.numericize(sqlalchemy.column('t'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'coalesce(regexp_substr(trim(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_substr_1)s), '
            'regexp_substr(trim(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_substr_2)s), '
            'nullif(regexp_replace(trim(CAST(CAST(t AS TEXT) AS TEXT)), %(regexp_replace_1)s, '
            '%(regexp_replace_2)s), %(nullif_1)s))',
            str(compiled))
        self.assertEqual(r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))', compiled.params['regexp_substr_1'])
        self.assertEqual(r'(^[+\-][0-9\.]+)', compiled.params['regexp_substr_2'])
        self.assertEqual(r'[^0-9\.]+', compiled.params['regexp_replace_1'])


class TestOnlyAsciiSnowflake(SnowflakeTest):
    def test_only_ascii_literal_range_class(self):
        # [[:ascii:]] is a PCRE extension Snowflake's documented POSIX-ERE
        # engine doesn't list, and \x{…} escapes don't exist — the class is
        # spelled with literal characters.
        expr = sqlalchemy.func.ascii('abc')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'regexp_replace(CAST(%(ascii_1)s AS TEXT), %(regexp_replace_1)s, %(regexp_replace_2)s)',
            str(compiled))
        self.assertEqual('[^\x01-\x7f]+', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])


class TestNormalizeWhitespaceSnowflake(SnowflakeTest):
    def test_normalize_whitespace_literal_char_class(self):
        expr = sqlalchemy.func.normalize_whitespace('foobar')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'regexp_replace(CAST(%(normalize_whitespace_1)s AS TEXT), %(regexp_replace_1)s, %(regexp_replace_2)s)',
            str(compiled))
        self.assertEqual(sf.SNOWFLAKE_WW_RE, compiled.params['regexp_replace_1'])
        self.assertEqual('[\n\r\f\x0b\x85\u2028\u2029\xa0]+', compiled.params['regexp_replace_1'])
        self.assertEqual(' ', compiled.params['regexp_replace_2'])


class TestTrimFamilySnowflake(SnowflakeTest):
    """Defaults confirmed valid: LTRIM/RTRIM/TRIM(<expr> [, <characters>])."""

    def test_ltrim_plain(self):
        expr = sqlalchemy.func.ltrim('12345', '')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('ltrim(CAST(%(ltrim_1)s AS TEXT))', str(compiled))

    def test_ltrim_specific(self):
        expr = sqlalchemy.func.ltrim('12345', '1')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('ltrim(CAST(%(ltrim_2)s AS TEXT), %(ltrim_1)s)', str(compiled))

    def test_rtrim_specific(self):
        expr = sqlalchemy.func.rtrim('12345', '5')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('rtrim(CAST(%(rtrim_2)s AS TEXT), %(rtrim_1)s)', str(compiled))

    def test_trim_specific(self):
        expr = sqlalchemy.func.trim('12345', '5')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('trim(CAST(%(trim_2)s AS TEXT), %(trim_1)s)', str(compiled))


class TestStrposSnowflake(SnowflakeTest):
    def test_strpos_becomes_charindex(self):
        expr = sqlalchemy.func.strpos('haystack', 'needle')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('charindex(%(strpos_1)s, %(strpos_2)s)', str(compiled))
        self.assertEqual('needle', compiled.params['strpos_1'])
        self.assertEqual('haystack', compiled.params['strpos_2'])


class TestStringToArraySnowflake(SnowflakeTest):
    def test_string_to_array_split(self):
        expr = sqlalchemy.func.string_to_array('1,2,3,4', ',')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'split(%(string_to_array_1)s, CASE WHEN (%(string_to_array_2)s = %(param_1)s OR '
            '%(string_to_array_2)s IS NULL) THEN %(param_2)s ELSE %(string_to_array_2)s END)',
            str(compiled))


class TestSafeDivideSnowflake(SnowflakeTest):
    def test_safe_divide(self):
        # Hand-rendered division: SQLAlchemy's truediv rendering under
        # snowflake-sqlalchemy's div_is_floordiv default would wrap the divisor
        # in CAST(... AS NUMERIC) — NUMBER(38, 0) — rounding it to an integer.
        expr = sqlalchemy.func.safe_divide(123.45, 987.65, 100)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(
            'coalesce(CAST(%(safe_divide_1)s AS NUMERIC(38, 10)) / '
            'nullif(CAST(%(safe_divide_2)s AS NUMERIC(38, 10)), %(nullif_1)s), %(safe_divide_3)s)',
            str(compiled))
        self.assertEqual(123.45, compiled.params['safe_divide_1'])
        self.assertEqual(987.65, compiled.params['safe_divide_2'])
        self.assertEqual(100, compiled.params['safe_divide_3'])
        self.assertEqual(0, compiled.params['nullif_1'])


class TestIntegerizeTruncate(BaseTest):
    def test_integerize_truncate(self):
        expr = sqlalchemy.func.integerize_truncate(sqlalchemy.column('a'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertTrue(str(compiled).startswith('CAST(trunc(CAST(nullif('))
        self.assertTrue(str(compiled).endswith('AS NUMERIC)) AS INTEGER)'))


class TestIntegerizeTruncateDatabend(DatabendTest):
    def test_integerize_truncate(self):
        expr = sqlalchemy.func.integerize_truncate(sqlalchemy.column('a'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertTrue(str(compiled).startswith('CAST(truncate(CAST(nullif('))
        self.assertTrue(str(compiled).endswith('AS DECIMAL(38, 10))) AS INTEGER)'))


class TestIntegerizeTruncateSnowflake(SnowflakeTest):
    def test_integerize_truncate_keeps_decimals_for_trunc(self):
        # Bare NUMERIC is NUMBER(38, 0) on Snowflake and casts round half away
        # from zero — the squash cast must keep scale so trunc sees decimals.
        expr = sqlalchemy.func.integerize_truncate(sqlalchemy.column('a'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertTrue(str(compiled).startswith('CAST(trunc(CAST(nullif('))
        self.assertTrue(str(compiled).endswith('AS NUMERIC(38, 10))) AS INTEGER)'))


class TestToNumberSnowflake(SnowflakeTest):
    def test_to_number_pins_precision_and_scale(self):
        # Without explicit precision/scale Snowflake TO_NUMBER returns
        # NUMBER(38, 0), rounding every fractional digit away.
        expr = sqlalchemy.func.to_number('12345', '999999')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_number(%(to_number_1)s, %(to_number_2)s, 38, 10)', str(compiled))
        self.assertEqual('12345', compiled.params['to_number_1'])
        self.assertEqual('999999', compiled.params['to_number_2'])


class TestToCharSnowflake(SnowflakeTest):
    def test_to_char_number(self):
        # Snowflake's numeric format models cover 9/0/,/./D/G/$/S/MI/B/X/TM and
        # the FM modifier; only the locale currency element L translates (→ $).
        expr = sqlalchemy.func.to_char(123456.789, 'LFM999,999,999,999D00')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_varchar(%(to_char_1)s, %(to_varchar_1)s)', str(compiled))
        self.assertEqual(123456.789, compiled.params['to_char_1'])
        self.assertEqual('$FM999,999,999,999D00', compiled.params['to_varchar_1'])

    def test_to_char_date(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_varchar(%(to_char_1)s, %(to_varchar_1)s)', str(compiled))
        self.assertEqual('YYYY-MM-DD', compiled.params['to_varchar_1'])

    def test_to_char_time(self):
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'HH24:MI:SS')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('HH24:MI:SS', compiled.params['to_varchar_1'])

    def test_to_char_iso_week_raises(self):
        # 'IYYY-IW' has no Snowflake ISO-week element — loud failure, not
        # literal-text output.
        dt = datetime.datetime(2023, 11, 20, 9, 30, 0, 0)
        expr = sqlalchemy.func.to_char(dt, 'IYYY-IW')
        with self.assertRaises(sqlalchemy.exc.CompileError):
            expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})

    def test_to_char_no_format(self):
        expr = sqlalchemy.func.to_char(sqlalchemy.column('c'))
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('to_varchar(c)', str(compiled))


class TestToStringSnowflake(SnowflakeTest):
    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_to_string_becomes_to_varchar(self):
        self.assertEqual('to_varchar(123)', self._sql(sqlalchemy.func.to_string(123)))

    def test_to_string_with_decimals_rounds_under_literal_binds(self):
        self.assertEqual('to_varchar(round(c, 2))',
                         self._sql(sqlalchemy.func.to_string(sqlalchemy.column('c'), 2)))


class TestTryToFloat64Snowflake(SnowflakeTest):
    def test_try_to_float64_becomes_try_to_double(self):
        expr = sqlalchemy.func.try_to_float64('1.5')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True})
        self.assertEqual("try_to_double('1.5')", str(compiled))


class TestTransactionTimestampSnowflake(SnowflakeTest):
    def test_transaction_timestamp_is_current_timestamp(self):
        expr = sqlalchemy.func.transaction_timestamp()
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('CURRENT_TIMESTAMP', str(compiled))


class TestQuantiles(BaseTest):
    """Default (ClickHouse-style parameterized-aggregate) renderings pinned."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_default_renderings(self):
        x = sqlalchemy.column('x')
        self.assertEqual('QUANTILE_TDIGEST(0.5)(x)', self._sql(sqlalchemy.func.quantile_tdigest(0.5, x)))
        self.assertEqual('QUANTILE_CONT(0.5)(x)', self._sql(sqlalchemy.func.quantile_cont(0.5, x)))
        self.assertEqual('QUANTILE_DISC(0.5)(x)', self._sql(sqlalchemy.func.quantile_disc(0.5, x)))
        self.assertEqual('QUANTILE_TDIGEST_WEIGHTED(0.5)(x, w)',
                         self._sql(sqlalchemy.func.quantile_tdigest_weighted(0.5, x, sqlalchemy.column('w'))))


class TestQuantilesDatabend(TestQuantiles, DatabendTest):
    pass


class TestQuantilesSnowflake(SnowflakeTest):
    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_quantile_tdigest_becomes_approx_percentile(self):
        # Snowflake's APPROX_PERCENTILE is itself t-digest-based; args reverse.
        self.assertEqual('APPROX_PERCENTILE(x, 0.5)',
                         self._sql(sqlalchemy.func.quantile_tdigest(0.5, sqlalchemy.column('x'))))

    def test_quantile_cont_within_group(self):
        self.assertEqual('PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY x)',
                         self._sql(sqlalchemy.func.quantile_cont(0.5, sqlalchemy.column('x'))))

    def test_quantile_disc_within_group(self):
        self.assertEqual('PERCENTILE_DISC(0.5) WITHIN GROUP (ORDER BY x)',
                         self._sql(sqlalchemy.func.quantile_disc(0.5, sqlalchemy.column('x'))))

    def test_quantile_tdigest_weighted_raises(self):
        # No weighted percentile aggregate on Snowflake — fail loud rather than
        # silently drop the weight.
        with self.assertRaises(sqlalchemy.exc.CompileError):
            self._sql(sqlalchemy.func.quantile_tdigest_weighted(0.5, sqlalchemy.column('x'), sqlalchemy.column('w')))


class TestMetricMultiplySnowflake(SnowflakeTest):
    def test_metric_multiply_raises(self):
        # The default's bare-NUMERIC squash is NUMBER(38, 0) on Snowflake, so
        # '1.5K' would round to 2 before the multiplier (→ 2000) — loud beats
        # silently wrong.
        expr = sqlalchemy.func.metric_multiply(sqlalchemy.column('t'))
        with self.assertRaises(sqlalchemy.exc.CompileError):
            expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})


class TestSnowflakeDefaultOk(unittest.TestCase):
    """The parity harness keys known-gap skips on snowflake-variant absence;
    _SNOWFLAKE_DEFAULT_OK is the explicit per-function confirmation that the
    default rendering is valid Snowflake SQL."""

    def test_members_have_no_snowflake_variant(self):
        for cls in sf._SNOWFLAKE_DEFAULT_OK:
            with self.subTest(cls=cls.__name__):
                specs = cls._compiler_dispatcher.specs
                self.assertNotIn('snowflake', specs)

    def test_safe_extract_default_on_snowflake(self):
        eng = sqlalchemy.create_engine('snowflake://127.0.0.1/')
        c = sqlalchemy.column('d', eng.dialect.type_descriptor(sqlalchemy.types.DateTime))
        expr = sqlalchemy.func.extract('year', c)
        compiled = expr.compile(dialect=eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('EXTRACT(year FROM d)', str(compiled))

    def test_regexp_substr_default_on_snowflake(self):
        eng = sqlalchemy.create_engine('snowflake://127.0.0.1/')
        expr = sqlalchemy.func.regexp_substr('s', 'p')
        compiled = expr.compile(dialect=eng.dialect, compile_kwargs={"literal_binds": True})
        self.assertEqual("regexp_substr('s', 'p')", str(compiled))


class TestAlteryxDialectAdditions(StarrocksTest):
    """array_tail / array_to_string / string_agg / titlecase / median / any."""
    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))


    def test_array_tail_becomes_array_slice(self):
        # Text To Columns overflow-to-last-field: array_to_string(array_tail(split(...), i), d)
        self.assertEqual("array_slice(split('a-b', '-'), 2)",
                         self._sql(sqlalchemy.func.array_tail(
                             sqlalchemy.func.split('a-b', '-'), 2)))

    def test_array_to_string_becomes_array_join(self):
        self.assertEqual("array_join(split('a-b', '-'), '-')",
                         self._sql(sqlalchemy.func.array_to_string(
                             sqlalchemy.func.split('a-b', '-'), '-')))

    def test_string_agg_becomes_group_concat_with_separator_clause(self):
        # Passing the delimiter as a second argument concatenates it onto every
        # value on StarRocks; it has to be a SEPARATOR clause.
        self.assertEqual("group_concat(c SEPARATOR '-')",
                         self._sql(sqlalchemy.func.string_agg(sqlalchemy.column('c'), '-')))

    def test_string_agg_without_separator(self):
        self.assertEqual('group_concat(c)',
                         self._sql(sqlalchemy.func.string_agg(sqlalchemy.column('c'))))

    def test_titlecase_becomes_initcap(self):
        self.assertEqual("initcap('a b')", self._sql(sqlalchemy.func.titlecase('a b')))

    def test_median_becomes_percentile_approx(self):
        # StarRocks has no median(); percentile_approx(col, 0.5) is the equivalent.
        self.assertEqual('percentile_approx(c, 0.5)',
                         self._sql(sqlalchemy.func.median(sqlalchemy.column('c'))))

    def test_any_becomes_any_value(self):
        self.assertEqual('any_value(c)', self._sql(sqlalchemy.func.any(sqlalchemy.column('c'))))

    def test_string_cast_renders_string_not_char(self):
        # StarRocks inherits MySQL's CAST rules (string types -> CHAR), but CHAR
        # maxes at 255 so CHAR(4000) is rejected by the strict INSERT ... UNION
        # type check against a STRING/varchar column. Render string casts as STRING.
        from plaidcloud.rpc.type_conversion import sqlalchemy_from_dtype
        self.assertEqual('CAST(x AS STRING)',
                         self._sql(sqlalchemy.cast(sqlalchemy.column('x'),
                                                   sqlalchemy_from_dtype('String'))))
        self.assertEqual('CAST(NULL AS STRING)',
                         self._sql(sqlalchemy.cast(sqlalchemy.null(),
                                                   sqlalchemy_from_dtype('String'))))

    def test_non_string_casts_are_unchanged(self):
        # Only string-target casts are rewritten; numeric/decimal casts keep the
        # MySQL-inherited rendering.
        self.assertEqual('CAST(n AS SIGNED INTEGER)',
                         self._sql(sqlalchemy.cast(sqlalchemy.column('n'), sqlalchemy.Integer())))
        self.assertEqual('CAST(d AS DECIMAL(38, 10))',
                         self._sql(sqlalchemy.cast(sqlalchemy.column('d'), sqlalchemy.Numeric(38, 10))))

    def test_fixed_char_and_uuid_casts_are_unchanged(self):
        # A fixed CHAR is ≤255 and valid on StarRocks; the uuid type (GUIDHyphens)
        # is a CHAR(36) decorator. Neither is rewritten to STRING.
        from plaidcloud.rpc.database import GUIDHyphens
        self.assertIn('CHAR', self._sql(sqlalchemy.cast(sqlalchemy.column('u'), GUIDHyphens())))
        self.assertNotIn('STRING', self._sql(sqlalchemy.cast(sqlalchemy.column('c'),
                                                             sqlalchemy.CHAR(36))))


class TestStringCastOtherDialects(DatabendTest):
    """The STRING cast rewrite is StarRocks-only; other dialects are untouched."""
    def test_databend_string_cast_unchanged(self):
        from plaidcloud.rpc.type_conversion import sqlalchemy_from_dtype
        rendered = str(sqlalchemy.cast(sqlalchemy.column('x'), sqlalchemy_from_dtype('String'))
                       .compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))
        self.assertNotIn('AS STRING', rendered)


class TestAlteryxDialectAdditionsDatabend(DatabendTest):
    """Default/Databend path -- notably titlecase, which Databend cannot render and
    must fail loudly rather than emit a literal `titlecase(...)`."""

    def _sql(self, expr):
        return str(expr.compile(dialect=self.eng.dialect, compile_kwargs={"literal_binds": True}))

    def test_titlecase_raises_on_databend(self):
        import sqlalchemy.exc
        with self.assertRaises(sqlalchemy.exc.CompileError):
            self._sql(sqlalchemy.func.titlecase('a b'))

    def test_string_agg_without_separator_databend(self):
        # Default/Databend path is a pure passthrough to the native string_agg.
        self.assertEqual('string_agg(c)',
                         self._sql(sqlalchemy.func.string_agg(sqlalchemy.column('c'))))

    def test_array_tail_renders_slice_on_databend(self):
        self.assertEqual("slice(split('a-b', '-'), 2)",
                         self._sql(sqlalchemy.func.array_tail(sqlalchemy.func.split('a-b', '-'), 2)))



class TestTitlecaseUnspecializedDialect(unittest.TestCase):
    """StarRocks is the only dialect with a titlecase specialization (initcap); the
    bare default must fail loud for every other target so a third dialect cannot
    silently reintroduce the literal titlecase() render."""

    def test_raises_on_a_dialect_without_a_specialization(self):
        import sqlalchemy, sqlalchemy.exc
        from sqlalchemy.dialects import postgresql
        with self.assertRaises(sqlalchemy.exc.CompileError):
            str(sqlalchemy.func.titlecase('a b').compile(
                dialect=postgresql.dialect(), compile_kwargs={"literal_binds": True}))
