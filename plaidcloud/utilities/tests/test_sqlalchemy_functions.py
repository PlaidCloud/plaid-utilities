# coding=utf-8
import unittest

import datetime

import sqlalchemy
from sqlalchemy import Column, Integer, SmallInteger, Numeric, Table, MetaData, String, literal
from sqlalchemy.engine.default import DefaultDialect
from sqlalchemy.sql import func
# from toolz.functoolz import curry
# from toolz.functoolz import identity as ident

# from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.utilities import sqlalchemy_functions as sf
# from plaidcloud.utilities.analyze_table import compiled


def _dialect(name):
    """Return a minimal dialect whose name is set to the given string.

    For hana/mssql where their packages aren't installed we fall back to
    DefaultDialect so compilation still proceeds using default behavior.
    For dialects that ARE installed we use the real dialect via
    create_engine so that their visit_* behaviors work correctly.
    """
    installed = {'greenplum', 'postgresql', 'databend', 'starrocks'}
    if name in installed:
        eng = sqlalchemy.create_engine(f'{name}://127.0.0.1/')
        return eng.dialect
    d = DefaultDialect()
    d.name = name
    return d


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
        self.assertEqual('to_date(nullif(trim(CAST(CAST(%(to_date_1)s AS CHAR) AS CHAR)), %(nullif_1)s))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier(self):
        expr = sqlalchemy.func.to_date('2019-01-05', '%Y-%m-%d')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('str2date(nullif(trim(CAST(CAST(%(to_date_1)s AS CHAR) AS CHAR)), %(nullif_1)s), CAST(%(param_1)s AS CHAR))', str(compiled))
        self.assertEqual('2019-01-05', compiled.params['to_date_1'])
        self.assertEqual('%Y-%m-%d', compiled.params['param_1'])
        self.assertEqual('', compiled.params['nullif_1'])

    def test_to_date_specifier_postgres(self):
        expr = sqlalchemy.func.to_date('2019-01-05', 'YYYY-MM-DD')
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('str2date(nullif(trim(CAST(CAST(%(to_date_1)s AS CHAR) AS CHAR)), %(nullif_1)s), CAST(%(param_1)s AS CHAR))', str(compiled))
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





class BaseDialectTest(unittest.TestCase):

    def setUp(self):
        # A table with integer and numeric columns to exercise branches
        # that depend on element.type.
        self.meta = MetaData()
        self.tbl = Table(
            't', self.meta,
            Column('i', Integer),
            Column('si', SmallInteger),
            Column('n', Numeric),
            Column('s', String),
        )

    # Helper to compile expressions
    def _compile(self, expr, dialect_name):
        d = _dialect(dialect_name)
        return str(expr.compile(dialect=d))


class TestElapsedSeconds(BaseDialectTest):

    def test_default(self):
        expr = sf.elapsed_seconds(literal('2020-01-01'), literal('2021-01-01'))
        sql = self._compile(expr, 'postgresql')
        self.assertIn('EPOCH', sql)

    def test_hana(self):
        expr = sf.elapsed_seconds(literal('2020-01-01'), literal('2021-01-01'))
        sql = self._compile(expr, 'hana')
        self.assertIn('Seconds_between', sql)

    def test_mssql(self):
        expr = sf.elapsed_seconds(literal('2020-01-01'), literal('2021-01-01'))
        sql = self._compile(expr, 'mssql')
        self.assertIn('datediff', sql)

    def test_databend(self):
        expr = sf.elapsed_seconds(literal('2020-01-01'), literal('2021-01-01'))
        sql = self._compile(expr, 'databend')
        self.assertIn('INT64', sql)

    def test_starrocks(self):
        expr = sf.elapsed_seconds(literal('2020-01-01'), literal('2021-01-01'))
        sql = self._compile(expr, 'starrocks')
        self.assertIn('seconds_diff', sql)


class TestAvgAndSumHana(BaseDialectTest):

    def test_avg_default(self):
        expr = sf.avg(self.tbl.c.n)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('avg', sql.lower())

    def test_avg_hana_integer(self):
        expr = sf.avg(self.tbl.c.i)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)

    def test_avg_hana_smallint(self):
        expr = sf.avg(self.tbl.c.si)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)

    def test_avg_hana_numeric(self):
        expr = sf.avg(self.tbl.c.n)
        sql = self._compile(expr, 'hana')
        self.assertIn('avg', sql.lower())
        self.assertNotIn('BIGINT', sql)

    def test_sum_hana_integer(self):
        expr = sqlalchemy.func.sum(self.tbl.c.i)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)

    def test_sum_hana_smallint(self):
        expr = sqlalchemy.func.sum(self.tbl.c.si)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)

    def test_sum_hana_numeric(self):
        expr = sqlalchemy.func.sum(self.tbl.c.n)
        sql = self._compile(expr, 'hana')
        self.assertNotIn('BIGINT', sql)


class TestVariance(BaseDialectTest):

    def test_variance_default(self):
        expr = sf.variance(self.tbl.c.n)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('variance', sql.lower())

    def test_variance_hana_integer(self):
        expr = sf.variance(self.tbl.c.i)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)
        self.assertIn('var(', sql)

    def test_variance_hana_smallint(self):
        expr = sf.variance(self.tbl.c.si)
        sql = self._compile(expr, 'hana')
        self.assertIn('BIGINT', sql)

    def test_variance_hana_numeric(self):
        expr = sf.variance(self.tbl.c.n)
        sql = self._compile(expr, 'hana')
        self.assertIn('var(', sql)
        self.assertNotIn('BIGINT', sql)


class TestCustomValues(BaseDialectTest):
    """custom_values is exercised by compiling it via compile_custom_values
    directly. Wrapping it in sqlalchemy.select() triggers
    _populate_column_collection which has a version-skew bug against the
    installed SQLAlchemy (_make_proxy signature). We exercise the compiler
    handler by invoking it via a minimal mock compiler stand-in."""

    def _cols(self):
        return [
            sqlalchemy.sql.expression.column('a', type_=sqlalchemy.Integer),
            sqlalchemy.sql.expression.column('b', type_=sqlalchemy.String),
        ]

    class _Compiler:
        def __init__(self, dialect):
            self.dialect = dialect
            self._type_compiler = dialect.type_compiler_instance

        def visit_column(self, c, include_table=True, **kw):  # noqa: D401
            return c.name

        def visit_cast(self, c, **kw):
            return 'CAST(?)'

        def render_literal_value(self, value, type_):
            return repr(value)

    def _invoke(self, cv, asfrom=False):
        from plaidcloud.utilities.sqlalchemy_functions import (
            compile_custom_values,
        )
        # Patch .columns to the raw column list so compile_custom_values can
        # zip against tup values without triggering _populate_column_collection.
        cv.columns = cv._column_args
        return compile_custom_values(
            cv, self._Compiler(sqlalchemy.create_engine('postgresql://x/').dialect), asfrom=asfrom,
        )

    def test_custom_values_basic(self):
        cv = sf.custom_values(self._cols(), (1, 'x'), (2, 'y'))
        sql = self._invoke(cv)
        self.assertIn('VALUES', sql)

    def test_custom_values_with_column_clause(self):
        cc = sqlalchemy.sql.expression.column('c', type_=sqlalchemy.Integer)
        cv = sf.custom_values(self._cols(), (cc, 'x'))
        sql = self._invoke(cv)
        self.assertIn('VALUES', sql)
        self.assertIn('c', sql)

    def test_custom_values_with_cast(self):
        cast_expr = sqlalchemy.cast(sqlalchemy.literal(1), sqlalchemy.Integer)
        cv = sf.custom_values(self._cols(), (cast_expr, 'x'))
        sql = self._invoke(cv)
        self.assertIn('CAST', sql)

    def test_custom_values_asfrom_alias(self):
        cv = sf.custom_values(self._cols(), (1, 'x'), alias_name='myalias')
        sql = self._invoke(cv, asfrom=True)
        self.assertIn('myalias', sql)

    def test_custom_values_asfrom_lateral(self):
        cv = sf.custom_values(
            self._cols(), (1, 'x'), alias_name='myalias', is_lateral=True,
        )
        sql = self._invoke(cv, asfrom=True)
        self.assertIn('LATERAL', sql)

    def test_custom_values_asfrom_no_alias(self):
        cv = sf.custom_values(self._cols(), (1, 'x'))
        sql = self._invoke(cv, asfrom=True)
        self.assertIn('VALUES', sql)

    def test_from_objects_returns_self(self):
        cv = sf.custom_values(self._cols(), (1, 'x'))
        self.assertEqual(cv._from_objects, [cv])


class TestImportCast(BaseDialectTest):
    """Default (postgres) import_cast branches (lines 169, 173, 175, 177, 181)."""

    def _call(self, dtype, date_format='YYYY-MM-DD', trailing_negs=False, dialect='postgresql'):
        expr = sqlalchemy.func.import_cast('Column1', dtype, date_format, trailing_negs)
        return self._compile(expr, dialect)

    # Default dialect
    def test_date(self):
        sql = self._call('date')
        self.assertIn('to_date', sql.lower())

    def test_timestamp(self):
        sql = self._call('timestamp')
        self.assertIn('to_timestamp', sql.lower())

    def test_time(self):
        sql = self._call('time')
        # 'HH24:MI:SS' is bound as a parameter, so just assert compilation
        # succeeded and emitted the to_timestamp call.
        self.assertIn('to_timestamp', sql.lower())

    def test_percent_date_format(self):
        # triggers the python_to_postgres_date_format conversion at line 169
        sql = self._call('date', date_format='%Y-%m-%d')
        self.assertIn('to_date', sql.lower())

    def test_boolean(self):
        sql = self._call('boolean')
        self.assertIn('boolean', sql.lower())

    def test_interval(self):
        sql = self._call('interval')
        self.assertIn('interval', sql.lower())

    def test_numeric_trailing(self):
        sql = self._call('numeric', trailing_negs=True)
        self.assertIn('to_number', sql.lower())

    def test_numeric(self):
        sql = self._call('numeric')
        self.assertIn('NUMERIC', sql)

    def test_text(self):
        sql = self._call('text')
        # Just ensure it compiled something
        self.assertTrue(len(sql) > 0)


class TestImportCastHana(BaseDialectTest):
    """hana branches: lines 192-222."""

    def _call(self, dtype, date_format='YYYY-MM-DD'):
        expr = sqlalchemy.func.import_cast('Column1', dtype, date_format, False)
        return self._compile(expr, 'hana')

    def test_text(self):
        sql = self._call('text')
        self.assertTrue(len(sql) > 0)

    def test_date(self):
        sql = self._call('date')
        self.assertIn('to_date', sql.lower())

    def test_date_percent_format(self):
        sql = self._call('date', date_format='%Y-%m-%d')
        self.assertIn('to_date', sql.lower())

    def test_timestamp(self):
        sql = self._call('timestamp')
        self.assertIn('to_timestamp', sql.lower())

    def test_interval(self):
        sql = self._call('interval')
        self.assertIn('interval', sql.lower())

    def test_boolean(self):
        sql = self._call('boolean')
        self.assertIn('CASE', sql.upper())

    def test_integer(self):
        sql = self._call('integer')
        self.assertIn('to_int', sql.lower())

    def test_bigint(self):
        sql = self._call('bigint')
        self.assertIn('to_bigint', sql.lower())

    def test_smallint(self):
        sql = self._call('smallint')
        self.assertIn('to_smallint', sql.lower())

    def test_numeric(self):
        sql = self._call('numeric')
        self.assertIn('to_decimal', sql.lower())


class TestImportCastDatabend(BaseDialectTest):

    def _call(self, dtype, date_format='YYYY-MM-DD', trailing_negs=False):
        expr = sqlalchemy.func.import_cast('Column1', dtype, date_format, trailing_negs)
        return self._compile(expr, 'databend')

    def test_date(self):
        sql = self._call('date')
        self.assertIn('to_date', sql.lower())

    def test_timestamp(self):
        sql = self._call('timestamp')
        self.assertIn('to_timestamp', sql.lower())

    def test_time(self):
        sql = self._call('time')
        self.assertIn('to_timestamp', sql.lower())

    def test_interval(self):
        sql = self._call('interval')
        self.assertIn('to_interval', sql.lower())

    def test_boolean(self):
        sql = self._call('boolean')
        self.assertIn('to_boolean', sql.lower())

    def test_integer(self):
        sql = self._call('integer')
        self.assertIn('to_int32', sql.lower())

    def test_bigint(self):
        sql = self._call('bigint')
        self.assertIn('to_int64', sql.lower())

    def test_smallint(self):
        sql = self._call('smallint')
        self.assertIn('to_int16', sql.lower())

    def test_numeric(self):
        sql = self._call('numeric')
        # 'NaN' is bound as a parameter; assert compilation produced a CAST.
        self.assertIn('cast', sql.lower())

    def test_integer_trailing_negs(self):
        sql = self._call('integer', trailing_negs=True)
        self.assertIn('to_int32', sql.lower())

    def test_text(self):
        sql = self._call('text')
        self.assertTrue(len(sql) > 0)


class TestImportCastStarrocks(BaseDialectTest):

    def _call(self, dtype, date_format='YYYY-MM-DD', trailing_negs=False):
        expr = sqlalchemy.func.import_cast('Column1', dtype, date_format, trailing_negs)
        return self._compile(expr, 'starrocks')

    def test_date(self):
        sql = self._call('date')
        # to_date on starrocks dispatches to str2date via the safe_to_date
        # handler registered for 'starrocks'.
        self.assertIn('str2date', sql.lower())

    def test_timestamp(self):
        sql = self._call('timestamp')
        # to_timestamp on starrocks dispatches to str_to_date via the
        # safe_to_timestamp handler registered for 'starrocks'.
        self.assertIn('str_to_date', sql.lower())

    def test_percent_date_format(self):
        sql = self._call('date', date_format='%Y-%m-%d')
        self.assertIn('str2date', sql.lower())

    def test_time(self):
        sql = self._call('time')
        # Starrocks compiles func.to_timestamp via str_to_date handler.
        self.assertIn('str_to_date', sql.lower())

    def test_interval(self):
        sql = self._call('interval')
        self.assertIn('interval', sql.lower())

    def test_boolean(self):
        sql = self._call('boolean')
        self.assertIn('BOOLEAN', sql.upper())

    def test_numeric_trailing(self):
        sql = self._call('numeric', trailing_negs=True)
        self.assertIn('to_number', sql.lower())

    def test_numeric(self):
        sql = self._call('numeric')
        # cast to numeric
        self.assertIn('CAST', sql.upper())

    def test_text(self):
        sql = self._call('text')
        self.assertTrue(len(sql) > 0)


class TestSafeToTimestamp(BaseDialectTest):

    def test_default_no_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_timestamp', sql.lower())

    def test_default_with_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_timestamp', sql.lower())

    def test_default_with_extra_args(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD', 'extra')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_timestamp', sql.lower())

    def test_databend_no_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_timestamp', sql.lower())

    def test_databend_with_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_timestamp', sql.lower())

    def test_databend_with_extra_args(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD', 'extra')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_timestamp', sql.lower())

    def test_databend_python_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_timestamp', sql.lower())

    def test_starrocks_no_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str_to_date', sql.lower())

    def test_starrocks_with_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str_to_date', sql.lower())

    def test_starrocks_with_extra_args(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', 'YYYY-MM-DD', 'extra')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str_to_date', sql.lower())

    def test_starrocks_python_format(self):
        expr = sqlalchemy.func.to_timestamp('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str_to_date', sql.lower())


class TestSafeExtractDialect(BaseDialectTest):

    def test_with_non_datetime(self):
        # Invokes the `not isinstance(timestamp.type, ...)` branch
        expr = sqlalchemy.func.extract('year', 'some-string')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('EXTRACT', sql.upper())

    def test_with_datetime_column(self):
        t = Table(
            't2', MetaData(),
            Column('dt', sqlalchemy.DateTime),
        )
        expr = sqlalchemy.func.extract('year', t.c.dt)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('EXTRACT', sql.upper())


class TestMetricMultiply(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.metric_multiply('1.5K')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('CASE', sql.upper())


class TestNumericize(BaseDialectTest):

    def test_default(self):
        expr = sqlalchemy.func.numericize('123')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('coalesce', sql.lower())

    def test_databend(self):
        expr = sqlalchemy.func.numericize('123')
        sql = self._compile(expr, 'databend')
        self.assertIn('regexp', sql.lower())

    def test_starrocks(self):
        expr = sqlalchemy.func.numericize('123')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('regexp', sql.lower())


class TestIntegerize(BaseDialectTest):

    def test_round(self):
        expr = sqlalchemy.func.integerize_round('1.5')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('CAST', sql.upper())

    def test_truncate_default(self):
        expr = sqlalchemy.func.integerize_truncate('1.5')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('trunc', sql.lower())

    def test_truncate_databend(self):
        expr = sqlalchemy.func.integerize_truncate('1.5')
        sql = self._compile(expr, 'databend')
        self.assertIn('truncate', sql.lower())

    def test_truncate_starrocks(self):
        expr = sqlalchemy.func.integerize_truncate('1.5')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('truncate', sql.lower())


class TestSliceStringDialect(BaseDialectTest):

    def test_slice_no_args(self):
        expr = sqlalchemy.func.slice_string('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('substring', sql.lower())

    def test_slice_start_positive_no_count(self):
        expr = sqlalchemy.func.slice_string('hello', 2)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('substring', sql.lower())

    def test_slice_start_positive_positive_count(self):
        expr = sqlalchemy.func.slice_string('hello', 1, 3)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('substring', sql.lower())

    def test_slice_start_positive_negative_count(self):
        expr = sqlalchemy.func.slice_string('hello', 1, -2)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('left', sql.lower())

    def test_slice_start_null(self):
        expr = sqlalchemy.func.slice_string('hello', sqlalchemy.null(), 3)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('substring', sql.lower())

    def test_slice_count_null(self):
        expr = sqlalchemy.func.slice_string('hello', 1, sqlalchemy.null())
        sql = self._compile(expr, 'postgresql')
        self.assertIn('substring', sql.lower())

    def test_slice_negative_start_no_count(self):
        expr = sqlalchemy.func.slice_string('hello', -3)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('right', sql.lower())

    def test_slice_negative_start_negative_count(self):
        expr = sqlalchemy.func.slice_string('hello', -3, -1)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('right', sql.lower())

    def test_slice_negative_start_positive_count_raises(self):
        expr = sqlalchemy.func.slice_string('hello', -3, 1)
        with self.assertRaises(NotImplementedError):
            self._compile(expr, 'postgresql')


class TestZfillDialect(BaseDialectTest):

    def test_zfill_default_char(self):
        expr = sqlalchemy.func.zfill('42', 5)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('lpad', sql.lower())

    def test_zfill_custom_char(self):
        expr = sqlalchemy.func.zfill('42', 5, '*')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('lpad', sql.lower())


class TestNormalizeWhitespaceDialect(BaseDialectTest):

    def test_default(self):
        expr = sqlalchemy.func.normalize_whitespace('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('regexp_replace', sql.lower())

    def test_databend(self):
        expr = sqlalchemy.func.normalize_whitespace('hello')
        sql = self._compile(expr, 'databend')
        self.assertIn('regexp_replace', sql.lower())


class TestUnixToTimestamp(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.unix_to_timestamp(12345)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_timestamp', sql.lower())


class TestSafeToDateDialect(BaseDialectTest):

    def test_default_no_args(self):
        expr = sqlalchemy.func.to_date('2020-01-01')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_date', sql.lower())

    def test_default_with_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_date', sql.lower())

    def test_default_with_python_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('to_date', sql.lower())

    def test_databend_no_args(self):
        expr = sqlalchemy.func.to_date('2020-01-01')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_date', sql.lower())

    def test_databend_with_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_date', sql.lower())

    def test_databend_python_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_date', sql.lower())

    def test_starrocks_no_args(self):
        expr = sqlalchemy.func.to_date('2020-01-01')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('to_date', sql.lower())

    def test_starrocks_with_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str2date', sql.lower())

    def test_starrocks_python_format(self):
        expr = sqlalchemy.func.to_date('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'starrocks')
        self.assertIn('str2date', sql.lower())


class TestSafeRoundDialect(BaseDialectTest):

    def test_no_digits(self):
        expr = sqlalchemy.func.round('1.5')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('round', sql.lower())

    def test_with_digits(self):
        expr = sqlalchemy.func.round('1.5', 2)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('round', sql.lower())

    def test_with_extra_args(self):
        expr = sqlalchemy.func.round('1.5', 2, 'HALF_EVEN')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('round', sql.lower())


class TestLtrim(BaseDialectTest):

    def test_default_no_args(self):
        expr = sqlalchemy.func.ltrim('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('ltrim', sql.lower())

    def test_default_with_empty_arg(self):
        expr = sqlalchemy.func.ltrim('hello', '')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('ltrim', sql.lower())

    def test_default_with_arg(self):
        expr = sqlalchemy.func.ltrim('hello', 'h')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('ltrim', sql.lower())

    def test_databend_no_args(self):
        expr = sqlalchemy.func.ltrim('hello')
        sql = self._compile(expr, 'databend')
        self.assertIn('LEADING', sql.upper())

    def test_databend_with_arg(self):
        expr = sqlalchemy.func.ltrim('hello', 'h')
        sql = self._compile(expr, 'databend')
        self.assertIn('LEADING', sql.upper())


class TestRtrim(BaseDialectTest):

    def test_default_no_args(self):
        expr = sqlalchemy.func.rtrim('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('rtrim', sql.lower())

    def test_default_with_arg(self):
        expr = sqlalchemy.func.rtrim('hello', 'o')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('rtrim', sql.lower())

    def test_databend_no_args(self):
        expr = sqlalchemy.func.rtrim('hello')
        sql = self._compile(expr, 'databend')
        self.assertIn('TRAILING', sql.upper())

    def test_databend_with_arg(self):
        expr = sqlalchemy.func.rtrim('hello', 'o')
        sql = self._compile(expr, 'databend')
        self.assertIn('TRAILING', sql.upper())


class TestTrimDialect(BaseDialectTest):

    def test_default_no_args(self):
        expr = sqlalchemy.func.trim('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('trim', sql.lower())

    def test_default_with_arg(self):
        expr = sqlalchemy.func.trim('hello', 'h')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('trim', sql.lower())

    def test_databend_no_args(self):
        expr = sqlalchemy.func.trim('hello')
        sql = self._compile(expr, 'databend')
        self.assertIn('TRIM', sql.upper())

    def test_databend_with_arg(self):
        expr = sqlalchemy.func.trim('hello', 'h')
        sql = self._compile(expr, 'databend')
        self.assertIn('BOTH', sql.upper())


class TestAscii(BaseDialectTest):

    def test_default(self):
        expr = sqlalchemy.func.ascii('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('regexp_replace', sql.lower())

    def test_databend(self):
        expr = sqlalchemy.func.ascii('hello')
        sql = self._compile(expr, 'databend')
        self.assertIn('regexp_replace', sql.lower())


class TestUpperLower(BaseDialectTest):

    def test_upper_no_args(self):
        expr = sqlalchemy.func.upper('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('upper', sql.lower())

    def test_upper_with_args(self):
        expr = sqlalchemy.func.upper('hello', 'en_US')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('upper', sql.lower())

    def test_lower_no_args(self):
        expr = sqlalchemy.func.lower('hello')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('lower', sql.lower())

    def test_lower_with_args(self):
        expr = sqlalchemy.func.lower('hello', 'en_US')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('lower', sql.lower())


class TestSetNull(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.null_values('x', 'NA', 'NULL')
        sql = self._compile(expr, 'postgresql')
        self.assertIn('CASE', sql.upper())


class TestSafeDivideDialect(BaseDialectTest):

    def test_default_with_divide_by_zero(self):
        expr = sqlalchemy.func.safe_divide(1, 2, 0)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('nullif', sql.lower())

    def test_default_without_divide_by_zero(self):
        expr = sqlalchemy.func.safe_divide(1, 2, None)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('nullif', sql.lower())

    def test_starrocks_with_divide_by_zero(self):
        expr = sqlalchemy.func.safe_divide(1, 2, 0)
        sql = self._compile(expr, 'starrocks')
        self.assertIn('nullif', sql.lower())

    def test_starrocks_two_args(self):
        expr = sqlalchemy.func.safe_divide(1, 2)
        sql = self._compile(expr, 'starrocks')
        self.assertIn('nullif', sql.lower())


class TestDateAddDialect(BaseDialectTest):

    def test_compiles(self):
        expr = sf.sql_date_add(literal('2020-01-01'), days=3)
        sql = self._compile(expr, 'postgresql')
        self.assertIn('make_interval', sql.lower())


class TestToCharDatabend(BaseDialectTest):

    def test_no_format(self):
        expr = sqlalchemy.func.to_char('2020-01-01')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_string', sql.lower())

    def test_numeric_format(self):
        expr = sqlalchemy.func.to_char(123, '999,999')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_char', sql.lower())

    def test_date_format(self):
        expr = sqlalchemy.func.to_char('2020-01-01', 'YYYY-MM-DD')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_string', sql.lower())

    def test_date_format_python(self):
        expr = sqlalchemy.func.to_char('2020-01-01', '%Y-%m-%d')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_string', sql.lower())


class TestToNumberDatabend(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.to_number('123', '999999')
        sql = self._compile(expr, 'databend')
        self.assertIn('to_int64', sql.lower())


class TestTransactionTimestampDatabend(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.transaction_timestamp()
        sql = self._compile(expr, 'databend')
        self.assertIn('now', sql.lower())


class TestStrposDatabend(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.strpos('hello', 'lo')
        sql = self._compile(expr, 'databend')
        self.assertIn('locate', sql.lower())


class TestStringToArrayDatabend(BaseDialectTest):

    def test_compiles(self):
        expr = sqlalchemy.func.string_to_array('a,b,c', ',')
        sql = self._compile(expr, 'databend')
        self.assertIn('split', sql.lower())


class TestQuantile(BaseDialectTest):

    def test_tdigest(self):
        expr = sf.quantile_tdigest(0.5, literal(1))
        sql = self._compile(expr, 'postgresql')
        self.assertIn('QUANTILE_TDIGEST', sql.upper())

    def test_cont(self):
        expr = sf.quantile_cont(0.5, literal(1))
        sql = self._compile(expr, 'postgresql')
        self.assertIn('QUANTILE_CONT', sql.upper())

    def test_disc(self):
        expr = sf.quantile_disc(0.5, literal(1))
        sql = self._compile(expr, 'postgresql')
        self.assertIn('QUANTILE_DISC', sql.upper())

    def test_tdigest_weighted(self):
        expr = sf.quantile_tdigest_weighted(0.5, literal(1), literal(2))
        sql = self._compile(expr, 'postgresql')
        self.assertIn('QUANTILE_TDIGEST_WEIGHTED', sql.upper())



if __name__ == '__main__':
    unittest.main()
