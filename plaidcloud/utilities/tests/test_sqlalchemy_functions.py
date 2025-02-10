# coding=utf-8
import unittest

import datetime

import sqlalchemy
# from toolz.functoolz import curry
# from toolz.functoolz import identity as ident

# from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.utilities import sqlalchemy_functions as sf
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


class TestImportCol(BaseTest):

    def test_import_col_text(self):
        expr = sqlalchemy.func.import_col('Column1', 'text', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('%(import_col_1)s', str(compiled))
        self.assertEqual('Column1', compiled.params['import_col_1'])

    def test_import_col_numeric(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(str(compiled), 'CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE CAST(%(import_col_1)s AS NUMERIC) END')
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual(0.0, compiled.params['param_1'])

    def test_import_col_numeric_trailing_negatives(self):
        expr = sqlalchemy.func.import_col('Column1', 'numeric', 'YYYY-MM-DD', True)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(str(compiled), 'CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE to_number(%(import_col_1)s, %(to_number_1)s) END')
        self.assertEqual('Column1', compiled.params['import_col_1'])
        self.assertEqual('\\s*', compiled.params['regexp_replace_1'])
        self.assertEqual('', compiled.params['regexp_replace_2'])
        self.assertEqual('', compiled.params['regexp_replace_3'])
        self.assertEqual(0.0, compiled.params['param_1'])
        self.assertEqual('9999999999999999999999999D9999999999999999999999999MI', compiled.params['to_number_1'])

    def test_import_col_interval(self):
        expr = sqlalchemy.func.import_col('Column1', 'interval', 'YYYY-MM-DD', False)
        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(str(compiled), 'CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) THEN NULL ELSE %(import_col_1)s::interval END')
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
        self.assertEqual(str(compiled), ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, '
                                         '%(regexp_replace_2)s) = %(regexp_replace_3)s) THEN %(param_1)s ELSE '
                                         'CAST(CASE WHEN (to_string(regexp_replace(%(import_col_1)s, '
                                         '%(regexp_replace_4)s, %(regexp_replace_5)s)) = %(to_string_1)s) THEN NULL '
                                         'ELSE regexp_replace(%(import_col_1)s, %(regexp_replace_4)s, '
                                         '%(regexp_replace_5)s) END AS DECIMAL(38, 10)) END'))

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
        self.assertEqual(str(compiled), ('CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, '
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
                                         'END END AS DECIMAL(38, 10)) END'))
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
        self.assertEqual(str(compiled), 'CASE WHEN (regexp_replace(%(import_col_1)s, %(regexp_replace_1)s, %(regexp_replace_2)s) = %(regexp_replace_3)s) THEN NULL ELSE to_interval(%(import_col_1)s) END')
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