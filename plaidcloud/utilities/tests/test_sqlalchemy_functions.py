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
