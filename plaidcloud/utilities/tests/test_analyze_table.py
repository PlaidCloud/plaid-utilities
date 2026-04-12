# coding=utf-8
"""Tests for plaidcloud.utilities.analyze_table."""

import unittest

import sqlalchemy

from plaidcloud.utilities import analyze_table


class TestCompiled(unittest.TestCase):

    def setUp(self):
        metadata = sqlalchemy.MetaData()
        self.table = sqlalchemy.Table(
            'widgets', metadata,
            sqlalchemy.Column('id', sqlalchemy.Integer),
            sqlalchemy.Column('name', sqlalchemy.String),
        )

    def test_compile_simple_select(self):
        query = sqlalchemy.select(self.table)
        sql, params = analyze_table.compiled(query, dialect='greenplum')
        self.assertIn('widgets', sql)
        # Single-line output (no embedded newlines)
        self.assertNotIn('\n', sql)
        self.assertIsInstance(params, dict)

    def test_none_dialect_defaults_to_greenplum(self):
        query = sqlalchemy.select(self.table)
        sql_none, _ = analyze_table.compiled(query, dialect=None)
        sql_gp, _ = analyze_table.compiled(query, dialect='greenplum')
        self.assertEqual(sql_none, sql_gp)

    def test_empty_string_dialect_defaults_to_greenplum(self):
        query = sqlalchemy.select(self.table)
        sql_empty, _ = analyze_table.compiled(query, dialect='')
        sql_gp, _ = analyze_table.compiled(query, dialect='greenplum')
        self.assertEqual(sql_empty, sql_gp)

    def test_starrocks_dialect_compiles(self):
        # If the starrocks dialect extra is available, it should also compile.
        query = sqlalchemy.select(self.table)
        try:
            sql, _ = analyze_table.compiled(query, dialect='starrocks')
        except Exception as e:
            self.skipTest(f'starrocks dialect unavailable: {e}')
        self.assertIn('widgets', sql)


if __name__ == '__main__':
    unittest.main()
