# coding=utf-8

import unittest

import sqlalchemy
from toolz.functoolz import identity as ident

from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.utilities import sql_expression as se
from plaidcloud.utilities.analyze_table import compiled

__author__ = "Adams Tower"
__copyright__ = "© Copyright 2009-2021, Tartan Solutions, Inc"
__credits__ = ["Adams Tower"]
__license__ = "Apache 2.0"
__maintainer__ = "Adams Tower"
__email__ = "adams.tower@tartansolutions.com"


class TestSQLExpression(unittest.TestCase):

    def assertEquivalent(self, left, right):
        """Asserts that two sqlalchemy expressions resolve to the same SQL code"""
        return self.assertEqual(compiled(left), compiled(right))

    def test_get_project_schema(self):
        self.assertEqual(se.get_project_schema('12345'), 'anlz12345')
        self.assertEqual(se.get_project_schema('anlz12345'), 'anlz12345')

    def test_get_agg_fn(self):
        self.assertEqual(se.get_agg_fn(None), ident)
        self.assertEqual(se.get_agg_fn(''), ident)
        self.assertEqual(se.get_agg_fn('group'), ident)
        self.assertEqual(se.get_agg_fn('dont_group'), ident)

        self.assertEqual(se.get_agg_fn('sum'), sqlalchemy.func.sum)
        self.assertEqual(se.get_agg_fn('count_null'), sqlalchemy.func.count)

    def test_get_table_rep(self):
        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        self.assertTrue(isinstance(table, sqlalchemy.Table))

        self.assertEqual(table.name, 'table_12345')
        self.assertEqual(table.schema, 'anlz_schema')

        self.assertEqual(len(table.columns), 2)
        column_1, column_2 = table.columns
        self.assertTrue(isinstance(column_1, sqlalchemy.Column))
        self.assertTrue(isinstance(column_2, sqlalchemy.Column))
        self.assertEqual(column_1.name, 'Column1')
        self.assertEqual(column_1.type, PlaidUnicode(length=5000))
        self.assertEqual(column_2.name, 'Column2')
        self.assertEqual(column_2.type, sqlalchemy.NUMERIC())

        same_table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=table.metadata,
        )
        self.assertEqual(table, same_table)

        table_using_column_key = se.get_table_rep(
            'table_12345',
            [
                {'foobar': 'Column1', 'dtype': 'text'},
                {'foobar': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=table.metadata,
            column_key='foobar',
        )
        self.assertEqual(table, table_using_column_key)

        aliased_table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=table.metadata,
            alias='table_alias',
        )
        self.assertTrue(isinstance(aliased_table, sqlalchemy.sql.selectable.Alias))
        self.assertEqual(aliased_table.name, 'table_alias')

    def test_get_table_rep_using_id(self):
        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        table2 = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            '_schema',
        )
        self.assertTrue(isinstance(table2, sqlalchemy.Table))
        self.assertEqual(table.schema, table2.schema)

    def test_get_column_table(self):
        self.assertEqual(se.get_column_table(['table1'], None, None), 'table1')

        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {
                    'source': 'foobar',
                    'target': 'foobar',
                    'dtype': 'text',
                    'source_table': 'table1',
                },
                None,
            ),
            'table1',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {
                    'source': 'foobar',
                    'target': 'foobar',
                    'dtype': 'text',
                    'source_table': 'table a',
                },
                None,
            ),
            'table1',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {
                    'source': 'foobar',
                    'target': 'foobar',
                    'dtype': 'text',
                    'source_table': 'table2',
                },
                None,
            ),
            'table2',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {
                    'source': 'foobar',
                    'target': 'foobar',
                    'dtype': 'text',
                    'source_table': 'table b',
                },
                None,
            ),
            'table2',
        )

        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table1.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
            ),
            'table1',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table2.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
            ),
            'table2',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table3.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
            ),
            'table3',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table0.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
                table_numbering_start=0,
            ),
            'table1',
        )

        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
            ),
            'table1',
        )

        source_column_configs = [
            [
                {'source': 'foobar', 'dtype': 'text'},
                {'source': 'barbar', 'dtype': 'text'},
            ],
            [
                {'source': 'barfoo', 'dtype': 'text'},
                {'source': 'barbar', 'dtype': 'text'},
            ],
        ]
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'foobar', 'target': 'foobar', 'dtype': 'text'},
                source_column_configs,
            ),
            'table1',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'barfoo', 'target': 'barfoo', 'dtype': 'text'},
                source_column_configs,
            ),
            'table2',
        )
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'barbar', 'target': 'barbar', 'dtype': 'text'},
                source_column_configs,
            ),
            'table1',
        )

        with self.assertRaises(se.SQLExpressionError):
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'foofoo', 'target': 'foofoo', 'dtype': 'text'},
                source_column_configs,
            )

    def test_clean_where(self):
        self.assertEqual(se.clean_where('where_clause'), 'where_clause')
        self.assertEqual(se.clean_where(' where\n\r_clause '), 'where_clause')

    def test_eval_expression(self):
        self.assertEqual(se.eval_expression("'foobar'", {}, []), 'foobar')

        self.assertEqual(se.eval_expression("'{var}'", {'var': 'foobar'}, []), 'foobar')
        self.assertEqual(
            se.eval_expression(
                "'{var}'", {'var': 'foobar'}, [], disable_variables=True
            ),
            '{var}',
        )

        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        self.assertEqual(se.eval_expression("table", {}, [table]), table)
        self.assertEqual(se.eval_expression("table1", {}, [table]), table)
        self.assertEqual(
            se.eval_expression("table0", {}, [table], table_numbering_start=0), table
        )

        self.assertEqual(
            se.eval_expression("foobar", {}, [], extra_keys={'foobar': 123}), 123
        )

        with self.assertRaises(se.SQLExpressionError):
            se.eval_expression("1/0", {}, [])

    def test_on_clause(self):
        table_a = se.get_table_rep(
            'table_a',
            [
                {'source': 'KeyA', 'dtype': 'text'},
                {'source': 'ValueA', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        table_b = se.get_table_rep(
            'table_b',
            [
                {'source': 'KeyB', 'dtype': 'text'},
                {'source': 'ValueB', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )

        self.assertEquivalent(
            se.on_clause(table_a, table_b, [{'a_column': 'KeyA', 'b_column': 'KeyB'}]),
            table_a.columns.KeyA == table_b.columns.KeyB,
        )
        self.assertEquivalent(
            se.on_clause(
                table_a,
                table_b,
                [
                    {'a_column': 'KeyA', 'b_column': 'KeyB'},
                    {'a_column': 'ValueA', 'b_column': 'valueB'},
                ],
            ),
            sqlalchemy.and_(
                table_a.columns.KeyA == table_b.columns.KeyB,
                table_a.columns.ValueA == table_b.columns.ValueB,
            ),
        )

        self.assertEquivalent(
            se.on_clause(
                table_a,
                table_b,
                [{'a_column': 'KeyA', 'b_column': 'KeyB'}],
                special_null_handling=True,
            ),
            sqlalchemy.or_(
                table_a.columns.KeyA == table_b.columns.KeyB,
                sqlalchemy.and_(
                    table_a.c.KeyA.is_(None),
                    table_b.c.KeyB.is_(None),
                ),
            ),
        )

    def test_get_from_clause(self):
        # hoo boy
        source_column_configs = [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
        ]
        table = se.get_table_rep('table_12345', source_column_configs, 'anlz_schema')

        # Should always return a Label object
        self.assertTrue(
            isinstance(
                se.get_from_clause(
                    [table],
                    {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                    source_column_configs,
                )
            ),
            sqlalchemy.sql.elements.Label,
        )

        # source
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )
        # TODO: test all the weird stuff related to which table columns come from, table.{}, etc.

        # constant
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            sqlalchemy.literal('foobar').label('TargetColumn'),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'constant': '{var}', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                variables={'var': 'foobar'},
            ),
            sqlalchemy.literal('foobar').label('TargetColumn'),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'constant': '{var}', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                variables={'var': 'foobar'},
                disable_variables=True,
            ),
            sqlalchemy.literal('{var}').label('TargetColumn'),
        )

        # expression - more complex tests would just go in test_eval_expression
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            sqlalchemy.cast('foobar', PlaudUnicode(length=5000)).label('TargetColumn'),
        )

        self.assertIsNone(
            se.get_from_clause(
                [table],
                {'target': 'TargetColumn', 'dtype': 'serial'},
                source_column_configs,
            )
        )
        self.assertIsNone(
            se.get_from_clause(
                [table],
                {'target': 'TargetColumn', 'dtype': 'bigserial'},
                source_column_configs,
            )
        )

        with self.assertRaises(se.SQLExpressionError):
            se.get_from_clause(
                [table],
                {'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            )

        #TODO: test agg, sort, cast=False, other dtypes, and the stuff I listed in the subsections
        #TODO: Hmm. Should this be refactored into three smaller functions for constant, expression, source? Plus one for generating the process function I guess? Probably yes, but compleete the tests first.
