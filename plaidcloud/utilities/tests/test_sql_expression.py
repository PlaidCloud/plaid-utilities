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
        self.assertEqual(se.eval_expression("table", {}, [table]), table.columns)
        self.assertEqual(se.eval_expression("table1", {}, [table]), table.columns)
        self.assertEqual(
            se.eval_expression("table0", {}, [table], table_numbering_start=0), table.columns
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
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'numeric'},
                source_column_configs,
            ),
            sqlalchemy.cast(table.c.Column1, sqlalchemy.NUMERIC).label(
                'TargetColumn'
            ),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': 'table.Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count_null'},
                source_column_configs,
            ),
            sqlalchemy.cast(None, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

        with self.assertRaises(se.SQLExpressionError):
            se.get_from_clause(
                [table],
                {'source': 'NonexistentColumn', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            )

        # weird edge case
        edge_source_column_configs = [
            {'source': 'column.with.dot', 'dtype': 'text'},
        ]
        edge_table = se.get_table_rep('table_12345', edge_source_column_configs, 'anlz_schema')
        self.assertEquivalent(
            se.get_from_clause(
                [edge_table],
                {'source': 'column.with.dot', 'target': 'TargetColumn', 'dtype': 'text'},
                edge_source_column_configs,
            ),
            sqlalchemy.cast(table.c['column.with.dot'], PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

                

        

        # TODO: test all the weird stuff related to which table columns come from, table.{}, etc.

        # For source, cast=False means don't cast
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                cast=False,
            ),
            table.c.Column1.label('TargetColumn'),
        )

        # constant
        self.assertEquivalent)(
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

        # For constant columns, cast is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            se.get_from_clause(
                [table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                cast=False,
            ),
        )

        # For constant columns, aggregate is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            se.get_from_clause(
                [table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                source_column_configs,
                aggregate=True,
            ),
        )

        # expression - more complex tests would just go in test_eval_expression
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            sqlalchemy.cast('foobar', PlaidUnicode(length=5000)).label('TargetColumn'),
        )

        # For expression columns, cast is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
            se.get_from_clause(
                [table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                cast=False,
            ),
        )

        # aggregate means pay attention to the agg param
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                source_column_configs,
                aggregate=True,
            ),
            sqlalchemy.cast(sqlalchemy.func.count(table.c.Column1), PlaidUnicode(length=5000)).label('TargetColumn')
        )

        # if aggregate is False or absent, agg param is ignored
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                source_column_configs,
            ),
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            ),
        )

        # sort
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}},
                source_column_configs,
                sort=True
            ),
            sqlalchemy.asc(sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000))).label('TargetColumn')
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': False}},
                source_column_configs,
                sort=True
            ),
            sqlalchemy.desc(sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000))).label('TargetColumn')
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
                sort=True
            ),
            sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000)).label('TargetColumn')
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}},
                source_column_configs
            ),
            sqlalchemy.asc(sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000))).label('TargetColumn')
        )

        # If a column doesn't have source, expression or constant, but is serial, return None
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

        # If a column doesn't have source, expression or constant, but is any type other than serial/bigserial, raise error
        with self.assertRaises(se.SQLExpressionError):
            se.get_from_clause(
                [table],
                {'target': 'TargetColumn', 'dtype': 'text'},
                source_column_configs,
            )

        # The function application order is sort(cast(agg(x))).label()
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}, 'agg': 'count'},
                source_column_configs,
                sort=True,
                aggregate=True,
            ),
            sqlalchemy.asc(sqlalchemy.func.count(sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000)))).label('TargetColumn')
        )

        # constant takes priority over expression takes priority over source
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {
                    'constant': 'barfoo',
                    'expression': "'foobar'",
                    'source': 'Column1',
                    'target': 'TargetColumn',
                    'dtype': 'text',
                },
                source_column_configs,
            ),
            sqlalchemy.literal('barfoo').label(
                'TargetColumn'
            ),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {
                    'expression': "'foobar'",
                    'source': 'Column1',
                    'target': 'TargetColumn',
                    'dtype': 'text',
                },
                source_column_configs,
            ),
            sqlalchemy.cast('foobar', PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )
        self.assertEquivalent(
            se.get_from_clause(
                [table],
                {
                    'source': 'Column1',
                    'target': 'TargetColumn',
                    'dtype': 'text',
                },
                source_column_configs,
            ),
            sqlalchemy.cast(table.c.Column1, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

        #TODO: Hmm. Should this be refactored into three smaller functions for constant, expression, source? Plus one for generating the process function I guess? Probably yes, but compleete the tests first.

    def test_get_combined_wheres(self):
        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )

        self.assertEquivalent(
            se.get_combined_wheres(["table.Column1 == 'foo'", "table.Column2 == 0", ""], [table], {})
            [table.c.Column1 == 'foo', table.c.Column2 == 'bar']
        )

    def test_get_select_query(self):
        source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        table = se.get_table_rep(
            'table_12345',
            source_columns,
            'anlz_schema',
        )
        from_clause = curry(se.get_from_clause, [table], [source_columns])

        # Things to test:
        # basic function
        target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], []),
            sqlalchemy.select(from_clause(target_column))
        )
        
        # serial are ignored
        row_number_tc = {'target': 'RowNumber', 'dtype': 'serial'}
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column, row_number_tc], []),
            se.get_select_query([table], [source_columns], [target_column], []),
        )

        # wheres section
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], ['table.Column2 > 0']),
            sqlalchemy.select(from_clause(target_column)).where(table.c.Column2 > 0),
        )
        
        # sorting
        column_2_ascending = {'target': 'Column2', 'source': 'Column2', 'sort': {'ascending': True, 'order': 0}}
        column_3_descending = {'target': 'Column3', 'source': 'Column3', 'sort': {'ascending': False, 'order': 1}}
        se.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [target_column, column_2_ascending, column_3_descending],
                [],
            ),
            sqlalchemy.select(
                from_clause(target_column),
                from_clause(column_2_ascending),
                from_clause(column_3_descending),
            ).order_by(
                from_clause(column_2_ascending, sort=True),
                from_clause(column_3_descending, sort=True),
            ),
        )
        # neither select nor sort should include serial columns
        serial_ascending = {'target': 'RowCount', 'dtype': 'serial', 'sort': {'ascending': True, 'order': 3}}
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column, column_2_ascending, serial_ascending], []),
            se.get_select_query([table], [source_columns], [target_column, column_2_ascending], []),
        )
        # sort should not include columns with malformed sort sections
        # TODO: malformed is currently defined as missing ascending. I believe missing order will error and that that's a current bug
        malformed_sort = {'target': 'Column2', 'source': 'Column2', 'sort': {'order': 4}}
        se.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [target_column, malformed_sort, column_3_descending],
                [],
            ),
            sqlalchemy.select(
                from_clause(target_column),
                from_clause(malformed_sort),
                from_clause(column_3_descending),
            ).order_by(from_clause(column_3_descending, sort=True)),
        )
            
        # groupby (if aggregate)
        groupby_column_1 = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'agg': 'group'}
        sum_column_2 = {'target': 'Sum', 'source': 'Column2', 'dtype': 'numeric', 'agg': 'sum'}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1, sum_column_2],
                [],
                aggregate=True,
            ),
            sqlalchemy.select(
                from_clause(groupby_column_1, aggregate=True),
                from_clause(sum_column_2, aggregate=True),
            ).group_by(from_clause(grouby_column_1, aggregate=False, cast=False)),
        )
        # constants aren't included in groupby
        groupby_constant = {'target': 'Five', 'constant': 5, 'dtype': 'numeric', 'agg': 'group'}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1, groupby_constant, sum_column_2],
                [],
                aggregate=True,
            ),
            sqlalchemy.select(
                from_clause(groupby_column_1, aggregate=True),
                from_clause(groupby_constant, aggregate=True),
                from_clause(sum_column_2, aggregate=True),
            ).group_by(from_clause(groupby_column_1, aggregate=False, cast=False)),
        )
        # serials aren't included in groupby
        groupby_serial = {'target': 'RowCount', 'dtype': 'serial', 'agg': 'group'}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1, groupby_serial, sum_column_2],
                [],
                aggregate=True,
            ),
            sqlalchemy.select(
                from_clause(groupby_column_1, aggregate=True),
                from_clause(groupby_serial, aggregate=True),
                from_clause(sum_column_2, aggregate=True),
            ).group_by(from_clause(groupby_column_1, aggregate=False, cast=False)),
        )
        # don't group by if aggregate is turned off
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1, sum_column_2],
                [],
                aggregate=False,
            ),
            sqlalchemy.select(
                from_clause(groupby_column_1),
                from_clause(sum_column_2),
            ),
        )

        # distinct
        distinct_column_1 = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'distinct': True}
        column_2 = {'target': 'Column2', 'source': 'Column2', 'dtype': 'numeric'}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [distinct_column_1, column_2],
                [],
                distinct=True
            ),
            sqlalchemy.select(
                from_clause(distinct_column_1),
                from_clause(column_2),
            ).distinct(from_clause(grouby_column_1)),
        )
        # constants aren't included in distinct
        distinct_constant = {'target': 'Five', 'constant': 5, 'dtype': 'numeric', 'distinct': True}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [distinct_column_1, distinct_constant, column_2],
                [],
                distinct=True,
            ),
            sqlalchemy.select(
                from_clause(distinct_column_1),
                from_clause(distinct_constant),
                from_clause(column_2),
            ).distinct(from_clause(distinct_column_1)),
        )
        # serials aren't included in distinct
        distinct_serial = {'target': 'RowCount', 'dtype': 'serial', 'distinct': True}
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [distinct_column_1, distinct_serial, column_2],
                [],
                distinct=True,
            ),
            sqlalchemy.select(
                from_clause(distinct_column_1),
                from_clause(distinct_serial),
                from_clause(column_2),
            ).group_by(from_clause(distinct_column_1)),
        )
        # don't apply distinct if distinct is turned off
        self.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [distinct_column_1, column_2],
                [],
                distinct=False,
            ),
            sqlalchemy.select(
                from_clause(distinct_column_1),
                from_clause(column_2),
            ),
        )

        # having
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], having='result.TargetColumn != 0'),
            se.apply_output_filter(se.get_select_query([table], [source_columns], [target_column], []), 'result.TargetColumn != 0')
        )
       
        # use_target_slicer
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True, limit_target_start=10, limit_target_end=100),
            sqlalchemy.select(from_clause(target_column)).limit(90).offset(10),
        )
        # defaults are 0
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True),
            sqlalchemy.select(from_clause(target_column)).limit(0).offset(0),
        )
        # typical use case, 0-10
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True, limit_target_end=10),
            sqlalchemy.select(from_clause(target_column)).limit(10).offset(0),
        )

        # count
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [], [], count=True),
            sqlalchemy.select(sqlalchemy.func.count()).select_from(table),
        )

        # args from config are the same as args passed in
        se.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1, sum_column_2],
                [],
                aggregate=True,
            ),
            se.get_select_query([table], [source_columns], [groupby_column_1, sum_column_2], [], config={'aggregate': True})
        )
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True, limit_target_start=10, limit_target_end = 100),
            se.get_select_query([table], [source_columns], [target_column], [], config={'use_target_slicer': True, 'limit_target_start': 10, 'limit_target_end': 100}),
        )
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [], [], count=True),
            se.get_select_query([table], [source_columns], [], [], config={'count': True}),
        )

        # args passed in take precedence over args from config
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [], [], count=True, config={'count': False}),
            se.get_select_query([table], [source_columns], [], [], count=True),
        )
        # ...unless Falsy (is this intended behavior? - probably fine since all the defaults are Falsy, just a little weird)
        se.assertEquivalent(
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True, limit_target_start=0, limit_target_end=0, config={'limit_target_start': 10, 'limit_target_end': 100}),
            se.get_select_query([table], [source_columns], [target_column], [], use_target_slicer=True, limit_target_start=10, limit_target_end=100),
        )
 
        # everything is applied in the right order
        groupby_column_1_new = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'agg': 'group'}
        sum_column_2_asc = {'target': 'Sum2', 'source': 'Column2', 'dtype': 'numeric', 'agg': 'sum', 'sort': {'ascending': True, 'order': 0}, 'distinct': True}
        sum_column_3_desc = {'target': 'Sum3', 'source': 'Column3', 'dtype': 'numeric', 'agg': 'sum', 'sort': {'ascending': False, 'order': 1}}

        se.assertEquivalent(
            se.get_select_query(
                [table],
                [source_columns],
                [groupby_column_1_new, sum_column_2_asc, sum_column_3_desc],
                ['table.Column2' > 0],
                aggregate=True,
                distinct=True,
                having='result.Category != "foobar"',
                use_target_slicer=True,
                limit_target_start=10,
                limit_target_end=100,
            ),
            se.apply_output_filter(
                sqlalchemy.select(
                    from_clause(groupby_column_1_new, aggregate=True),
                    from_clause(sum_column_2_asc, aggregate=True),
                    from_clause(sum_column_3_desc, aggregate=True),
                )
                .where(table.c.Column2 > 0)
                .order_by(
                    from_clause(sum_column_2_asc, sort=True, aggregate=True),
                    from_clause(sum_column_3_desc, sort=True, aggregate=True),
                )
                .group_by(
                    from_clause(groupby_column_1_new, aggregate=False, cast=False)
                )
                .distinct(
                    from_clause(sum_column_2_asc, aggregate=True)
                ),
                'result.Category != "foobar"'
            )
            .limit(90)
            .offset(10)
        )

    def test_apply_output_filter(self):
        source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        table = se.get_table_rep(
            'table_12345',
            source_columns,
            'anlz_schema',
        )
        from_clause = curry(se.get_from_clause, [table], [source_columns])
        target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}
        select = sqlalchemy.select(from_clause(target_column))
        result = select.subquery('result')
        self.assertEquivalent(
            se.apply_output_filter(select, 'result.TargetColumn != 0'),
            sqlalchemy.select(*result.columns).where(result.c.TargetColumn != 0)
        )

    #TODO: get_insert_query
    #TODO: get_update_query
    #TODO: get_delete_query
    #TODO: import_data_query
    #TODO: allocate (this is another big one?)
