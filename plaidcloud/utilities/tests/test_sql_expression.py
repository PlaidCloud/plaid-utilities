# coding=utf-8
import unittest

import sqlalchemy
from toolz.functoolz import curry
from toolz.functoolz import identity as ident

from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.utilities import sql_expression as se
from plaidcloud.utilities.analyze_table import compiled

__author__ = "Adams Tower"
__copyright__ = "© Copyright 2009-2023, Tartan Solutions, Inc"
__credits__ = ["Adams Tower"]
__license__ = "Apache 2.0"
__maintainer__ = "Adams Tower"
__email__ = "adams.tower@tartansolutions.com"


#TODO: test allocate
class TestSQLExpression(unittest.TestCase):
    def assertEquivalent(self, left, right):
        """Asserts that two sqlalchemy expressions resolve to the same SQL code"""
        return self.assertEqual(compiled(left), compiled(right))
    def assertNotEquivalent(self, left, right):
        return self.assertNotEqual(compiled(left), compiled(right))


class TestGetProjectSchema(TestSQLExpression):
    def test_adds_anlz(self):
        self.assertEqual(se.get_project_schema('12345'), 'anlz12345')

    def test_doesnt_add_anlz_when_already_there(self):
        self.assertEqual(se.get_project_schema('anlz12345'), 'anlz12345')


class TestGetAggFn(TestSQLExpression):
    def test_agg_none(self):
        self.assertEqual(se.get_agg_fn(None), ident)

    def test_agg_group(self):
        self.assertEqual(se.get_agg_fn('group'), ident)

    def test_agg_dont_group(self):
        self.assertEqual(se.get_agg_fn('dont_group'), ident)

    def test_agg_func(self):
        self.assertEquivalent(se.get_agg_fn('sum')(), sqlalchemy.func.sum())

    def test_agg_func_null(self):
        self.assertEquivalent(se.get_agg_fn('count_null')(), sqlalchemy.func.count())

    def test_agg_count_distinct(self):
        self.assertEquivalent(se.get_agg_fn('count_distinct')(), sqlalchemy.func.count(sqlalchemy.func.distinct()))


class TestGetTableRep(TestSQLExpression):
    def setUp(self):
        self.metadata = sqlalchemy.MetaData()
        self.table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=self.metadata
        )

    def test_basic_use_case(self):
        self.assertEqual(
            self.table,
            sqlalchemy.Table(
                'table_12345',
                self.metadata,
                sqlalchemy.Column('Column1', PlaidUnicode(length=5000)),
                sqlalchemy.Column('Column2', sqlalchemy.NUMERIC()),
                schema='anlz_schema',
                extend_existing=True,
            )
        )

    def test_works_without_metadata(self):
        # This is actually just testing that it doesn't error
        self.assertIsInstance(
            se.get_table_rep(
                'table_12345',
                [
                    {'source': 'Column1', 'dtype': 'text'},
                    {'source': 'Column2', 'dtype': 'numeric'},
                ],
                'anlz_schema',
            ),
            sqlalchemy.Table
        )

    def test_metadata(self):
        same_table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=self.metadata,
        )
        self.assertEqual(self.table, same_table)

    def test_column_key(self):
        table_using_column_key = se.get_table_rep(
            'table_12345',
            [
                {'foobar': 'Column1', 'dtype': 'text'},
                {'foobar': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
            metadata=self.metadata,
            column_key='foobar',
        )
        self.assertEqual(self.table, table_using_column_key)

    def test_alias(self):
        self.assertEquivalent(
            se.get_table_rep(
                'table_12345',
                [
                    {'source': 'Column1', 'dtype': 'text'},
                    {'source': 'Column2', 'dtype': 'numeric'},
                ],
                'anlz_schema',
                metadata=self.metadata,
                alias='table_alias',
            ),
            sqlalchemy.orm.aliased(self.table, name='table_alias')
        )

    def test_no_table_id_errors(self):
        with self.assertRaises(se.SQLExpressionError):
            se.get_table_rep(None, [], None)

class TestGetTableRepUsingID(TestSQLExpression):
    def test_get_table_rep_using_id(self):
        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        table2 = se.get_table_rep_using_id(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            '_schema',
            metadata=None,
        )
        self.assertIsInstance(table2, sqlalchemy.Table)
        self.assertEqual(table.schema, table2.schema)

class TestGetColumnTable(TestSQLExpression):
    def setUp(self):
        self.source_column_configs = [
            [
                {'source': 'foobar', 'dtype': 'text'},
                {'source': 'barbar', 'dtype': 'text'},
            ],
            [
                {'source': 'barfoo', 'dtype': 'text'},
                {'source': 'barbar', 'dtype': 'text'},
            ],
        ]

    def test_default_is_table1(self):
        self.assertEqual(se.get_column_table(['table1'], None, None), 'table1')

    def test_source_table1(self):
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

    def test_source_table_a(self):
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

    def test_source_table2(self):
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

    def test_source_table_b(self):
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

    def test_tableN_dot_column(self):
        tables = ['table1', 'table2', 'table3']
        for n, t in enumerate(tables, start=1):
            with self.subTest(expected_table=tables[n-1], source=f'table{n}.foobar'):
                self.assertEqual(
                    se.get_column_table(
                        tables,
                        {'source': f'table{n}.foobar', 'target': 'foobar', 'dtype': 'text'},
                        None,
                    ),
                    tables[n-1],
                )

    def test_table_numbering_start(self):
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table0.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
                table_numbering_start=0,
            ),
            'table1',
        )

    # This one errors in v1.0.0 because the check for table.column comes after the check for tableN.column. table.column matches the tableN.column regex, but then there's no N, so it errors. This has been fixed by checking for table.column first.
    def test_table_dot_column(self):
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2', 'table3'],
                {'source': 'table.foobar', 'target': 'foobar', 'dtype': 'text'},
                None,
            ),
            'table1',
        )

    def test_search_for_column_name_in_table_1(self):
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'foobar', 'target': 'foobar', 'dtype': 'text'},
                self.source_column_configs,
            ),
            'table1',
        )

    def test_search_for_column_name_in_table_2(self):
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'barfoo', 'target': 'barfoo', 'dtype': 'text'},
                self.source_column_configs,
            ),
            'table2',
        )

    def test_search_for_column_name_in_both(self):
        self.assertEqual(
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'barbar', 'target': 'barbar', 'dtype': 'text'},
                self.source_column_configs,
            ),
            'table1',
        )

    def test_search_for_column_name_in_neither(self):
        with self.assertRaises(se.SQLExpressionError):
            se.get_column_table(
                ['table1', 'table2'],
                {'source': 'foofoo', 'target': 'foofoo', 'dtype': 'text'},
                self.source_column_configs,
            )

class TestCleanWhere(TestSQLExpression):
    def test_doesnt_overclean(self):
        self.assertEqual(se.clean_where('where_clause'), 'where_clause')

    def test_cleans_newlines(self):
        self.assertEqual(se.clean_where('where\n\r_clause'), 'where_clause')

    def test_trims(self):
        self.assertEqual(se.clean_where(' where_clause '), 'where_clause')

class TestEvalExpression(TestSQLExpression):
    def setUp(self):
        self.table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )

    def test_basic_use_case(self):
        self.assertEqual(se.eval_expression("'foobar'", {}, []), 'foobar')

    def test_variables(self):
        self.assertEqual(se.eval_expression("'{var}'", {'var': 'foobar'}, []), 'foobar')

    def test_disable_variables_var_exists(self):
        self.assertEqual(
            se.eval_expression(
                "'{var}'", {'var': 'foobar'}, [], disable_variables=True
            ),
            'foobar',
        )

    def test_disable_variables_var_not_exists(self):
        self.assertEqual(
            se.eval_expression(
                "'{var}'", {'var2': 'foobar'}, [], disable_variables=True
            ),
            '{var}',
        )

    def test_table(self):
        self.assertEqual(se.eval_expression("table", {}, [self.table]), self.table.columns)

    def test_tableN(self):
        self.assertEqual(se.eval_expression("table1", {}, [self.table]), self.table.columns)

    def test_table_numbering_start(self):
        self.assertEqual(
            se.eval_expression("table0", {}, [self.table], table_numbering_start=0), self.table.columns
        )

    def test_get_column(self):
        self.assertEqual(
            se.eval_expression("get_column(table, 'Column1')", {}, [self.table]),
            self.table.c.Column1,
        )

    def test_get_column_errors_if_not_found(self):
        with self.assertRaises(se.SQLExpressionError):
            se.eval_expression("get_column(table, 'foobar')", {}, [self.table])

    def test_extra_keys(self):
        self.assertEqual(
            se.eval_expression("foobar", {}, [], extra_keys={'foobar': 123}), 123
        )

    def test_error(self):
        with self.assertRaises(se.SQLExpressionError):
            se.eval_expression("1/0", {}, [])

class TestOnClause(TestSQLExpression):
    def setUp(self):
        self.table_a = se.get_table_rep(
            'table_a',
            [
                {'source': 'KeyA', 'dtype': 'text'},
                {'source': 'ValueA', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        self.table_b = se.get_table_rep(
            'table_b',
            [
                {'source': 'KeyB', 'dtype': 'text'},
                {'source': 'ValueB', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )

    def test_basic_use_case(self):
        self.assertEquivalent(
            se.on_clause(self.table_a, self.table_b, [{'a_column': 'KeyA', 'b_column': 'KeyB'}]),
            self.table_a.columns.KeyA == self.table_b.columns.KeyB,
        )

    def test_two_keys(self):
        self.assertEquivalent(
            se.on_clause(
                self.table_a,
                self.table_b,
                [
                    {'a_column': 'KeyA', 'b_column': 'KeyB'},
                    {'a_column': 'ValueA', 'b_column': 'ValueB'},
                ],
            ),
            sqlalchemy.and_(
                self.table_a.columns.KeyA == self.table_b.columns.KeyB,
                self.table_a.columns.ValueA == self.table_b.columns.ValueB,
            ),
        )

    def test_special_null_handling(self):
        self.assertEquivalent(
            se.on_clause(
                self.table_a,
                self.table_b,
                [{'a_column': 'KeyA', 'b_column': 'KeyB'}],
                special_null_handling=True,
            ),
            sqlalchemy.or_(
                self.table_a.columns.KeyA == self.table_b.columns.KeyB,
                sqlalchemy.and_(
                    self.table_a.c.KeyA.is_(None),
                    self.table_b.c.KeyB.is_(None),
                ),
            ),
        )

class TestProcessFn(TestSQLExpression):
    def test_cast(self):
        self.assertEquivalent(
            se.process_fn(None, sqlalchemy.NUMERIC, None, 'test')(321),
            sqlalchemy.cast(321, sqlalchemy.NUMERIC).label('test'),
        )

    def test_agg(self):
        for agg_type in [None, 'group', 'dont_group', 'sum', 'count_null']:
            with self.subTest(agg_type=agg_type):
                self.assertEquivalent(
                    se.process_fn(None, sqlalchemy.NUMERIC, agg_type, 'test')(321),
                    sqlalchemy.cast(se.get_agg_fn(agg_type)(321), sqlalchemy.Numeric).label('test'),
                )

    def test_sort(self):
        for sort_type, expected_fn in [(True, sqlalchemy.asc), (False, sqlalchemy.desc), (None, ident)]:
            with self.subTest(sort_type=sort_type, expected_fn=expected_fn):
                self.assertEquivalent(
                    se.process_fn(sort_type, sqlalchemy.NUMERIC, None, 'test')(321),
                    expected_fn(sqlalchemy.cast(321, sqlalchemy.NUMERIC)).label('test'),
                )


class TestGetFromClause(TestSQLExpression):
    def setUp(self):
        self.source_column_configs = [[
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
        ]]
        self.table = se.get_table_rep('table_12345', self.source_column_configs[0], 'anlz_schema')

    def test_returns_label_object(self):
        # Should always return a Label object
        self.assertIsInstance(
            se.get_from_clause(
                [self.table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            sqlalchemy.sql.elements.Label,
        )

    def test_source_dtype_text(self):
        # source
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

    def test_source_dtype_numeric(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'numeric'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(self.table.c.Column1, sqlalchemy.NUMERIC).label(
                'TargetColumn'
            ),
        )

    def test_source_table_dot_column(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': 'table.Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

    def test_source_count_null(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count_null'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(None, PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

    def test_nonexistent_source_errors(self):
        with self.assertRaises(se.SQLExpressionError):
            se.get_from_clause(
                [self.table],
                {'source': 'NonexistentColumn', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            )

    def test_source_cast_false(self):
        # For source, cast=False means don't cast
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': 'Column1', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                cast=False,
            ),
            self.table.c.Column1.label('TargetColumn'),
        )

    def test_source_column_with_dot(self):
        # weird edge case - column with dot in the name that doesn't represent a relationship to a self.table
        edge_source_column_configs = [[
            {'source': 'column.with.dot', 'dtype': 'text'},
        ]]
        edge_table = se.get_table_rep('table_12345', edge_source_column_configs[0], 'anlz_schema')

        self.assertEquivalent(
            se.get_from_clause(
                [edge_table],
                {'source': 'column.with.dot', 'target': 'TargetColumn', 'dtype': 'text'},
                edge_source_column_configs,
            ),
            sqlalchemy.cast(edge_table.c['column.with.dot'], PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

    def test_constant(self):
        # constant
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('foobar').label('TargetColumn'),
                type_ = PlaidUnicode(length=5000)
            ),
        )

    def test_constant_variables(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'constant': '{var}', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                variables={'var': 'foobar'},
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('foobar').label('TargetColumn'),
                type_=PlaidUnicode(length=5000)
            ),
        )

    def test_constant_disable_variables(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'constant': '{var}', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                variables={'var2': 'foobar'},
                disable_variables=True,
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('{var}').label('TargetColumn'),
                type_=PlaidUnicode(length=5000)
            ),
        )

    def test_constant_irrelevant_cast(self):
        # For constant columns, cast param is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            se.get_from_clause(
                [self.table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                cast=False,
            ),
        )

    def test_constant_irrelevant_aggregate(self):
        # For constant columns, aggregate is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            se.get_from_clause(
                [self.table],
                {'constant': 'foobar', 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                self.source_column_configs,
                aggregate=True,
            ),
        )

    def test_expression(self):
        # expression - more complex tests would just go in test_eval_expression
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            sqlalchemy.cast('foobar', PlaidUnicode(length=5000)).label('TargetColumn'),
        )

    def test_expression_irrelevant_cast(self):
        # For expression columns, cast is irrelevant
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
            se.get_from_clause(
                [self.table],
                {'expression': "'foobar'", 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                cast=False,
            ),
        )

    def test_expression_aggregate(self):
        # aggregate means pay attention to the agg param
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                self.source_column_configs,
                aggregate=True,
            ),
            sqlalchemy.cast(sqlalchemy.func.count(self.table.c.Column1), PlaidUnicode(length=5000)).label('TargetColumn')
        )

    def test_expression_aggregate_false(self):
        # if aggregate is False or absent, agg param is ignored
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'agg': 'count'},
                self.source_column_configs,
            ),
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            ),
        )

    def test_sort_asc(self):
        # sort
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}},
                self.source_column_configs,
                sort=True
            ),
            sqlalchemy.asc(sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000))).label('TargetColumn')
        )

    def test_sort_desc(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': False}},
                self.source_column_configs,
                sort=True
            ),
            sqlalchemy.desc(sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000))).label('TargetColumn')
        )

    def test_sort_but_no_sort_columns(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
                sort=True
            ),
            sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000)).label('TargetColumn')
        )

    def test_sort_no_order(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}},
                self.source_column_configs
            ),
            sqlalchemy.cast(self.table.c.Column1, PlaidUnicode(length=5000)).label('TargetColumn')
        )

    def test_serial_is_none(self):
        # If a column doesn't have source, expression or constant, but is serial, return None
        self.assertIsNone(
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'serial'},
                self.source_column_configs,
                use_row_number_for_serial=False,
            )
        )

    def test_serial_is_row_number(self):
        # If a column doesn't have source, expression or constant, but is serial, return None
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'serial'},
                self.source_column_configs,
                use_row_number_for_serial=True,
            ),
            sqlalchemy.cast(sqlalchemy.func.row_number().over(), sqlalchemy.Integer).label('TargetColumn')
        )

    def test_bigserial_is_none(self):
        self.assertIsNone(
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'bigserial'},
                self.source_column_configs,
                use_row_number_for_serial=False,
            )
        )

    def test_bigserial_is_row_number(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'bigserial'},
                self.source_column_configs,
                use_row_number_for_serial=True,
            ),
            sqlalchemy.cast(sqlalchemy.func.row_number().over(), sqlalchemy.BigInteger).label('TargetColumn')
        )

    def test_magic_columns_is_none(self):
        for dtype in se.MAGIC_COLUMN_MAPPING.keys():
            if dtype == "source_table_name":
                continue
            self.assertIsNone(
                se.get_from_clause(
                    [self.table],
                    {'target': 'TargetColumn', 'dtype': dtype},
                    self.source_column_configs,
                )
            )

    def test_magic_source_table_name(self):
        aliased_table = sqlalchemy.orm.aliased(self.table, name='table_alias')
        self.assertEquivalent(
            se.get_from_clause(
                [aliased_table],
                {'target': 'TargetColumn', 'dtype': 'source_table_name'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('table_alias').label('TargetColumn'),
                type_=PlaidUnicode(length=5000)
            )
        )

    def test_magic_source_table_name_no_alias(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'source_table_name'},
                self.source_column_configs,
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('table_12345').label('TargetColumn'),
                type_=PlaidUnicode(length=5000)
            )
        )

    def test_errors_when_no_source_expression_or_constant(self):
        # If a column doesn't have source, expression or constant, but is any type other than serial/bigserial, raise error
        with self.assertRaises(se.SQLExpressionError):
            se.get_from_clause(
                [self.table],
                {'target': 'TargetColumn', 'dtype': 'text'},
                self.source_column_configs,
            )

    def test_function_application_order(self):
        # The function application order is sort(cast(agg(x))).label()
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {'source': "Column1", 'target': 'TargetColumn', 'dtype': 'text', 'sort': {'ascending': True}, 'agg': 'count'},
                self.source_column_configs,
                sort=True,
                aggregate=True,
            ),
            sqlalchemy.asc(sqlalchemy.cast(sqlalchemy.func.count(self.table.c.Column1), PlaidUnicode(length=5000))).label('TargetColumn')
        )

    def test_priority_constant_expression_source(self):
        # constant takes priority over expression takes priority over source
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {
                    'constant': 'barfoo',
                    'expression': "'foobar'",
                    'source': 'Column1',
                    'target': 'TargetColumn',
                    'dtype': 'text',
                },
                self.source_column_configs,
            ),
            sqlalchemy.cast(
                sqlalchemy.literal('barfoo').label(
                    'TargetColumn',
                ),
                type_=PlaidUnicode(length=5000),
            ),
        )

    def test_priority_expression_source(self):
        self.assertEquivalent(
            se.get_from_clause(
                [self.table],
                {
                    'expression': "'foobar'",
                    'source': 'Column1',
                    'target': 'TargetColumn',
                    'dtype': 'text',
                },
                self.source_column_configs,
            ),
            sqlalchemy.cast('foobar', PlaidUnicode(length=5000)).label(
                'TargetColumn'
            ),
        )

class TestGetCombinedWheres(TestSQLExpression):
    def test_get_combined_wheres(self):
        table = se.get_table_rep(
            'table_12345',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )

        for returned_where, expected_where in zip(
            se.get_combined_wheres(["table.Column1 == 'foo'", "table.Column2 == 0", ""], [table], {}),
            [table.c.Column1 == 'foo', table.c.Column2 == 0]
        ):
            # Not using subTest because I want to test the equivalence of the list
            self.assertEquivalent(returned_where, expected_where)

class TestGetSelectQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [[
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]]
        self.table = se.get_table_rep(
            'table_12345',
            self.source_columns[0],
            'anlz_schema',
        )
        self.from_clause = curry(se.get_from_clause, [self.table], source_column_configs=[self.source_columns])
        self.target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}
        self.column_2_ascending = {'target': 'Column2', 'source': 'Column2', 'dtype': 'numeric', 'sort': {'ascending': True, 'order': 0}}
        self.column_3_descending = {'target': 'Column3', 'source': 'Column3', 'dtype': 'numeric', 'sort': {'ascending': False, 'order': 1}}
        self.groupby_column_1 = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'agg': 'group'}
        self.sum_column_2 = {'target': 'Sum', 'source': 'Column2', 'dtype': 'numeric', 'agg': 'sum'}
        self.distinct_column_1 = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'distinct': True}
        self.column_2 = {'target': 'Column2', 'source': 'Column2', 'dtype': 'numeric'}

    def test_basic_use_case(self):
        # basic function
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], []),
            sqlalchemy.select(self.from_clause(self.target_column))
        )

    def test_serial(self):
        # serial are ignored
        row_number_tc = {'target': 'RowNumber', 'dtype': 'serial'}
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column, row_number_tc], [], use_row_number_for_serial=False),
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_row_number_for_serial=False),
        )

    # def test_serial_row_number(self):
    #     # serial are ignored
    #     row_number_tc = {'target': 'RowNumber', 'dtype': 'serial'}
    #     self.assertEquivalent(
    #         se.get_select_query([self.table], [self.source_columns], [self.target_column, row_number_tc], [], use_row_number_for_serial=True),
    #         se.get_select_query([self.table], [self.source_columns], [self.target_column], [], use_row_number_for_serial=True),
    #     )

    def test_wheres(self):
        # wheres section
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], ['table.Column2 > 0']),
            sqlalchemy.select(self.from_clause(self.target_column)).where(self.table.c.Column2 > 0),
        )

    def test_sort(self):
        # sorting
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.target_column, self.column_2_ascending, self.column_3_descending],
                [],
            ),
            sqlalchemy.select(
                self.from_clause(self.target_column),
                self.from_clause(self.column_2_ascending),
                self.from_clause(self.column_3_descending),
            ).order_by(
                self.from_clause(self.column_2_ascending, sort=True),
                self.from_clause(self.column_3_descending, sort=True),
            ),
        )

    def test_dont_sort_serial(self):
        # neither select nor sort should include serial columns
        serial_ascending = {'target': 'RowCount', 'dtype': 'serial', 'sort': {'ascending': True, 'order': 3}}
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column, self.column_2_ascending, serial_ascending], [], use_row_number_for_serial=False),
            se.get_select_query([self.table], self.source_columns, [self.target_column, self.column_2_ascending], [], use_row_number_for_serial=False),
        )

    def test_dont_sort_without_ascending_param(self):
        # sort should not include columns with sort sections that don't have the 'ascending' param
        malformed_sort = {'target': 'Column2', 'source': 'Column2', 'dtype': 'numeric', 'sort': {'order': 4}}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.target_column, malformed_sort, self.column_3_descending],
                [],
            ),
            sqlalchemy.select(
                self.from_clause(self.target_column),
                self.from_clause(malformed_sort),
                self.from_clause(self.column_3_descending),
            ).order_by(self.from_clause(self.column_3_descending, sort=True)),
        )

    # In v1.0.0 this kind of config just errors. I'm not sure it's possible to produce a config like this in the UI, but there is a reasonable way to handle such a config, so that's better than erroring
    def test_put_columns_without_order_at_end_of_sort(self):
        # columns without a sort order should go at the end for sort
        sort_without_order = {'target': 'Column2', 'source': 'Column2', 'dtype': 'numeric', 'sort': {'ascending': True}}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.target_column, sort_without_order, self.column_3_descending],
                [],
            ),
            sqlalchemy.select(
                self.from_clause(self.target_column),
                self.from_clause(sort_without_order),
                self.from_clause(self.column_3_descending),
            ).order_by(self.from_clause(self.column_3_descending, sort=True), self.from_clause(sort_without_order, sort=True)),
        )

    def test_groupby(self):
        # groupby (if aggregate)
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=True,
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(self.from_clause(self.groupby_column_1, aggregate=False, cast=False)),
        )

    def test_dont_groupby_constant(self):
        # constants aren't included in groupby
        groupby_constant = {'target': 'Five', 'constant': '5', 'dtype': 'numeric', 'agg': 'group'}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, groupby_constant, self.sum_column_2],
                [],
                aggregate=True,
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(groupby_constant, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(self.from_clause(self.groupby_column_1, aggregate=False, cast=False)),
        )

    def test_dont_groupby_serial(self):
        # serials aren't included in groupby
        groupby_serial = {'target': 'RowCount', 'dtype': 'serial', 'agg': 'group'}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, groupby_serial, self.sum_column_2],
                [],
                aggregate=True,
                use_row_number_for_serial=False,
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(self.from_clause(self.groupby_column_1, aggregate=False, cast=False)),
        )

    def test_groupby_serial_row_number(self):
        # serials aren't included in groupby
        groupby_serial = {'target': 'RowCount', 'dtype': 'serial', 'agg': 'group'}
        self.assertEquivalent(
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(groupby_serial, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(
                self.from_clause(self.groupby_column_1, aggregate=False, cast=False),
                self.from_clause(groupby_serial, aggregate=False, cast=False),
            ),
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, groupby_serial, self.sum_column_2],
                [],
                aggregate=True,
                use_row_number_for_serial=True,
            ),
        )

    def test_rollup(self):
        # rollups (if aggregate and aggregation_type == 'rollup')
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=True,
                aggregation_type='rollup'
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(sqlalchemy.func.rollup(self.from_clause(self.groupby_column_1, aggregate=False, cast=False))),
        )

    def test_grouping_sets(self):
        # grouping_sets (if aggregate and aggregation_type == 'sets')
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=True,
                aggregation_type='sets'
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(sqlalchemy.func.grouping_sets(self.from_clause(self.groupby_column_1, aggregate=False, cast=False))),
        )

    def test_cube(self):
        # cube (if aggregate and aggregation_type == 'cube')
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=True,
                aggregation_type='cube'
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1, aggregate=True),
                self.from_clause(self.sum_column_2, aggregate=True),
            ).group_by(sqlalchemy.func.cube(self.from_clause(self.groupby_column_1, aggregate=False, cast=False))),
        )

    def test_aggregate_false(self):
        # don't group by if aggregate is turned off
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=False,
            ),
            sqlalchemy.select(
                self.from_clause(self.groupby_column_1),
                self.from_clause(self.sum_column_2),
            ),
        )

    def test_distinct(self):
        # distinct
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.distinct_column_1, self.column_2],
                [],
                distinct=True
            ),
            sqlalchemy.select(
                self.from_clause(self.distinct_column_1),
                self.from_clause(self.column_2),
            ).distinct(
            #    self.from_clause(self.distinct_column_1)
            ),
        )

    def test_dont_distinct_on_constant(self):
        # constants aren't included in distinct
        distinct_constant = {'target': 'Five', 'constant': '5', 'dtype': 'numeric', 'distinct': True}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.distinct_column_1, distinct_constant, self.column_2],
                [],
                distinct=True,
            ),
            sqlalchemy.select(
                self.from_clause(self.distinct_column_1),
                self.from_clause(distinct_constant),
                self.from_clause(self.column_2),
            ).distinct(
                #self.from_clause(self.distinct_column_1)
            ),
        )

    def test_dont_distinct_on_serial(self):
        # serials aren't included in distinct
        distinct_serial = {'target': 'RowCount', 'dtype': 'serial', 'distinct': True}
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.distinct_column_1, distinct_serial, self.column_2],
                [],
                distinct=True,
                use_row_number_for_serial=False,
            ),
            sqlalchemy.select(
                self.from_clause(self.distinct_column_1, use_row_number_for_serial=False),
                self.from_clause(self.column_2, use_row_number_for_serial=False),
            ).distinct(
                #self.from_clause(self.distinct_column_1, use_row_number_for_serial=False)
            ),
        )

    def test_distinct_on_serial_row_number(self):
        # serials row number is included in distinct
        distinct_serial = {'target': 'RowCount', 'dtype': 'serial', 'distinct': True}
        self.assertEquivalent(
            sqlalchemy.select(
                self.from_clause(self.distinct_column_1),
                self.from_clause(distinct_serial),
                self.from_clause(self.column_2),
            ).distinct(
                #self.from_clause(self.distinct_column_1),
                #self.from_clause(distinct_serial),
            ),
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.distinct_column_1, distinct_serial, self.column_2],
                [],
                distinct=True,
                use_row_number_for_serial=True,
            ),
        )

    def test_distinct_false(self):
        # don't apply distinct if distinct is turned off
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.distinct_column_1, self.column_2],
                [],
                distinct=False,
            ),
            sqlalchemy.select(
                self.from_clause(self.distinct_column_1),
                self.from_clause(self.column_2),
            ),
        )

    def test_having(self):
        # having
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], having='result.TargetColumn != 0'),
            se.apply_output_filter(se.get_select_query([self.table], self.source_columns, [self.target_column], []), 'result.TargetColumn != 0', {})
        )

    def test_use_target_slicer(self):
        # use_target_slicer
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_target_slicer=True, limit_target_start=10, limit_target_end=100),
            sqlalchemy.select(self.from_clause(self.target_column)).limit(90).offset(10),
        )

    def test_limit_defaults(self):
        # defaults are 0
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_target_slicer=True),
            sqlalchemy.select(self.from_clause(self.target_column)).limit(0).offset(0),
        )

    def test_limit_target_end_not_start(self):
        # typical use case, 0-10
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_target_slicer=True, limit_target_end=10),
            sqlalchemy.select(self.from_clause(self.target_column)).limit(10).offset(0),
        )

    def test_count(self):
        # count
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [], [], count=True),
            sqlalchemy.select(sqlalchemy.func.count()).select_from(self.table),
        )

    def test_config(self):
        # args from config are the same as args passed in
        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [self.groupby_column_1, self.sum_column_2],
                [],
                aggregate=True,
            ),
            se.get_select_query([self.table], self.source_columns, [self.groupby_column_1, self.sum_column_2], [], config={'aggregate': True})
        )

    def test_config_lower_priority(self):
        # args passed in take precedence over args from config
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [], [], count=True, config={'count': False}),
            se.get_select_query([self.table], self.source_columns, [], [], count=True),
        )

    def test_config_special_falsy_case(self):
        # Tests that a bug in v1.0.2 is fixed
        self.assertEquivalent(
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_target_slicer=True, limit_target_start=0, limit_target_end=0, config={'limit_target_start': 10, 'limit_target_end': 100}),
            se.get_select_query([self.table], self.source_columns, [self.target_column], [], use_target_slicer=True, limit_target_start=0, limit_target_end=0),
        )

    def test_order(self):
        # everything is applied in the right order
        groupby_column_1_new = {'target': 'Category', 'source': 'Column1', 'dtype': 'text', 'agg': 'group'}
        sum_column_2_asc = {'target': 'Sum2', 'source': 'Column2', 'dtype': 'numeric', 'agg': 'sum', 'sort': {'ascending': True, 'order': 0}, 'distinct': True}
        sum_column_3_desc = {'target': 'Sum3', 'source': 'Column3', 'dtype': 'numeric', 'agg': 'sum', 'sort': {'ascending': False, 'order': 1}}

        self.assertEquivalent(
            se.get_select_query(
                [self.table],
                self.source_columns,
                [groupby_column_1_new, sum_column_2_asc, sum_column_3_desc],
                ['table.Column2 > 0'],
                aggregate=True,
                distinct=True,
                having='result.Category != "foobar"',
                use_target_slicer=True,
                limit_target_start=10,
                limit_target_end=100,
            ),
            se.apply_output_filter(
                sqlalchemy.select(
                    self.from_clause(groupby_column_1_new, aggregate=True),
                    self.from_clause(sum_column_2_asc, aggregate=True),
                    self.from_clause(sum_column_3_desc, aggregate=True),
                )
                .where(self.table.c.Column2 > 0)
                .order_by(
                    self.from_clause(sum_column_2_asc, sort=True, aggregate=True),
                    self.from_clause(sum_column_3_desc, sort=True, aggregate=True),
                )
                .group_by(
                    self.from_clause(groupby_column_1_new, aggregate=False, cast=False)
                )
                .distinct(
                    #self.from_clause(sum_column_2_asc, aggregate=True)
                ),
                'result.Category != "foobar"',
                {},
            )
            .limit(90)
            .offset(10)
        )

class TestSimpleSelectQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )
        self.target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}

    def test_basic_use_case(self):
        self.assertEquivalent(
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
            }, '_schema', None, {}),
            se.get_select_query([self.table], [self.source_columns], [self.target_column], []),
        )

    def test_source_where(self):
        self.assertEquivalent(
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
                'source_where': 'table.Column1 == "foobar"',
            }, '_schema', None, {}),
            se.get_select_query([self.table], [self.source_columns], [self.target_column], ['table.Column1 == "foobar"']),
        )

    def test_source_alias(self):
        aliased_table = sqlalchemy.orm.aliased(self.table, name='table_alias')
        self.assertEquivalent(
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
                'source_alias': 'table_alias',
            }, '_schema', None, {}),
            se.get_select_query([aliased_table], [self.source_columns], [self.target_column], []),
        )

class TestModifiedSelectQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )
        self.target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}

    def test_errors_when_no_fmt_or_mapping_fn(self):
        # no fmt or mapping_fn
        with self.assertRaises(se.SQLExpressionError):
            se.modified_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
            }, 'schema', None)

    def test_fmt(self):
        # fmt
        self.assertEquivalent(
            se.modified_select_query({
                'source_b': 'table_12345',
                'source_columns_b': self.source_columns,
                'target_columns_b': [self.target_column],
            }, 'schema', None, fmt='{}_b'),
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
            }, 'schema', None, {}),
        )

    def test_mapping_fn(self):
        #mapping_fn
        self.assertEquivalent(
            se.modified_select_query({
                'source_b': 'table_12345',
                'source_columns_b': self.source_columns,
                'target_columns_b': [self.target_column],
            }, 'schema', None, mapping_fn=lambda x: f'{x}_b'),
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
            }, 'schema', None, {}),
        )

    def test_default(self):
        #default to standard key
        self.assertEquivalent(
            se.modified_select_query({
                'source_b': 'table_12345',
                'source_columns_b': self.source_columns,
                'target_columns': [self.target_column],
            }, 'schema', None, fmt='{}_b'),
            se.simple_select_query({
                'source': 'table_12345',
                'source_columns': self.source_columns,
                'target_columns': [self.target_column],
            }, 'schema', None, {}),
        )

class TestApplyOutputFilter(TestSQLExpression):
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
        from_clause = curry(se.get_from_clause, [table], source_column_configs=[source_columns])
        target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}
        select = sqlalchemy.select(from_clause(target_column))
        result = select.subquery('result')
        self.assertEquivalent(
            se.apply_output_filter(select, 'result.TargetColumn != 0', {}),
            sqlalchemy.select(*result.columns).where(result.c.TargetColumn != 0)
        )

class TestGetInsertQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.source_table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )
        self.from_clause = curry(se.get_from_clause, [self.source_table], source_column_configs=[self.source_columns])
        self.target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}

    def test_basic_use_case(self):
        target_table_columns = [
            {'source': 'TargetColumn', 'dtype': 'text'}
        ]
        target_table = se.get_table_rep(
            'table_54321',
            target_table_columns,
            'anlz_schema',
        )
        select = sqlalchemy.select(self.from_clause(self.target_column))
        self.assertEquivalent(
            se.get_insert_query(target_table, [self.target_column], select),
            target_table.insert().from_select(['TargetColumn'], select)
        )

    def test_serial(self):
        # Don't include serial columns
        serial_column = {'target': 'RowNumber', 'dtype': 'serial'}
        serial_target_table_columns = [{'source': 'TargetColumn', 'dtype': 'text'}, {'source': 'RowNumber', 'dtype': 'serial'}]
        serial_target_table = se.get_table_rep(
            'table_54321',
            serial_target_table_columns,
            'anlz_schema',
        )
        serial_select = sqlalchemy.select(self.from_clause(self.target_column), self.from_clause(serial_column))
        self.assertEquivalent(
            serial_target_table.insert().from_select(['TargetColumn'], serial_select),
            se.get_insert_query(
                serial_target_table,
                [self.target_column, serial_column],
                serial_select,
                use_row_number_for_serial=False,
            ),
        )

    def test_serial_row_number(self):
        # Include serial columns as row_number
        serial_column = {'target': 'RowNumber', 'dtype': 'serial'}
        serial_target_table_columns = [{'source': 'TargetColumn', 'dtype': 'text'}, {'source': 'RowNumber', 'dtype': 'serial'}]
        serial_target_table = se.get_table_rep(
            'table_54321',
            serial_target_table_columns,
            'anlz_schema',
        )
        serial_select = sqlalchemy.select(self.from_clause(self.target_column), self.from_clause(serial_column))
        self.assertEquivalent(
            serial_target_table.insert().from_select(['TargetColumn', 'RowNumber'], serial_select),
            se.get_insert_query(
                serial_target_table,
                [self.target_column, serial_column],
                serial_select,
                use_row_number_for_serial=True,
            ),
        )


class TestGetDeleteQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.source_table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )

    def test_delete_no_where(self):
        # If no where clause, delete everything
        self.assertEquivalent(
            se.get_delete_query(self.source_table, []),
            sqlalchemy.delete(self.source_table),
        )

    def test_delete_where(self):
        # if there's a where clause, use it
        self.assertEquivalent(
            se.get_delete_query(self.source_table, ['table.Column1 == "foobar"']),
            sqlalchemy.delete(self.source_table).where(self.source_table.c.Column1 == 'foobar'),
        )

# these tests error in v1.0.0, because import_data_query is not a true function and generates a random uuid for the temp table name. For testing purposes, I've added the ability to pass in a temp table name.
class TestImportDataQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns = [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.target_column = {'target': 'TargetColumn', 'source': 'Column1', 'dtype': 'text'}
        self.target_table_columns = [{'source': 'TargetColumn', 'dtype': 'text'}]
        self.target_table = se.get_table_rep(
            'table_54321',
            self.target_table_columns,
            'anlz_schema',
        )
        self.expected_temp_table_columns = [[
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'text'},
            {'source': 'Column3', 'dtype': 'text'},
            {'source': ':::DOCUMENT_PATH:::', 'dtype': 'path'},
            {'source': ':::FILE_NAME:::', 'dtype': 'file_name'},
            {'source': ':::TAB_NAME:::', 'dtype': 'tab_name'},
            {'source': ':::LAST_MODIFIED:::', 'dtype': 'last_modified'},
        ]]
        self.expected_temp_table = se.get_table_rep(
            'temp_table',
            self.expected_temp_table_columns[0],
            'anlz_schema',
            alias='text_import',
        )

    def test_basic_use_case(self):
        expected_target_column = {
            'target': 'TargetColumn',
            'source': 'Column1',
            'dtype': 'text',
            'expression': """func.import_col(get_column(table, 'Column1'), 'text', '', False)""",
        }
        self.assertEquivalent(
            se.import_data_query(
                '_schema',
                'table_54321',
                self.source_columns,
                [self.target_column],
                temp_table_id='temp_table',
            ),
            se.get_insert_query(
                self.target_table,
                [expected_target_column],
                se.get_select_query(
                    [self.expected_temp_table],
                    self.expected_temp_table_columns,
                    [expected_target_column],
                    [],
                ),
            ),
        )

    def test_trailing_negatives(self):
        # trailing_negatives
        expected_target_column_tn = {
            'target': 'TargetColumn',
            'source': 'Column1',
            'dtype': 'text',
            'expression': """func.import_col(get_column(table, 'Column1'), 'text', '', True)""",
        }
        self.assertEquivalent(
            se.import_data_query(
                '_schema',
                'table_54321',
                self.source_columns,
                [self.target_column],
                trailing_negatives=True,
                temp_table_id='temp_table',
            ),
            se.get_insert_query(
                self.target_table,
                [expected_target_column_tn],
                se.get_select_query(
                    [self.expected_temp_table],
                    self.expected_temp_table_columns,
                    [expected_target_column_tn],
                    [],
                ),
            ),
        )

    def test_date_format(self):
        # date_format
        expected_target_column_df = {
            'target': 'TargetColumn',
            'source': 'Column1',
            'dtype': 'text',
            'expression': """func.import_col(get_column(table, 'Column1'), 'text', 'YYYYMMDD', False)""",
        }
        self.assertEquivalent(
            se.import_data_query(
                '_schema',
                'table_54321',
                self.source_columns,
                [self.target_column],
                date_format='YYYYMMDD',
                temp_table_id='temp_table',
            ),
            se.get_insert_query(
                self.target_table,
                [expected_target_column_df],
                se.get_select_query(
                    [self.expected_temp_table],
                    self.expected_temp_table_columns,
                    [expected_target_column_df],
                    [],
                ),
            ),
        )

    def test_magic_columns(self):
        # magic columns
        magic_target_columns = [
            {'target': 'Path', 'dtype': 'path'},
            {'target': 'FileName', 'dtype': 'file_name'},
            {'target': 'TabName', 'dtype': 'tab_name'},
            {'target': 'LastModified', 'dtype': 'last_modified'},
        ]
        magic_target_table_columns = [
            {'source': 'Path', 'dtype': 'path'},
            {'source': 'FileName', 'dtype': 'file_name'},
            {'source': 'TabName', 'dtype': 'tab_name'},
            {'source': 'LastModified', 'dtype': 'last_modified'},
        ]
        magic_target_table = se.get_table_rep(
            'table_54321',
            magic_target_table_columns,
            'anlz_schema',
        )
        magic_expected_target_columns = [
            {
                'target': 'Path',
                'source': ':::DOCUMENT_PATH:::',
                'dtype': 'path',
                'expression': """func.import_col(get_column(table, ':::DOCUMENT_PATH:::'), 'path', '', False)""",
            },
            {
                'target': 'FileName',
                'source': ':::FILE_NAME:::',
                'dtype': 'file_name',
                'expression': """func.import_col(get_column(table, ':::FILE_NAME:::'), 'file_name', '', False)""",
            },
            {
                'target': 'TabName',
                'source': ':::TAB_NAME:::',
                'dtype': 'tab_name',
                'expression': """func.import_col(get_column(table, ':::TAB_NAME:::'), 'tab_name', '', False)""",
            },
            {
                'target': 'LastModified',
                'source': ':::LAST_MODIFIED:::',
                'dtype': 'last_modified',
                'expression': """func.import_col(get_column(table, ':::LAST_MODIFIED:::'), 'last_modified', '', False)""",
            },
        ]

        self.assertEquivalent(
            se.import_data_query(
                '_schema',
                'table_54321',
                self.source_columns,
                magic_target_columns,
                temp_table_id='temp_table',
            ),
            se.get_insert_query(
                magic_target_table,
                magic_expected_target_columns,
                se.get_select_query(
                    [self.expected_temp_table],
                    self.expected_temp_table_columns,
                    magic_expected_target_columns,
                    []
                ),
            ),
        )

# This function doesn't exist in v1.0.0. It's used to implement another function more clearly, and test more cleanly
class TestGetUpdateValue(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )
        self.dtype_map = {
            sc['source']: sc['dtype']
            for sc in self.source_columns
        }

    def test_nullify(self):
        null_target_col = {'source': 'Column1', 'nullify': 'True'}
        self.assertEqual(
            se.get_update_value(null_target_col, self.table, self.dtype_map, {}),
            (True, None)
        )

    def test_expression(self):
        expression_col = {'source': 'Column1', 'expression': '"foobar"'}
        self.assertEqual(
            se.get_update_value(expression_col, self.table, self.dtype_map, {}),
            (True, 'foobar')
        )

    def test_constant(self):
        constant_col = {'source': 'Column2', 'constant': '5'}
        include, value = se.get_update_value(constant_col, self.table, self.dtype_map, {})
        self.assertTrue(include)
        self.assertEquivalent(
            value,
            sqlalchemy.literal('5', type_=sqlalchemy.NUMERIC)
        )

    def test_expression_none_returns_empty_string_for_text(self):
        empty_string_col = {'source': 'Column1', 'expression': 'None'}
        self.assertEqual(
            se.get_update_value(empty_string_col, self.table, self.dtype_map, {}),
            (True, u''),
        )

    def test_include_because_text(self):
        include_because_text_col = {'source': 'Column1'}
        self.assertEqual(
            se.get_update_value(include_because_text_col, self.table, self.dtype_map, {}),
            (True, u'')
        )

    def test_dont_include(self):
        dont_include_col = {'source': 'Column2'}
        # We don't care about the value, only about include
        self.assertFalse(se.get_update_value(dont_include_col, self.table, self.dtype_map, {})[0])

class TestGetUpdateQuery(TestSQLExpression):
    def setUp(self):
        self.source_columns =  [
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
            {'source': 'Column3', 'dtype': 'numeric'},
        ]
        self.table = se.get_table_rep(
            'table_12345',
            self.source_columns,
            'anlz_schema',
        )
        self.dtype_map = {
            sc['source']: sc['dtype']
            for sc in self.source_columns
        }
        self.target_columns = [
            {'source': 'Column1', 'nullify': True},
            {'source': 'Column2', 'expression': '2'},
            {'source': 'Column3'},
        ]

    def test_basic_use_case(self):
        self.assertEquivalent(
            se.get_update_query(self.table, self.target_columns, [], self.dtype_map),
            sqlalchemy.update(self.table).values({'Column1': None, 'Column2': 2}),
        )

    def test_wheres(self):
        self.assertEquivalent(
            se.get_update_query(self.table, self.target_columns, ['table.Column1 == "foobar"'], self.dtype_map),
            sqlalchemy.update(self.table).where(self.table.c.Column1 == 'foobar').values({'Column1': None, 'Column2': 2}),
        )

    def test_empty_string(self):
        # weird empty string stuff
        # This one makes sense to me
        empty_string_col = {'source': 'Column1', 'expression': 'None'}
        self.assertEquivalent(
            se.get_update_query(self.table, [empty_string_col], [], self.dtype_map),
            sqlalchemy.update(self.table).values({'Column1': u''})
        )

    def test_empty_string_no_matter_what(self):
        # It works this way because it's impossible to type 'constant': '' in the UI
        include_because_text_col = {'source': 'Column1'}
        self.assertEquivalent(
            se.get_update_query(self.table, [include_because_text_col], [], self.dtype_map),
            sqlalchemy.update(self.table).values({'Column1': u''})
        )


if __name__ == '__main__':
    unittest.main()
