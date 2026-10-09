"""Emitted SQL for the executor options the KNIME converter needs (epic 32079, Phase 6)."""
import decimal
import unittest

import duckdb
import sqlalchemy

from plaidcloud.utilities import sql_expression as se

DIALECTS = ('databend', 'starrocks')


def sql(statement, dialect):
    engine = sqlalchemy.create_engine(f'{dialect}://127.0.0.1/')
    return str(statement.compile(dialect=engine.dialect, compile_kwargs={'literal_binds': True}))


class TestSortNulls(unittest.TestCase):
    source_columns = [{'source': 'v', 'dtype': 'integer'}]

    def order_by(self, sort, dialect):
        table = se.get_table_rep('table_1', self.source_columns, 'anlz')
        target = {'source': 'v', 'target': 'v', 'dtype': 'integer', 'sort': sort}
        statement = se.get_select_query([table], [self.source_columns], [target], [])
        return sql(statement, dialect).split('ORDER BY ')[1]

    def test_nulls_placement(self):
        for dialect in DIALECTS:
            for sort, tail in [
                ({'ascending': True, 'nulls': 'first'}, 'ASC NULLS FIRST'),
                ({'ascending': True, 'nulls': 'last'}, 'ASC NULLS LAST'),
                ({'ascending': False, 'nulls': 'first'}, 'DESC NULLS FIRST'),
                ({'ascending': False, 'nulls': 'last'}, 'DESC NULLS LAST'),
            ]:
                with self.subTest(dialect=dialect, sort=sort):
                    self.assertTrue(self.order_by(sort, dialect).endswith(tail))

    def test_default_placement_is_unchanged(self):
        for dialect in DIALECTS:
            for sort, tail in [({'ascending': True}, 'ASC'), ({'ascending': False}, 'DESC')]:
                with self.subTest(dialect=dialect, sort=sort):
                    self.assertTrue(self.order_by(sort, dialect).endswith(tail))
                    self.assertNotIn('NULLS', self.order_by(sort, dialect))


class TestOrderedStringAgg(unittest.TestCase):
    source_columns = [{'source': 'v', 'dtype': 'integer'}, {'source': 's', 'dtype': 'text'}]

    def aggregate(self, expression, dialect):
        table = se.get_table_rep('table_1', self.source_columns, 'anlz')
        targets = [
            {'target': 'k', 'source': 'v', 'dtype': 'integer', 'agg': 'group'},
            {'target': 'c', 'expression': expression, 'dtype': 'text', 'agg': 'dont_group'},
        ]
        statement = se.get_select_query([table], [self.source_columns], targets, [], aggregate=True)
        return sql(statement, dialect)

    def test_ordered_databend_uses_within_group(self):
        rendered = self.aggregate("func.string_agg(table.s, ', ').within_group(table.v)", 'databend')
        self.assertIn("string_agg(anlz.table_1.s, ', ') WITHIN GROUP (ORDER BY anlz.table_1.v)", rendered)

    def test_ordered_starrocks_uses_group_concat_order_by(self):
        rendered = self.aggregate("func.string_agg(table.s, ', ').within_group(table.v)", 'starrocks')
        self.assertIn("group_concat(anlz.table_1.s ORDER BY anlz.table_1.v SEPARATOR ', ')", rendered)
        self.assertNotIn('WITHIN GROUP', rendered)

    def test_ordered_starrocks_descending_and_multiple_keys(self):
        rendered = self.aggregate("func.string_agg(table.s, '|').within_group(table.v.desc(), table.s)", 'starrocks')
        self.assertIn("group_concat(anlz.table_1.s ORDER BY anlz.table_1.v DESC, anlz.table_1.s SEPARATOR '|')", rendered)

    def test_unordered_starrocks_is_unchanged(self):
        rendered = self.aggregate("func.string_agg(table.s, ', ')", 'starrocks')
        self.assertIn("group_concat(anlz.table_1.s SEPARATOR ', ')", rendered)


class TestRoundHalfEven(unittest.TestCase):
    table = sqlalchemy.Table('t', sqlalchemy.MetaData(), sqlalchemy.Column('v', sqlalchemy.Numeric(38, 6)))

    def test_no_decimal_38_10_cast_and_no_round_on_either_dialect(self):
        for dialect in DIALECTS:
            with self.subTest(dialect=dialect):
                rendered = sql(sqlalchemy.func.round_half_even(self.table.c.v, 2), dialect)
                self.assertIn('floor(t.v * 100)', rendered)
                self.assertNotIn('CAST', rendered)
                self.assertNotIn('round(', rendered)

    def test_digits_must_be_literal(self):
        with self.assertRaises(sqlalchemy.exc.CompileError):
            sql(sqlalchemy.func.round_half_even(self.table.c.v, self.table.c.v), 'databend')

    def test_values(self):
        connection = duckdb.connect()
        connection.execute('CREATE TABLE t (v DECIMAL(38,6))')
        cases = [
            # (value, digits, expected)
            ('0.5', 0, '0'), ('1.5', 0, '2'), ('2.5', 0, '2'), ('3.5', 0, '4'), ('-0.5', 0, '0'), ('-1.5', 0, '-2'),
            ('-2.5', 0, '-2'), ('2.4', 0, '2'), ('2.6', 0, '3'), ('-2.6', 0, '-3'),
            ('1.005', 2, '1.00'), ('2.675', 2, '2.68'), ('0.125', 2, '0.12'), ('0.135', 2, '0.14'), ('-0.125', 2, '-0.12'),
            ('25', -1, '20'), ('35', -1, '40'), ('15', -1, '20'), ('-25', -1, '-20'),
            ('12345678901234567890.5', 0, '12345678901234567890'), ('12345678901234567891.5', 0, '12345678901234567892'),
            ('0.123125', 5, '0.12312'),
        ]
        for value, digits, expected in cases:
            with self.subTest(value=value, digits=digits):
                connection.execute('DELETE FROM t')
                connection.execute(f'INSERT INTO t VALUES ({value})')
                expression = sqlalchemy.func.round_half_even(self.table.c.v, digits)
                (result,), = connection.execute(
                    'SELECT ' + sql(expression, 'postgresql') + ' FROM t').fetchall()
                self.assertEqual(decimal.Decimal(expected), decimal.Decimal(str(result)))


if __name__ == '__main__':
    unittest.main()
