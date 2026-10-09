"""Emitted SQL for the executor options the KNIME converter needs (epic 32079, Phase 6)."""
import decimal
import unittest

import duckdb
import sqlalchemy

from plaidcloud.utilities import sql_expression as se

DIALECTS = ('databend', 'starrocks')


def sql(statement, dialect):
    return str(statement.compile(dialect=sqlalchemy.dialects.registry.load(dialect)(), compile_kwargs={'literal_binds': True}))


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


class TestNaturalSort(unittest.TestCase):
    source_columns = [{'source': 's', 'dtype': 'text'}]
    table = sqlalchemy.Table('t', sqlalchemy.MetaData(), sqlalchemy.Column('s', sqlalchemy.Text))

    def order_by(self, sort, dialect):
        table = se.get_table_rep('table_1', self.source_columns, 'anlz')
        target = {'source': 's', 'target': 's', 'dtype': 'text', 'sort': sort}
        return sql(se.get_select_query([table], [self.source_columns], [target], []), dialect).split('ORDER BY ')[1]

    def test_natural_sorts_on_the_key_in_both_directions_with_nulls(self):
        for dialect in DIALECTS:
            for sort, tail in [
                ({'ascending': True, 'natural': True}, "ASC"),
                ({'ascending': False, 'natural': True, 'nulls': 'first'}, "DESC NULLS FIRST"),
            ]:
                with self.subTest(dialect=dialect, sort=sort):
                    ordering = self.order_by(sort, dialect)
                    self.assertTrue(ordering.startswith('regexp_replace(regexp_replace('))
                    self.assertTrue(ordering.endswith(tail))

    def test_plain_sort_has_no_key(self):
        self.assertNotIn('regexp_replace', self.order_by({'ascending': True}, 'databend'))

    def test_starrocks_backreference_is_re2_and_databend_is_rust(self):
        self.assertIn("'$1'", self.order_by({'ascending': True, 'natural': True}, 'databend'))
        self.assertIn("'\\\\1'", self.order_by({'ascending': True, 'natural': True}, 'starrocks'))

    def test_order_of_values_with_the_re2_flavour(self):
        # DuckDB speaks RE2 like StarRocks, so the StarRocks parameters run as they are.
        params = list(sqlalchemy.func.natural_sort_key(self.table.c.s).compile(
            dialect=sqlalchemy.create_engine('starrocks://127.0.0.1/').dialect).params.values())
        connection = duckdb.connect()
        connection.execute('CREATE TABLE t (s VARCHAR)')
        values = ['a10', 'a2', 'a1', 'b1', 'a02x', 'a2x', 'file12.txt', 'file9.txt', 'file100.txt', '10', '9', 'a', '']
        connection.executemany('INSERT INTO t VALUES (?)', [(v,) for v in values])
        ordered = [row[0] for row in connection.execute(
            "SELECT s FROM t ORDER BY regexp_replace(regexp_replace(s, ?, ?, 'g'), ?, ?, 'g'), s", params).fetchall()]
        self.assertEqual(
            ['', '9', '10', 'a', 'a1', 'a2', 'a02x', 'a2x', 'a10', 'b1', 'file9.txt', 'file12.txt', 'file100.txt'], ordered)


class TestOtherDialects(unittest.TestCase):
    text = sqlalchemy.Table('t', sqlalchemy.MetaData(), sqlalchemy.Column('s', sqlalchemy.Text))
    number = sqlalchemy.Table('n', sqlalchemy.MetaData(), sqlalchemy.Column('i', sqlalchemy.Integer), sqlalchemy.Column('f', sqlalchemy.Float))

    def key_sql(self, dialect):
        return sql(sqlalchemy.func.natural_sort_key(self.text.c.s), dialect)

    def test_natural_key_spelling_per_dialect(self):
        for dialect, repl, flag in [
            ('databend', "'$1'", False), ('databricks', "'$1'", False),
            ('starrocks', "'\\\\1'", False), ('snowflake', "'\\\\1'", False),
            ('postgresql', "'\\\\1'", True), ('duckdb', "'\\\\1'", True),
        ]:
            with self.subTest(dialect=dialect):
                rendered = self.key_sql(dialect)
                self.assertIn(repl, rendered)
                self.assertEqual(flag, "'g')" in rendered)

    def test_natural_key_refuses_unverified_dialects(self):
        with self.assertRaisesRegex(sqlalchemy.exc.CompileError, 'mssql'):
            self.key_sql('mssql')

    def test_natural_key_orders_on_duckdb(self):
        engine = sqlalchemy.create_engine('duckdb:///:memory:')
        with engine.begin() as connection:
            connection.exec_driver_sql('CREATE TABLE t (s VARCHAR)')
            connection.exec_driver_sql("INSERT INTO t VALUES ('a10'), ('a2'), ('a1'), ('b1')")
            ordered = [row[0] for row in connection.execute(
                sqlalchemy.select(self.text.c.s).order_by(sqlalchemy.func.natural_sort_key(self.text.c.s)))]
        self.assertEqual(['a1', 'a2', 'a10', 'b1'], ordered)

    def test_round_half_even_on_duckdb_for_int_and_float(self):
        engine = sqlalchemy.create_engine('duckdb:///:memory:')
        with engine.begin() as connection:
            connection.exec_driver_sql('CREATE TABLE n (i INTEGER, f DOUBLE)')
            connection.exec_driver_sql('INSERT INTO n VALUES (25, 2.5), (35, 3.5), (15, 0.125), (-25, -2.5)')
            rows = connection.execute(sqlalchemy.select(
                sqlalchemy.func.round_half_even(self.number.c.i, -1),
                sqlalchemy.func.round_half_even(self.number.c.f),
                sqlalchemy.func.round_half_even(self.number.c.f, 2),
                sqlalchemy.func.round_half_even(self.number.c.i, 1),
            ).select_from(self.number)).fetchall()
        self.assertEqual([(20, 2, 2.5, 25), (40, 4, 3.5, 35), (20, 0, 0.12, 15), (-20, -2, -2.5, -25)],
                         [tuple(float(v) for v in row) for row in rows])

    def test_nulls_first_refused_on_mssql_only_when_asked(self):
        column = self.text.c.s
        with self.assertRaises(sqlalchemy.exc.CompileError):
            sql(sqlalchemy.select(column).order_by(sqlalchemy.nulls_first(sqlalchemy.asc(column))), 'mssql')
        self.assertIn('ORDER BY t.s ASC', sql(sqlalchemy.select(column).order_by(sqlalchemy.asc(column)), 'mssql'))


if __name__ == '__main__':
    unittest.main()
