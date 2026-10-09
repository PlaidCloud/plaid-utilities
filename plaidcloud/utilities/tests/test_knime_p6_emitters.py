"""Emitted SQL for the executor options the KNIME converter needs (epic 32079, Phase 6)."""
import unittest

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


if __name__ == '__main__':
    unittest.main()
