import logging
import re
import unittest
from unittest.mock import MagicMock, patch

import sqlalchemy

from plaidcloud.utilities import panel_data


def make_table(conn=None, **filters):
    meta = sqlalchemy.MetaData()
    sa = sqlalchemy.Table('t', meta, sqlalchemy.Column('region', sqlalchemy.Text),
                          sqlalchemy.Column('id', sqlalchemy.Integer), sqlalchemy.Column('amount', sqlalchemy.Integer))
    conn = conn or MagicMock()
    conn.get_table.return_value = sa
    conn.execute.return_value = [{'n': 7}]
    with patch.dict('os.environ', {panel_data.PROJECT_ENV: 'p'}):
        return panel_data.table('t', conn=conn).filter(**filters), conn


def sql(conn):
    stmt = conn.execute.call_args.args[0]
    return str(stmt.compile(compile_kwargs={'literal_binds': True}))


class TestPage(unittest.TestCase):
    def test_page_two_orders_by_sort_then_every_column_and_limits(self):
        t, conn = make_table()
        t.page(2, 50, sort=['-amount'])
        s = sql(conn)
        self.assertIn('ORDER BY t.amount DESC, t.region, t.id', s)
        self.assertIn('LIMIT 50', s)
        self.assertIn('OFFSET 50', s)

    def test_explicit_key_is_the_tie_break_and_not_repeated(self):
        t, conn = make_table()
        t.page(1, 200, sort=['id'], key='id')
        self.assertIn('ORDER BY t.id\n LIMIT 200 OFFSET 0', sql(conn))

    def test_limit_is_always_set(self):
        t, conn = make_table()
        t.page()
        self.assertIn('LIMIT 200', sql(conn))
        with self.assertRaises(ValueError):
            t.page(size=0)
        with self.assertRaises(ValueError):
            t.page(n=0)

    def test_unknown_sort_column_is_rejected(self):
        t, _ = make_table()
        with self.assertRaises(ValueError):
            t.page(sort=['nope'])


class TestFilter(unittest.TestCase):
    def test_equality_and_in_and_blank_values(self):
        t, conn = make_table(region=['E', 'W'], id=3, amount=None)
        t.page()
        s = sql(conn)
        self.assertIn("t.region IN ('E', 'W')", s)
        self.assertIn('t.id = 3', s)
        self.assertNotIn('amount =', s)
        self.assertNotIn('amount IS', s)

    def test_filter_is_immutable(self):
        t, conn = make_table()
        t.filter(id=1)
        t.page()
        self.assertNotIn('WHERE', sql(conn))


class TestCountAndAggregate(unittest.TestCase):
    def test_count_applies_filters(self):
        t, conn = make_table(region='E')
        self.assertEqual(t.count(), 7)
        self.assertIn("count(*) AS n", sql(conn))
        self.assertIn("WHERE t.region = 'E'", sql(conn))

    def test_grouped_aggregate(self):
        t, conn = make_table(id=1)
        t.aggregate(by=['region'], total=('amount', 'sum'), rows=('id', 'count'))
        s = sql(conn)
        self.assertIn('sum(t.amount) AS total', s)
        self.assertIn('count(t.id) AS rows', s)
        self.assertIn('GROUP BY t.region', s)
        self.assertIn('ORDER BY t.region', s)
        self.assertIn('LIMIT 1000', s)

    def test_totals_row_has_no_group_by_and_still_a_limit(self):
        t, conn = make_table()
        t.aggregate(total=('amount', 'sum'))
        s = sql(conn)
        self.assertNotIn('GROUP BY', s)
        self.assertIn('LIMIT 1000', s)


class TestProject(unittest.TestCase):
    def _connect(self, env, **kwargs):
        with patch.dict('os.environ', env, clear=True), \
                patch('plaidcloud.utilities.connect.PlaidConnection') as pc:
            panel_data.table('t', **kwargs)
        return pc

    def test_env_project_is_used(self):
        pc = self._connect({panel_data.PROJECT_ENV: 'env-proj'})
        pc.assert_called_once_with(project_id='env-proj')

    def test_explicit_project_overrides_env(self):
        pc = self._connect({panel_data.PROJECT_ENV: 'env-proj'}, project='mine')
        pc.assert_called_once_with(project_id='mine')

    def test_missing_env_warns_naming_the_variable(self):
        with self.assertLogs(panel_data.logger, logging.WARNING) as logs:
            pc = self._connect({})
        self.assertIn('PLAID_PANEL_PROJECT_ID', logs.output[0])
        pc.assert_called_once_with()


class TestInputs(unittest.TestCase):
    def test_unknown_aggregate_names_the_allowed_set(self):
        t, _ = make_table()
        with self.assertRaisesRegex(ValueError, "'median'.*'avg'.*'sum'"):
            t.aggregate(x=('amount', 'median'))

    def test_aggregate_without_measures_is_rejected(self):
        t, conn = make_table()
        with self.assertRaises(ValueError):
            t.aggregate(by=['region'])
        conn.execute.assert_not_called()

    def test_frozenset_filters_as_in(self):
        t, conn = make_table(id=frozenset({4}))
        t.page()
        self.assertIn('t.id IN (4)', sql(conn))

    def test_string_sort_is_one_column(self):
        t, conn = make_table()
        t.page(sort='amount')
        self.assertIn('ORDER BY t.amount, t.region, t.id', sql(conn))


class TestOnRealSql(unittest.TestCase):
    """Compile through a real query.Connection and execute on sqlite, so the SQL is proven, not just shaped."""

    def setUp(self):
        from plaidcloud.utilities.query import Connection
        engine = sqlalchemy.create_engine('sqlite://')
        with engine.begin() as c:
            c.execute(sqlalchemy.text('create table t (region text, id int, amount int)'))
            c.execute(sqlalchemy.text('insert into t values (:r, :i, :a)'),
                      [dict(r='East' if i % 2 else 'West', i=i, a=i % 3) for i in range(1, 11)])
        rpc = MagicMock()
        rpc.analyze.project.lookup_by_name.return_value = 'proj'
        rpc.analyze.query.dialect.return_value = 'postgresql'

        def query(project_id, query, params):
            with engine.connect() as c:
                result = c.execute(sqlalchemy.text(re.sub(r'%\((\w+)\)s', r':\1', query)), params)
                return [dict(r._mapping) for r in result]

        rpc.analyze.query.query.side_effect = query
        with patch('plaidcloud.utilities.query.Dimensions'):
            conn = Connection(project='proj', rpc=rpc)
        sa = sqlalchemy.Table('t', sqlalchemy.MetaData(), sqlalchemy.Column('region', sqlalchemy.Text),
                              sqlalchemy.Column('id', sqlalchemy.Integer), sqlalchemy.Column('amount', sqlalchemy.Integer))
        self.t = panel_data.Table(conn, sa)

    def test_unfiltered_count_is_the_row_count(self):
        self.assertEqual(self.t.count(), 10)
        self.assertEqual(self.t.filter(region='East').count(), 5)

    def test_page_returns_real_string_values_in_stable_pages(self):
        first = self.t.page(1, 4, sort=['-amount'])
        self.assertEqual(set(first['region']) - {'East', 'West'}, set())
        second = self.t.page(2, 4, sort=['-amount'])
        self.assertEqual(len(set(first['id']) | set(second['id'])), 8)

    def test_grouped_aggregate_and_totals_equal_direct_sql(self):
        by_region = self.t.aggregate(by=['region'], rows=('id', 'count'), total=('amount', 'sum'))
        self.assertEqual(dict(zip(by_region['region'], by_region['rows'])), {'East': 5, 'West': 5})
        self.assertEqual(int(self.t.aggregate(total=('amount', 'sum'))['total'][0]), sum(i % 3 for i in range(1, 11)))
