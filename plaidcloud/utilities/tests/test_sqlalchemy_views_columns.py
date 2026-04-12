# coding=utf-8
"""Coverage for CreateView column-list branches on generic and starrocks dialects."""

import unittest

import sqlalchemy as sa

from plaidcloud.utilities import sqlalchemy_views as vw


class TestCreateViewWithColumns(unittest.TestCase):

    def setUp(self):
        self.gp = sa.create_engine('greenplum://127.0.0.1/')
        self.sr = sa.create_engine('starrocks://127.0.0.1/')

        metadata = sa.MetaData()
        self.view_obj = sa.Table(
            'article-vw', metadata,
            sa.Column('id', sa.Integer),
            sa.Column('name', sa.String),
            schema='public',
        )
        self.selectable = sa.select(sa.literal(1).label('id'), sa.literal('x').label('name'))

    def test_create_view_renders_column_list_greenplum(self):
        expr = vw.CreateView(self.view_obj, selectable=self.selectable)
        compiled = expr.compile(
            dialect=self.gp.dialect,
            compile_kwargs={'render_postcompile': True},
        )
        rendered = str(compiled)
        # Expect explicit (id, name) list in the DDL
        self.assertIn('(id, name)', rendered)

    def test_create_view_renders_column_list_starrocks(self):
        # materialized=False retains the column list on starrocks too.
        expr = vw.CreateView(
            self.view_obj, selectable=self.selectable, materialized=False,
        )
        compiled = expr.compile(
            dialect=self.sr.dialect,
            compile_kwargs={'render_postcompile': True},
        )
        rendered = str(compiled)
        self.assertIn('(id, name)', rendered)

    def test_create_materialized_view_starrocks_skips_column_list(self):
        # StarRocks materialized views omit the explicit column list even
        # when the underlying table has columns.
        expr = vw.CreateView(
            self.view_obj, selectable=self.selectable, materialized=True,
        )
        compiled = expr.compile(
            dialect=self.sr.dialect,
            compile_kwargs={'render_postcompile': True},
        )
        rendered = str(compiled)
        self.assertNotIn('(id, name)', rendered)


if __name__ == '__main__':
    unittest.main()
