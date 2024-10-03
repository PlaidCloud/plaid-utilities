# coding=utf-8
import unittest
# import pytest

import sqlalchemy as sa
from sqlalchemy.orm import declarative_base
from plaidcloud.utilities import sqlalchemy_views as vw


__author__ = "Pat Buxton"
__copyright__ = "© Copyright 2024-2024, Tartan Solutions, Inc"
__credits__ = ["Pat Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Pat Buxton"
__email__ = "patrick.buxton@tartansolutions.com"

Base = declarative_base()



class User(Base):
    __tablename__ = 'user'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)

class Article(Base):
    __tablename__ = 'article'
    id = sa.Column(sa.Integer, primary_key=True)
    name = sa.Column(sa.String)
    author_id = sa.Column(sa.Integer, sa.ForeignKey(User.id))
    author = sa.orm.relationship(User)


class BaseTest(unittest.TestCase):

    dialect = 'greenplum'

    def setUp(self) -> None:
        self.eng = sa.create_engine(f'{self.dialect}://127.0.0.1/')


class StarRocksTest(unittest.TestCase):

    dialect = 'starrocks'

    def setUp(self) -> None:
        self.eng = sa.create_engine(f'{self.dialect}://127.0.0.1/')


class TestDropView(BaseTest):

    def test_drop_default(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW public."article-vw"', str(compiled))

    def test_drop_cascade(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            cascade=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW public."article-vw" CASCADE', str(compiled))

    def test_drop_if_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            if_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW IF EXISTS public."article-vw"', str(compiled))

class TestCreateView(BaseTest):

    def test_create_comment(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE VIEW public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))

    def test_create_no_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE VIEW public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))

    def test_create_with_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=True
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE VIEW public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))


    def test_create_with_replace_if_not_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=True,
            if_not_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE VIEW IF NOT EXISTS public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))

    def test_create_with_options(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            options={
                'option1': 'opt'
            }
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE VIEW public."article-vw" WITH (option1=opt) AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))


class TestDropMaterializedView(BaseTest):

    def test_drop_default(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW public."article-vw"', str(compiled))

    def test_drop_cascade(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
            cascade=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW public."article-vw" CASCADE', str(compiled))

    def test_drop_if_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
            if_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW IF EXISTS public."article-vw"', str(compiled))


class TestCreateMaterializedView(BaseTest):

    def test_create_no_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE MATERIALIZED VIEW public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))

    def test_create_with_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=True
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE MATERIALIZED VIEW public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))


    def test_create_with_replace_if_not_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=True,
            if_not_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE MATERIALIZED VIEW IF NOT EXISTS public."article-vw" AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))

    def test_create_with_options(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            options={
                'option1': 'opt'
            }
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE MATERIALIZED VIEW public."article-vw" WITH (option1=opt) AS SELECT article.id, article.name, '
         '"user".id AS author_id, "user".name AS author_name \n'
         'FROM article JOIN "user" ON article.author_id = "user".id\n'
         '\n'), str(compiled))


class TestDropViewStarRocks(StarRocksTest):

    def test_drop_default(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW public.`article-vw`', str(compiled))

    # N.B. No cascade on Starrocks
    def test_drop_cascade(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            cascade=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW public.`article-vw`', str(compiled))

    def test_drop_if_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            if_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP VIEW IF EXISTS public.`article-vw`', str(compiled))

class TestCreateViewStarRocks(StarRocksTest):

    def test_create_comment(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
            'CREATE VIEW public.`article-vw` \n'
            "COMMENT 'blah'\n"
            'AS SELECT article.id, article.name, user.id AS author_id, user.name AS '
            'author_name \n'
            'FROM article INNER JOIN user ON article.author_id = user.id\n'
            '\n'), str(compiled))

    def test_create_no_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE VIEW public.`article-vw` AS SELECT article.id, article.name, user.id '
         'AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=True
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE VIEW public.`article-vw` AS SELECT article.id, '
         'article.name, user.id AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_replace_if_not_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            or_replace=True,
            if_not_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE OR REPLACE VIEW IF NOT EXISTS public.`article-vw` AS SELECT '
         'article.id, article.name, user.id AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_options(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            options={
                'option1': 'opt'
            }
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE VIEW public.`article-vw` AS SELECT article.id, article.name, user.id '
         'AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))



class TestDropMaterializedViewStarRocks(StarRocksTest):

    def test_drop_default(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW public.`article-vw`', str(compiled))

    # N.B. No cascade on Starrocks
    def test_drop_cascade(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
            cascade=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW public.`article-vw`', str(compiled))

    def test_drop_if_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.DropView(
            view_obj,
            materialized=True,
            if_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual('\nDROP MATERIALIZED VIEW IF EXISTS public.`article-vw`', str(compiled))

class TestCreateMaterializedViewStarRocks(StarRocksTest):

    def test_create_comment(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', comment='blah')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
            'CREATE MATERIALIZED VIEW public.`article-vw` \n'
            "COMMENT 'blah'\n"
            'AS SELECT article.id, article.name, user.id AS author_id, user.name AS '
            'author_name \n'
            'FROM article INNER JOIN user ON article.author_id = user.id\n'
            '\n'), str(compiled))

    def test_create_refresh(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public', starrocks_refresh='IMMEDIATE ASYNC')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
            'CREATE MATERIALIZED VIEW public.`article-vw` \n'
            'REFRESH IMMEDIATE ASYNC\n'
            'AS SELECT article.id, article.name, user.id AS author_id, user.name AS '
            'author_name \n'
            'FROM article INNER JOIN user ON article.author_id = user.id\n'
            '\n'), str(compiled))

    def test_create_no_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=False
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
         'CREATE MATERIALIZED VIEW public.`article-vw` AS SELECT article.id, article.name, user.id '
         'AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_replace(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=True
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        # N.B. or_replace is not supported for materialized views in Starrocks
        self.assertEqual(('\n'
         'CREATE MATERIALIZED VIEW public.`article-vw` AS SELECT article.id, '
         'article.name, user.id AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_replace_if_not_exists(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            or_replace=True,
            if_not_exists=True,
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        # N.B. or_replace is not supported for materialized views in Starrocks
        self.assertEqual(('\n'
         'CREATE MATERIALIZED VIEW IF NOT EXISTS public.`article-vw` AS SELECT '
         'article.id, article.name, user.id AS author_id, user.name AS author_name \n'
         'FROM article INNER JOIN user ON article.author_id = user.id\n'
         '\n'), str(compiled))

    def test_create_with_options(self):
        metadata = sa.MetaData()
        view_obj = sa.Table('article-vw', metadata, schema='public')

        expr = vw.CreateView(
            view_obj,
            selectable=sa.select(
                Article.id,
                Article.name,
                User.id.label('author_id'),
                User.name.label('author_name'),
            ).join(
                User, Article.author_id == User.id
            ),
            materialized=True,
            options={
                'option1': 'opt'
            }
        )

        compiled = expr.compile(dialect=self.eng.dialect, compile_kwargs={"render_postcompile": True})
        self.assertEqual(('\n'
            'CREATE MATERIALIZED VIEW public.`article-vw` PROPERTIES ("option1"="opt") AS '
            'SELECT article.id, article.name, user.id AS author_id, user.name AS '
            'author_name \n'
            'FROM article INNER JOIN user ON article.author_id = user.id\n'
            '\n'), str(compiled))


if __name__ == '__main__':
    unittest.main()
