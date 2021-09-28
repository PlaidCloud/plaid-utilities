# coding=utf-8

import unittest

import sqlalchemy
from toolz.functoolz import identity as ident

from plaidcloud.rpc.database import PlaidUnicode
from plaidcloud.utilities import sql_expression as se

__author__ = "Adams Tower"
__copyright__ = "© Copyright 2009-2021, Tartan Solutions, Inc"
__credits__ = ["Adams Tower"]
__license__ = "Apache 2.0"
__maintainer__ = "Adams Tower"
__email__ = "adams.tower@tartansolutions.com"


class TestSQLExpression(unittest.TestCase):

    def test_get_project_schema():
        assert se.get_project_schema('12345') == 'anlz12345'
        assert se.get_project_schema('anlz12345') == 'anlz12345'

    def test_get_agg_fn():
        assert se.get_agg_fn(None) == ident
        assert se.get_agg_fn('') == ident
        assert se.get_agg_fn('group') == ident
        assert se.get_agg_fn('dont_group') == ident
        
        assert se.get_agg_fn('sum') == sqlalchemy.func.sum
        assert se.get_agg_fn('count_null') == sqlalchemy.func.count

    def test_get_table_rep():
        table = se.get_table_rep('table_12345', [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}], 'anlz_schema')
        assert isinstance(table, sqlalchemy.Table)

        assert table.name == 'table_12345'
        assert table.schema == 'anlz_schema'
        
        assert len(table.columns) == 2
        column_1, column_2 = table.columns
        assert isinstance(column_1, sqlalchemy.Column)
        assert isinstance(column_2, sqlalchemy.Column)
        assert column_1.name == 'Column1'
        assert column_1.type == PlaidUnicode(length=5000)
        assert column_2.name == 'Column2'
        assert column_2.type == sqlalchemy.NUMERIC()

        same_table = se.get_table_rep('table_12345', [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}], 'anlz_schema', metadata=table.metadata)
        assert table == same_table

        table_using_column_key = se.get_table_rep('table_12345', [{'foobar': 'Column1', 'dtype': 'text'}, {'foobar': 'Column2', 'dtype': 'numeric'}], 'anlz_schema', metadata=table.metadata, column_key='foobar')
        assert table == table_using_column_key

        aliased_table = se.get_table_rep('table_12345', [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}], 'anlz_schema', metadata=table.metadata, alias='table_alias')
        assert isinstance(aliased_table, sqlalchemy.Alias)
        assert table2.name == 'table_alias'

    def test_get_table_rep_using_id():
         table = se.get_table_rep('table_12345', [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}], 'anlz_schema')
         table2 = se.get_table_rep('table_12345', [{'source': 'Column1', 'dtype': 'text'}, {'source': 'Column2', 'dtype': 'numeric'}], '_schema')
         assert isinstance(table2, sqlalchemy.Table)
         assert table.schema == table2.schema

    #TODO: maybe get_table_column next?
