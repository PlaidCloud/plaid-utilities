# coding=utf-8

"""Tests for the sql_expression helpers added for frame_join_multi:
edge_predicate, topo_sort_edges, check_cartesian_explosion, get_column_table extension."""

import unittest

import sqlalchemy

from plaidcloud.utilities.sql_expression import (
    SQLExpressionError,
    check_cartesian_explosion,
    edge_predicate,
    eval_expression,
    get_column_table,
    get_safe_dict,
    topo_sort_edges,
)


def _make_table(name: str, columns: list[str]) -> sqlalchemy.Table:
    """Build a sqlalchemy Table with the given columns (all Integer for simplicity)."""
    md = sqlalchemy.MetaData()
    return sqlalchemy.Table(
        name, md,
        *[sqlalchemy.Column(c, sqlalchemy.Integer) for c in columns],
    )


def _compile_sql(predicate) -> str:
    """Compile a predicate to a SQL string with literal binds inlined — for assertion only."""
    return str(predicate.compile(compile_kwargs={'literal_binds': True}))


class TestEdgePredicateEqualityOperators(unittest.TestCase):

    def setUp(self):
        self.sales = _make_table('sales', ['id', 'customer_id', 'amt'])
        self.cust = _make_table('cust', ['id', 'amt'])
        self.tables = {'sales': self.sales, 'cust': self.cust}

    def test_equality(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.customer_id', 'operator': '=', 'right_expr': 'cust.id'},
            ],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('sales.customer_id', sql)
        self.assertIn('cust.id', sql)
        self.assertIn('=', sql)

    def test_not_equal(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.amt', 'operator': '<>', 'right_expr': 'cust.amt'},
            ],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('!=', sql)  # sqlalchemy renders <> as != on default dialect

    def test_less_than(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.amt', 'operator': '<', 'right_expr': 'cust.amt'},
            ],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('sales.amt < cust.amt', sql)

    def test_multiple_conditions_anded(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.customer_id', 'operator': '=', 'right_expr': 'cust.id'},
                {'left_expr': 'sales.amt', 'operator': '>=', 'right_expr': 'cust.amt'},
            ],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('AND', sql.upper())


class TestEdgePredicateNullOperators(unittest.TestCase):

    def setUp(self):
        self.sales = _make_table('sales', ['id', 'name'])
        self.cust = _make_table('cust', ['id'])
        self.tables = {'sales': self.sales, 'cust': self.cust}

    def test_is_null(self):
        edge = {
            'join_type': 'left',
            'conditions': [{'left_expr': 'sales.name', 'operator': 'IS NULL'}],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('IS NULL', sql.upper())
        self.assertNotIn('IS NOT NULL', sql.upper())

    def test_is_not_null(self):
        edge = {
            'join_type': 'left',
            'conditions': [{'left_expr': 'sales.name', 'operator': 'IS NOT NULL'}],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('IS NOT NULL', sql.upper())


class TestEdgePredicateLiteralBinding(unittest.TestCase):
    """Critical: all literals must be bound parameters, never string-interpolated."""

    def setUp(self):
        self.sales = _make_table('sales', ['id', 'name', 'amt'])
        self.cust = _make_table('cust', ['id'])
        self.tables = {'sales': self.sales, 'cust': self.cust}

    def test_like_pattern_is_bound_not_inlined(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.name', 'operator': 'LIKE', 'pattern': "foo'; DROP TABLE x; --"},
            ],
        }
        # Compile WITHOUT literal_binds — pattern should appear as a bind parameter, not inlined
        compiled = edge_predicate(edge, self.tables).compile()
        sql = str(compiled)
        # The dangerous string should NOT appear in the raw SQL — it's in the params dict
        self.assertNotIn('DROP TABLE', sql)
        self.assertIn("foo'; DROP TABLE x; --", str(compiled.params.values()))

    def test_in_values_are_bound_not_inlined(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.id', 'operator': 'IN', 'in_values': [1, 2, "'; DROP --"]},
            ],
        }
        compiled = edge_predicate(edge, self.tables).compile()
        sql = str(compiled)
        self.assertNotIn('DROP', sql)
        self.assertIn("'; DROP --", str(compiled.params.values()))

    def test_between_literal_bounds_are_bound(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.amt', 'operator': 'BETWEEN', 'between_low': 10, 'right_expr': 100},
            ],
        }
        compiled = edge_predicate(edge, self.tables).compile()
        sql = str(compiled)
        # SQL has placeholders, not literal 10/100
        self.assertNotIn(' 10 ', sql)
        self.assertNotIn(' 100 ', sql)
        param_vals = list(compiled.params.values())
        self.assertIn(10, param_vals)
        self.assertIn(100, param_vals)


class TestEdgePredicateBetween(unittest.TestCase):

    def setUp(self):
        self.sales = _make_table('sales', ['id', 'amt'])
        self.cust = _make_table('cust', ['low', 'high'])
        self.tables = {'sales': self.sales, 'cust': self.cust}

    def test_between_with_column_bounds(self):
        edge = {
            'join_type': 'inner',
            'conditions': [
                {'left_expr': 'sales.amt', 'operator': 'BETWEEN',
                 'between_low': 'cust.low', 'right_expr': 'cust.high'},
            ],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn('BETWEEN', sql.upper())
        self.assertIn('cust.low', sql)
        self.assertIn('cust.high', sql)


class TestEdgePredicateInOperators(unittest.TestCase):

    def setUp(self):
        self.a = _make_table('a', ['code'])
        self.b = _make_table('b', ['id'])
        self.tables = {'a': self.a, 'b': self.b}

    def test_in_compiles_to_in_clause(self):
        edge = {
            'join_type': 'inner',
            'conditions': [{'left_expr': 'a.code', 'operator': 'IN', 'in_values': ['x', 'y', 'z']}],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        self.assertIn(' IN ', sql.upper())

    def test_not_in_compiles_to_not_in(self):
        edge = {
            'join_type': 'inner',
            'conditions': [{'left_expr': 'a.code', 'operator': 'NOT IN', 'in_values': ['x']}],
        }
        sql = _compile_sql(edge_predicate(edge, self.tables))
        upper = sql.upper()
        self.assertTrue('NOT IN' in upper or 'NOT (' in upper)


class TestEdgePredicateUnknownReferences(unittest.TestCase):
    """edge_predicate raises on missing alias/column. Defense-in-depth — validator should catch first."""

    def setUp(self):
        self.a = _make_table('a', ['id'])
        self.tables = {'a': self.a}

    def test_unknown_alias_raises(self):
        edge = {
            'join_type': 'inner',
            'conditions': [{'left_expr': 'missing.id', 'operator': '=', 'right_expr': 'a.id'}],
        }
        with self.assertRaises(SQLExpressionError):
            edge_predicate(edge, self.tables)

    def test_unknown_column_raises(self):
        edge = {
            'join_type': 'inner',
            'conditions': [{'left_expr': 'a.nonexistent', 'operator': 'IS NULL'}],
        }
        with self.assertRaises(SQLExpressionError):
            edge_predicate(edge, self.tables)

    def test_unsupported_operator_raises(self):
        edge = {
            'join_type': 'inner',
            'conditions': [{'left_expr': 'a.id', 'operator': 'REGEXP', 'pattern': '.*'}],
        }
        with self.assertRaises(SQLExpressionError):
            edge_predicate(edge, self.tables)


class TestTopoSortEdges(unittest.TestCase):

    def test_chain_in_order(self):
        edges = [
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'b', 'to_alias': 'c'},
            {'from_alias': 'c', 'to_alias': 'd'},
        ]
        sorted_edges = topo_sort_edges(edges, root='a')
        self.assertEqual([e['to_alias'] for e in sorted_edges], ['b', 'c', 'd'])

    def test_chain_out_of_order_gets_sorted(self):
        edges = [
            {'from_alias': 'c', 'to_alias': 'd'},
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'b', 'to_alias': 'c'},
        ]
        sorted_edges = topo_sort_edges(edges, root='a')
        self.assertEqual([e['to_alias'] for e in sorted_edges], ['b', 'c', 'd'])

    def test_fan_out_sibling_order_preserved(self):
        """Root has two children. Order between siblings is the order they appear in edges."""
        edges = [
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'a', 'to_alias': 'c'},
        ]
        sorted_edges = topo_sort_edges(edges, root='a')
        self.assertEqual([e['to_alias'] for e in sorted_edges], ['b', 'c'])

    def test_fan_out_with_grandchildren(self):
        edges = [
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'a', 'to_alias': 'c'},
            {'from_alias': 'b', 'to_alias': 'd'},
        ]
        sorted_edges = topo_sort_edges(edges, root='a')
        # BFS order: b, c, d (b's child before c's children at same depth not relevant since c has none)
        self.assertEqual([e['to_alias'] for e in sorted_edges], ['b', 'c', 'd'])

    def test_diamond_raises(self):
        edges = [
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'a', 'to_alias': 'c'},
            {'from_alias': 'b', 'to_alias': 'd'},
            {'from_alias': 'c', 'to_alias': 'd'},
        ]
        with self.assertRaises(SQLExpressionError):
            topo_sort_edges(edges, root='a')

    def test_orphan_edges_raise(self):
        edges = [
            {'from_alias': 'a', 'to_alias': 'b'},
            {'from_alias': 'x', 'to_alias': 'y'},
        ]
        with self.assertRaises(SQLExpressionError):
            topo_sort_edges(edges, root='a')


class TestCheckCartesianExplosion(unittest.TestCase):

    def test_cross_within_limit_passes(self):
        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'cross', 'conditions': []}]
        row_counts = {'tabA': 100, 'tabB': 200}
        check_cartesian_explosion(sources, edges, row_count_fn=lambda t: row_counts[t],
                                  row_limit=100_000)
        # No raise

    def test_cross_over_limit_raises(self):
        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'cross', 'conditions': []}]
        row_counts = {'tabA': 1_000_000, 'tabB': 1_000_000}
        with self.assertRaises(SQLExpressionError) as ctx:
            check_cartesian_explosion(sources, edges, row_count_fn=lambda t: row_counts[t],
                                      row_limit=1_000_000_000)
        self.assertIn('cartesian explosion', str(ctx.exception))

    def test_filtered_inner_join_does_not_explode(self):
        """A regular inner join with conditions doesn't trigger the guard (predicates filter)."""
        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'inner',
                  'conditions': [{'left_expr': 'a.id', 'operator': '=', 'right_expr': 'b.id'}]}]
        row_counts = {'tabA': 10_000_000_000, 'tabB': 10_000_000_000}
        check_cartesian_explosion(sources, edges, row_count_fn=lambda t: row_counts[t],
                                  row_limit=1_000_000)
        # No raise — inner join with predicate is not counted

    def test_hard_limit_clamps_user_limit(self):
        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'cross', 'conditions': []}]
        row_counts = {'tabA': 10**8, 'tabB': 10**8}
        # User specifies a stupidly large limit; hard_limit should clamp
        with self.assertRaises(SQLExpressionError):
            check_cartesian_explosion(sources, edges, row_count_fn=lambda t: row_counts[t],
                                      row_limit=10**20, hard_limit=10**15)


class TestGetColumnTableExtension(unittest.TestCase):
    """The keyword-only tables_by_alias parameter must:
       1. Be honored when source_alias is set.
       2. Not break any existing callers (positional path unchanged).
       3. Reject ambiguous name-intersect when len(source_tables) > 2.
    """

    def setUp(self):
        self.a = _make_table('a', ['id', 'name'])
        self.b = _make_table('b', ['id', 'name'])
        self.c = _make_table('c', ['id', 'name', 'unique_to_c'])
        self.tables_list = [self.a, self.b, self.c]
        self.columns = [
            [{'source': 'id'}, {'source': 'name'}],
            [{'source': 'id'}, {'source': 'name'}],
            [{'source': 'id'}, {'source': 'name'}, {'source': 'unique_to_c'}],
        ]
        self.tables_by_alias = {'a': self.a, 'b': self.b, 'c': self.c}

    def test_source_alias_resolved_via_tables_by_alias(self):
        tc = {'source_alias': 'b', 'source': 'b.id'}
        result = get_column_table(self.tables_list, tc, self.columns,
                                  tables_by_alias=self.tables_by_alias)
        self.assertIs(result, self.b)

    def test_source_alias_without_tables_by_alias_raises(self):
        tc = {'source_alias': 'b', 'source': 'b.id'}
        with self.assertRaises(SQLExpressionError):
            get_column_table(self.tables_list, tc, self.columns)

    def test_source_alias_unknown_raises(self):
        tc = {'source_alias': 'missing', 'source': 'missing.id'}
        with self.assertRaises(SQLExpressionError):
            get_column_table(self.tables_list, tc, self.columns,
                             tables_by_alias=self.tables_by_alias)

    def test_legacy_source_table_for_two_sources_unchanged(self):
        tables_2 = [self.a, self.b]
        cols_2 = [self.columns[0], self.columns[1]]
        tc1 = {'source_table': 'table1', 'source': 'id'}
        tc2 = {'source_table': 'table2', 'source': 'id'}
        self.assertIs(get_column_table(tables_2, tc1, cols_2), self.a)
        self.assertIs(get_column_table(tables_2, tc2, cols_2), self.b)

    def test_legacy_source_table_for_three_sources_rejected(self):
        tc = {'source_table': 'table1', 'source': 'id'}
        with self.assertRaises(SQLExpressionError):
            get_column_table(self.tables_list, tc, self.columns)

    def test_tableN_positional_unchanged(self):
        tc = {'source': 'table2.id'}
        result = get_column_table(self.tables_list, tc, self.columns)
        self.assertIs(result, self.b)

    def test_name_intersect_unique_for_3_sources_resolves(self):
        tc = {'source': 'unique_to_c'}
        result = get_column_table(self.tables_list, tc, self.columns)
        self.assertIs(result, self.c)

    def test_name_intersect_ambiguous_for_3_sources_raises(self):
        tc = {'source': 'name'}  # appears in all 3
        with self.assertRaises(SQLExpressionError):
            get_column_table(self.tables_list, tc, self.columns)

    def test_name_intersect_for_2_sources_returns_first_match(self):
        """Backwards-compat: existing binary-join behavior preserved."""
        tables_2 = [self.a, self.b]
        cols_2 = [self.columns[0], self.columns[1]]
        tc = {'source': 'name'}  # in both
        result = get_column_table(tables_2, tc, cols_2)
        self.assertIs(result, self.a)

    def test_one_source_shortcut(self):
        """Single-table case shortcuts at line 132; no precedence rules apply."""
        tc = {'source_alias': 'whatever', 'source': 'anything'}
        result = get_column_table([self.a], tc, [self.columns[0]])
        self.assertIs(result, self.a)


class TestExpressionAliasExposure(unittest.TestCase):
    """frame_join_multi target-column expressions reference join aliases (`mdp.name`), not
    positional `table1`/`table2`. get_safe_dict must expose each alias from tables_by_alias so
    eval_expression can resolve them; without this the alias raises NameError at execute time."""

    def setUp(self):
        self.a = _make_table('analyzetable_aaa', ['id', 'firstname']).alias('up')
        self.b = _make_table('analyzetable_bbb', ['id', 'name']).alias('mdp')
        self.tables = [self.a, self.b]
        self.tables_by_alias = {'up': self.a, 'mdp': self.b}

    def test_get_safe_dict_exposes_aliases(self):
        safe = get_safe_dict(self.tables, tables_by_alias=self.tables_by_alias)
        # Aliases present in addition to the positional table1/table2 keys.
        self.assertIn('up', safe)
        self.assertIn('mdp', safe)
        self.assertIn('table1', safe)
        self.assertIs(safe['mdp'], self.b.columns)

    def test_alias_qualified_expression_resolves(self):
        # The smoking-gun expression shape from the ToS Collections join.
        expr = "case((mdp.name.isnot(None), mdp.name), else_=up.firstname)"
        rendered = eval_expression(expr, None, self.tables, tables_by_alias=self.tables_by_alias)
        sql = _compile_sql(rendered)
        self.assertIn('CASE', sql.upper())
        self.assertIn('mdp.name', sql)
        self.assertIn('up.firstname', sql)

    def test_alias_unknown_without_tables_by_alias(self):
        # Without alias exposure the alias name is undefined — the pre-fix failure mode.
        expr = "mdp.name"
        with self.assertRaises(SQLExpressionError):
            eval_expression(expr, None, self.tables)

    def test_builtins_win_alias_name_clash(self):
        # A pathological alias named like a builtin must not shadow the builtin.
        clash = {'func': self.b}
        safe = get_safe_dict(self.tables, tables_by_alias=clash)
        self.assertIs(safe['func'], sqlalchemy.func)


if __name__ == '__main__':
    unittest.main()
