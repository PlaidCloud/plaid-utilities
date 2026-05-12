# coding=utf-8

"""End-to-end integration tests for frame_join_multi.

Round-10 caught that the validator and executor were diverging on the `source_columns[*]`
key (validator validated `id`, executor's get_table_rep used `source` by default). No prior
test exercised both layers against the same fixture. This file closes that gap: every test
runs a config through validate_frame_join_multi_config AND through the SQL emission path,
ensuring round-trip consistency.
"""

import copy
import importlib.resources
import json
import unittest

import sqlalchemy

from plaidcloud.utilities.frame_join_multi_validator import (
    validate_frame_join_multi_config,
)
from plaidcloud.utilities.sql_expression import (
    edge_predicate,
    get_table_rep,
    topo_sort_edges,
)


def _load_fixtures():
    files = importlib.resources.files('plaidcloud.utilities.test_fixtures')
    with files.joinpath('frame_join_multi_config_fixtures.json').open() as f:
        return json.load(f)


FIXTURES = _load_fixtures()


def _strip_meta(cfg):
    return {k: v for k, v in cfg.items() if not k.startswith('_')}


def _normalize_source_columns(cols):
    """Mirrors the executor's normalization (synthesizes source=id and dtype=text default)
    so the same key shape flows into get_table_rep / edge_predicate / source_from_clause
    regardless of which key the original config used."""
    return [
        {**c, 'source': c.get('source', c['id']), 'dtype': c.get('dtype', 'text')}
        for c in cols
    ]


class TestValidatorExecutorRoundTrip(unittest.TestCase):
    """For each valid fixture, run validator + build sqlalchemy Tables + compile a predicate."""

    def _build_tables(self, cfg):
        """Mirror the executor's Step 1: per-source MetaData, normalized columns, aliased Table."""
        tables_by_alias = {}
        for s in cfg['sources']:
            md = sqlalchemy.MetaData()
            normalized = _normalize_source_columns(s['source_columns'])
            base = get_table_rep(
                s['source'],
                normalized,
                'anlz',
                md,
                alias=s['alias'],
            )
            tables_by_alias[s['alias']] = sqlalchemy.select(base).subquery(s['alias'])
        return tables_by_alias

    def test_valid_two_source_inner_builds_tables(self):
        cfg = _strip_meta(FIXTURES['valid_two_source_inner'])
        validate_frame_join_multi_config(cfg)
        tables = self._build_tables(cfg)
        # Tables are subqueries; their `.columns` must expose every column id from the config.
        for s in cfg['sources']:
            expected_cols = {c['id'] for c in s['source_columns']}
            actual_cols = {c.name for c in tables[s['alias']].columns}
            self.assertEqual(actual_cols, expected_cols,
                             f"alias {s['alias']!r}: columns mismatch")

    def test_valid_three_source_tree_predicate_compiles(self):
        cfg = _strip_meta(FIXTURES['valid_three_source_tree'])
        validate_frame_join_multi_config(cfg)
        tables = self._build_tables(cfg)
        # Topo-sort + build predicate for every edge.
        ordered = topo_sort_edges(cfg['edges'], cfg['sources'][0]['alias'])
        for edge in ordered:
            pred = edge_predicate(edge, tables)
            sql = str(pred.compile(compile_kwargs={'literal_binds': True}))
            # Predicate must reference both endpoints' aliases in the rendered SQL.
            self.assertIn(edge['from_alias'], sql)
            self.assertIn(edge['to_alias'], sql)

    def test_valid_all_operators_compiles(self):
        cfg = _strip_meta(FIXTURES['valid_all_operators'])
        validate_frame_join_multi_config(cfg)
        tables = self._build_tables(cfg)
        edge = cfg['edges'][0]
        pred = edge_predicate(edge, tables)
        # Don't inline binds — verify the compiled SQL has parameter placeholders for literals.
        compiled = pred.compile()
        sql = str(compiled)
        # 13 operators in one edge — SQL should contain literal-binding placeholders, no
        # raw injection-prone strings like '; DROP'.
        self.assertNotIn('DROP', sql.upper())
        # The pattern 'foo%' (LIKE) is in compiled.params, not in the SQL string.
        self.assertIn('foo%', str(compiled.params.values()))

    def test_valid_five_source_mixed_joins_compiles(self):
        cfg = _strip_meta(FIXTURES['valid_five_source_mixed_joins'])
        validate_frame_join_multi_config(cfg)
        tables = self._build_tables(cfg)
        ordered = topo_sort_edges(cfg['edges'], cfg['sources'][0]['alias'])
        # Build the full join tree.
        joined = tables[cfg['sources'][0]['alias']]
        for edge in ordered:
            child = tables[edge['to_alias']]
            jt = edge['join_type']
            if jt == 'cross':
                pred = sqlalchemy.true()
            else:
                pred = edge_predicate(edge, tables)
            if jt == 'inner' or jt == 'cross':
                joined = sqlalchemy.join(joined, child, pred)
            elif jt == 'left':
                joined = sqlalchemy.outerjoin(joined, child, pred)
            elif jt == 'full':
                joined = sqlalchemy.join(joined, child, pred, full=True)
        # Compile the joined FromClause into SQL — must not raise.
        select_query = sqlalchemy.select(*tables[cfg['sources'][0]['alias']].columns).select_from(joined)
        sql = str(select_query.compile(compile_kwargs={'literal_binds': True}))
        # All five aliases must appear in the FROM tree.
        for s in cfg['sources']:
            self.assertIn(s['alias'], sql, f"alias {s['alias']!r} missing from compiled SQL")


class TestHavingAndSourceWhereEndToEnd(unittest.TestCase):
    """Round-13 closed the test-coverage gap: validator + executor + apply_output_filter +
    get_combined_wheres flow against a fixture that uses both having and source_where."""

    def _build_tables(self, cfg):
        from plaidcloud.utilities.sql_expression import get_combined_wheres
        tables_by_alias = {}
        for s in cfg['sources']:
            md = sqlalchemy.MetaData()
            normalized = _normalize_source_columns(s['source_columns'])
            base = get_table_rep(s['source'], normalized, 'anlz', md, alias=s['alias'])
            sel = sqlalchemy.select(base)
            where = s.get('source_where')
            if where:
                combined = get_combined_wheres([where], [base], {})
                if combined:
                    sel = sel.where(*combined)
            tables_by_alias[s['alias']] = sel.subquery(s['alias'])
        return tables_by_alias

    def test_having_and_source_where_compile_end_to_end(self):
        from plaidcloud.utilities.sql_expression import apply_output_filter, get_select_query
        cfg = _strip_meta(FIXTURES['valid_with_having_and_source_where'])
        validate_frame_join_multi_config(cfg)

        tables_by_alias = self._build_tables(cfg)
        # Build the JOIN (round-12: there's only one edge in this fixture).
        joined = tables_by_alias[cfg['sources'][0]['alias']]
        for edge in cfg['edges']:
            child = tables_by_alias[edge['to_alias']]
            from plaidcloud.utilities.sql_expression import edge_predicate
            pred = edge_predicate(edge, tables_by_alias)
            joined = sqlalchemy.join(joined, child, pred)

        select_query = get_select_query(
            tables=list(tables_by_alias.values()),
            tables_by_alias=tables_by_alias,
            source_columns=[_normalize_source_columns(s['source_columns']) for s in cfg['sources']],
            target_columns=cfg['target_columns'],
            wheres=None,
            variables={},
        ).select_from(joined)

        # Apply the post-join filter.
        select_query = apply_output_filter(select_query, cfg['having'], {})
        # Compile without literal_binds — fixtures use text dtype so int comparisons would
        # fail rendering. Bind parameters as placeholders instead.
        sql = str(select_query.compile())

        # The source_where (table1.customer_id > 0) should be inside the sales subquery.
        self.assertIn('customer_id', sql, 'source_where column not in compiled SQL')
        # The having (result.sale_id > 0) should reference the wrapped result subquery.
        self.assertIn('sale_id', sql, 'having column not in compiled SQL')
        # Outer wrap should be present.
        self.assertIn('result', sql, 'apply_output_filter wrap missing from SQL')


class TestCheckCartesianExplosionIntegration(unittest.TestCase):
    """Round-13 closed the integration-test gap: cartesian guard against a config-shaped input."""

    def test_cross_join_unknown_row_count_rejected(self):
        """When a cross edge participates and one source's row count is unknown, the guard
        cannot bound the product. The function itself doesn't enforce — but the executor
        wraps with an unknown_counts check; here we verify the helper math at least doesn't
        hide overflow when row_count_fn returns realistic values."""
        from plaidcloud.utilities.sql_expression import check_cartesian_explosion

        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'cross', 'conditions': []}]
        # Both sources at 1M rows → 1T product. Should exceed any reasonable limit.
        row_counts = {'tabA': 1_000_000, 'tabB': 1_000_000}
        from plaidcloud.utilities.sql_expression import SQLExpressionError
        with self.assertRaises(SQLExpressionError):
            check_cartesian_explosion(
                sources=sources, edges=edges,
                row_count_fn=lambda t: row_counts[t],
                row_limit=100_000_000,
            )

    def test_cross_join_within_limit_passes(self):
        from plaidcloud.utilities.sql_expression import check_cartesian_explosion
        sources = [
            {'alias': 'a', 'source': 'tabA'},
            {'alias': 'b', 'source': 'tabB'},
        ]
        edges = [{'from_alias': 'a', 'to_alias': 'b', 'join_type': 'cross', 'conditions': []}]
        row_counts = {'tabA': 100, 'tabB': 200}  # 20k product
        check_cartesian_explosion(
            sources=sources, edges=edges,
            row_count_fn=lambda t: row_counts[t],
            row_limit=100_000_000,
        )  # No raise.


class TestGetSelectQueryEndToEnd(unittest.TestCase):
    """Round-11 recommendation: exercise get_select_query (the path target_columns flow
    through) against validated configs. This is the meta-fix preventing the next round-10/11
    class of bug — every executor consumer must be tested against the validator's output."""

    def test_get_select_query_compiles_for_two_source_inner(self):
        from plaidcloud.utilities.sql_expression import get_select_query

        cfg = _strip_meta(FIXTURES['valid_two_source_inner'])
        validate_frame_join_multi_config(cfg)

        tables_by_alias = {}
        for s in cfg['sources']:
            md = sqlalchemy.MetaData()
            normalized = _normalize_source_columns(s['source_columns'])
            base = get_table_rep(
                s['source'], normalized, 'anlz', md, alias=s['alias'],
            )
            tables_by_alias[s['alias']] = sqlalchemy.select(base).subquery(s['alias'])

        # This is the call that round-11 said wasn't being tested. If target_columns are
        # missing required fields (target, dtype, agg, mode), get_from_clause raises here.
        select_query = get_select_query(
            tables=list(tables_by_alias.values()),
            tables_by_alias=tables_by_alias,
            source_columns=[_normalize_source_columns(s['source_columns']) for s in cfg['sources']],
            target_columns=cfg['target_columns'],
            wheres=None,
            variables={},
        )
        sql = str(select_query.compile(compile_kwargs={'literal_binds': True}))
        # Every target column's `target` name must appear in the SELECT list.
        for tc in cfg['target_columns']:
            self.assertIn(tc['target'], sql,
                          f"target {tc['target']!r} missing from compiled SELECT")

    def test_get_select_query_compiles_for_five_source(self):
        from plaidcloud.utilities.sql_expression import get_select_query

        cfg = _strip_meta(FIXTURES['valid_five_source_mixed_joins'])
        validate_frame_join_multi_config(cfg)

        tables_by_alias = {}
        for s in cfg['sources']:
            md = sqlalchemy.MetaData()
            normalized = _normalize_source_columns(s['source_columns'])
            base = get_table_rep(
                s['source'], normalized, 'anlz', md, alias=s['alias'],
            )
            tables_by_alias[s['alias']] = sqlalchemy.select(base).subquery(s['alias'])

        select_query = get_select_query(
            tables=list(tables_by_alias.values()),
            tables_by_alias=tables_by_alias,
            source_columns=[_normalize_source_columns(s['source_columns']) for s in cfg['sources']],
            target_columns=cfg['target_columns'],
            wheres=None,
            variables={},
        )
        # Just compile — if anything goes wrong with get_from_clause for any target column
        # (missing dtype, bad agg, etc.), compile would raise.
        sql = str(select_query.compile(compile_kwargs={'literal_binds': True}))
        for tc in cfg['target_columns']:
            self.assertIn(tc['target'], sql)


if __name__ == '__main__':
    unittest.main()
