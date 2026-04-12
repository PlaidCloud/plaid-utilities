# coding=utf-8
"""Supplemental tests for plaidcloud.utilities.sql_expression to bring coverage to ~100%.

These tests cover missed regions:
  - eval_expression raise path (line 62) when apply_variables fails and disable_variables=False
  - process_fn trim_fn branch (lines 191-192)
  - Result class dict comprehension (line 321)
  - allocate() function (lines 933-1150)
  - eval_rule() function (lines 1154-1169)
  - apply_rules() function (lines 1202-1272)

Only .compile() is used to validate queries -- no database required.
"""
import unittest

import pandas as pd
import sqlalchemy

from plaidcloud.utilities import sql_expression as se


def _compile(stmt):
    """Compile a SQLAlchemy statement to a string (no DB required)."""
    return str(stmt.compile(compile_kwargs={"literal_binds": False}))


class TestEvalExpressionRaise(unittest.TestCase):
    """Covers line 62: the `raise` when apply_variables fails and disable_variables is False."""

    def test_apply_variables_failure_raises(self):
        # "'{var}'" with no matching key should cause apply_variables to raise.
        # With disable_variables=False (default), this should propagate.
        with self.assertRaises(Exception):
            se.eval_expression("'{var}'", {}, [])


class TestProcessFnTrim(unittest.TestCase):
    """Covers lines 191-192: trim_fn branch when trim_type is truthy."""

    def test_trim_true_wraps_rtrim(self):
        # When trim_type=True, process_fn should wrap expression with rtrim(rtrim(expr, '0'), '.')
        result = se.process_fn(None, None, None, 'foo', trim_type=True)(sqlalchemy.literal("12.300"))
        compiled = _compile(result)
        # Expect two rtrim calls in compiled SQL
        self.assertIn('rtrim', compiled.lower())

    def test_trim_false_is_identity(self):
        # When trim_type is False/None, no rtrim calls appear.
        result = se.process_fn(None, None, None, 'foo', trim_type=False)(sqlalchemy.literal("12.300"))
        compiled = _compile(result)
        self.assertNotIn('rtrim', compiled.lower())


class TestResultClass(unittest.TestCase):
    """Covers line 321 (Result.__init__ dict comprehension)."""

    def test_result_builds_dict_of_columns(self):
        table = se.get_table_rep(
            'table_x',
            [
                {'source': 'Column1', 'dtype': 'text'},
                {'source': 'Column2', 'dtype': 'numeric'},
            ],
            'anlz_schema',
        )
        source_column_configs = [[
            {'source': 'Column1', 'dtype': 'text'},
            {'source': 'Column2', 'dtype': 'numeric'},
        ]]
        target_columns = [
            {'source': 'Column1', 'target': 'OutputA', 'dtype': 'text'},
            {'source': 'Column2', 'target': 'OutputB', 'dtype': 'numeric'},
            # serial columns must be skipped by the comprehension
            {'source': None, 'target': 'OutputSerial', 'dtype': 'serial'},
        ]
        result = se.Result(
            tables=[table],
            target_columns=target_columns,
            source_column_configs=source_column_configs,
        )
        # The result object should have attributes for non-serial target columns only
        self.assertTrue(hasattr(result, 'OutputA'))
        self.assertTrue(hasattr(result, 'OutputB'))
        self.assertFalse(hasattr(result, 'OutputSerial'))


# ---------------------------------------------------------------------------
# Helpers for allocate/apply_rules
# ---------------------------------------------------------------------------

def _build_source_query():
    """A simple source query used by allocate()."""
    source_table = sqlalchemy.table(
        'source_tbl',
        sqlalchemy.column('region'),
        sqlalchemy.column('product'),
        sqlalchemy.column('amount'),
        sqlalchemy.column('reassign_me'),
    )
    return sqlalchemy.select(
        source_table.c.region,
        source_table.c.product,
        source_table.c.amount,
        source_table.c.reassign_me,
    )


def _build_driver_query():
    """A simple driver query used by allocate()."""
    driver_table = sqlalchemy.table(
        'driver_tbl',
        sqlalchemy.column('region'),
        sqlalchemy.column('product'),
        sqlalchemy.column('reassign_me'),
        sqlalchemy.column('value'),
    )
    return sqlalchemy.select(
        driver_table.c.region,
        driver_table.c.product,
        driver_table.c.reassign_me,
        driver_table.c.value,
    )


class TestAllocate(unittest.TestCase):
    """Covers allocate() (lines 933-1150)."""

    def test_allocate_basic_no_parent_context(self):
        source_query = _build_source_query()
        driver_query = _build_driver_query()

        stmt = se.allocate(
            source_query=source_query,
            driver_query=driver_query,
            allocate_columns=['amount'],
            numerator_columns=['reassign_me'],
            denominator_columns=['region'],
            driver_value_column='value',
            unique_cte_index=1,
        )
        compiled = _compile(stmt)
        # Sanity check that the output contains the expected CTE names
        self.assertIn('alloc_source_1', compiled)
        self.assertIn('alloc_driver_1', compiled)
        self.assertIn('consol_driver_1', compiled)
        self.assertIn('denominator_1', compiled)
        self.assertIn('ratios_1', compiled)

    def test_allocate_with_include_source_columns_and_no_overwrite(self):
        source_query = _build_source_query()
        driver_query = _build_driver_query()

        stmt = se.allocate(
            source_query=source_query,
            driver_query=driver_query,
            allocate_columns=['amount'],
            numerator_columns=['reassign_me'],
            denominator_columns=['region'],
            driver_value_column='value',
            overwrite_cols_for_allocated=False,
            include_source_columns=['amount'],
            unique_cte_index=2,
        )
        compiled = _compile(stmt)
        # With overwrite=False, allocated column should be suffixed
        self.assertIn('amount_allocated', compiled)
        # include_source_columns should add a '_source' suffix column
        self.assertIn('amount_source', compiled)

    def test_allocate_with_parent_context(self):
        """Hit the parent_context_queries branch (lines ~992-1018)."""
        source_query = _build_source_query()
        driver_query = _build_driver_query()

        # Build parent_child and leaves queries for the 'region' column.
        pc_table = sqlalchemy.table(
            'pc_tbl',
            sqlalchemy.column('Parent'),
            sqlalchemy.column('Child'),
        )
        parent_child_query = sqlalchemy.select(pc_table.c.Parent, pc_table.c.Child)

        leaves_table = sqlalchemy.table(
            'leaves_tbl',
            sqlalchemy.column('Node'),
            sqlalchemy.column('Leaf'),
        )
        leaves_query = sqlalchemy.select(leaves_table.c.Node, leaves_table.c.Leaf)

        parent_context_queries = {
            'region': {
                'PARENT_CHILD': parent_child_query,
                'LEAVES': leaves_query,
            },
        }

        stmt = se.allocate(
            source_query=source_query,
            driver_query=driver_query,
            allocate_columns=['amount'],
            numerator_columns=['reassign_me'],
            denominator_columns=['region'],
            driver_value_column='value',
            unique_cte_index=3,
            parent_context_queries=parent_context_queries,
        )
        compiled = _compile(stmt)
        # Parent/leaves CTEs must appear when parent_context_queries is provided
        self.assertIn('parent_region_3', compiled)
        self.assertIn('leaves_region_3', compiled)
        self.assertIn('parent_driver_3', compiled)


class TestEvalRule(unittest.TestCase):
    """Covers eval_rule() (lines 1154-1169)."""

    def test_apply_variables_fails_but_disable_variables_true(self):
        # "'{missing}'" causes apply_variables to raise; disable_variables=True
        # swallows and uses the raw string. The raw string ''{missing}'' evaluates
        # to the literal string "{missing}" -- eval should succeed.
        result = se.eval_rule(
            "'{missing}'",
            variables={},
            tables=[],
            disable_variables=True,
        )
        self.assertEqual(result, '{missing}')

    def test_eval_failure_raises_sql_expression_error(self):
        # A rule that compiles fine but fails to eval (division by zero).
        with self.assertRaises(se.SQLExpressionError):
            se.eval_rule("1/0", variables={}, tables=[])

    def test_apply_variables_failure_propagates_when_not_disabled(self):
        # Unknown variable + disable_variables=False -> raise propagates.
        with self.assertRaises(Exception):
            se.eval_rule("'{missing}'", variables={}, tables=[])


class TestApplyRules(unittest.TestCase):
    """Covers apply_rules() (lines 1202-1272)."""

    def _df(self):
        return pd.DataFrame([
            {
                'rule_id': 'R1',
                'condition': "table['region'] == 'North'",
                'include': True,
                'iteration': 1,
                'value': 100,
            },
            {
                'rule_id': 'R2',
                'condition': "table['region'] == 'South'",
                'include': True,
                'iteration': 1,
                'value': 200,
            },
            # A rule that should be filtered out due to include=False
            {
                'rule_id': 'R3',
                'condition': "table['region'] == 'East'",
                'include': False,
                'iteration': 1,
                'value': 300,
            },
        ])

    def _source_query(self):
        source_table = sqlalchemy.table(
            'src',
            sqlalchemy.column('region'),
            sqlalchemy.column('amount'),
        )
        return sqlalchemy.select(source_table.c.region, source_table.c.amount)

    def test_apply_rules_include_once_true(self):
        df_rules = self._df()
        cte_rules, final_select = se.apply_rules(
            source_query=self._source_query(),
            df_rules=df_rules,
            rule_id_column='rule_id',
            target_columns=['value'],
            include_once=True,
        )
        compiled = _compile(final_select)
        self.assertIn('rule_id', compiled)
        self.assertIn('applied_rules', compiled)

    def test_apply_rules_include_once_false(self):
        df_rules = self._df()
        cte_rules, final_select = se.apply_rules(
            source_query=self._source_query(),
            df_rules=df_rules,
            rule_id_column='rule_id',
            target_columns=['value'],
            include_once=False,
        )
        compiled = _compile(final_select)
        self.assertIn('applied_rules', compiled)

    def test_apply_rules_auto_iteration_column(self):
        # Drop the iteration column to hit the `if iteration_column not in df_rules.columns` branch.
        df_rules = self._df().drop(columns=['iteration'])
        cte_rules, final_select = se.apply_rules(
            source_query=self._source_query(),
            df_rules=df_rules,
            rule_id_column='rule_id',
            target_columns=['value'],
            include_once=True,
        )
        # Just ensure it compiles without error.
        _compile(final_select)


if __name__ == '__main__':
    unittest.main()
