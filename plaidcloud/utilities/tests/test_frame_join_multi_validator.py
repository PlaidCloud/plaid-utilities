# coding=utf-8

"""Unit tests for the frame_join_multi config validator.

Most of the heavy lifting is in the JSON fixture file shipped via package_data; each fixture
declares whether it's valid or its expected `_expected_reason` token. This file:
  (a) Loads fixtures and runs them through validate_frame_join_multi_config.
  (b) Adds targeted tests for size caps, edge cases, and the dialect parameter.
"""

import copy
import importlib.resources
import json
import unittest

from plaidcloud.utilities.frame_join_multi_validator import (
    JoinMultiValidationError,
    JOIN_TYPES,
    MAX_CONFIG_BYTES,
    MAX_IN_VALUE_BYTES,
    MAX_IN_VALUES,
    MAX_PATTERN_LEN,
    MAX_SOURCES,
    OPERATORS,
    RESERVED_ALIASES,
    validate_frame_join_multi_config,
)


def _load_fixtures():
    files = importlib.resources.files('plaidcloud.utilities.test_fixtures')
    with files.joinpath('frame_join_multi_config_fixtures.json').open() as f:
        return json.load(f)


FIXTURES = _load_fixtures()


def _strip_meta(cfg):
    """Fixtures carry _doc / _expected_reason / _dialect metadata; the validator should ignore."""
    out = {k: v for k, v in cfg.items() if not k.startswith('_')}
    return out


class TestFixtures(unittest.TestCase):
    """Fixture-driven: each `valid_*` fixture must pass; each `invalid_*` must raise the
    documented reason token."""

    def test_valid_fixtures_pass(self):
        for name, fixture in FIXTURES.items():
            if name.startswith('_'):
                continue
            if not name.startswith('valid_'):
                continue
            with self.subTest(fixture=name):
                cfg = _strip_meta(fixture)
                validate_frame_join_multi_config(cfg)

    def test_invalid_fixtures_raise_expected_reason(self):
        for name, fixture in FIXTURES.items():
            if not name.startswith('invalid_'):
                continue
            with self.subTest(fixture=name):
                cfg = _strip_meta(fixture)
                dialect = fixture.get('_dialect')
                expected = fixture['_expected_reason']
                # Some fixtures need a synth step to construct over-cap values that would
                # bloat the JSON otherwise.
                synth = fixture.get('_runtime_synth')
                if synth == 'having_oversize':
                    cfg['having'] = 'result.id > 0 and ' * 300  # ~6000 chars > 4096 cap
                with self.assertRaises(JoinMultiValidationError) as ctx:
                    validate_frame_join_multi_config(cfg, dialect=dialect)
                self.assertEqual(ctx.exception.reason, expected,
                                 f"fixture {name}: expected reason {expected!r}, got {ctx.exception.reason!r}")


class TestShapeCaps(unittest.TestCase):
    """Validator must short-circuit on shape caps before per-field validation, to bound CPU."""

    def _minimal_valid(self):
        return copy.deepcopy(FIXTURES['valid_two_source_inner'])

    def test_config_not_dict_rejected(self):
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config("not a dict")
        self.assertEqual(ctx.exception.reason, 'config_not_dict')

    def test_config_too_large_rejected(self):
        cfg = self._minimal_valid()
        # Inflate source_columns on source[0] while keeping the required 'id' and 'customer_id'
        # columns intact (so the edge condition still resolves). Pad with extra columns whose
        # long ids push JSON over 256 KiB.
        cfg['sources'][0]['source_columns'] = (
            cfg['sources'][0]['source_columns']
            + [{'id': f'pad_col_{i:06d}_' + 'x' * 100} for i in range(2500)]
        )
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'config_too_large')

    def test_sources_count_over_max(self):
        cfg = _strip_meta(FIXTURES['valid_two_source_inner'])
        cfg = copy.deepcopy(cfg)
        # Inflate sources past MAX_SOURCES
        cfg['sources'] = [
            {'alias': f'a{i}', 'source': f'tabT{i:03d}', 'source_columns': [{'id': 'id'}]}
            for i in range(MAX_SOURCES + 1)
        ]
        cfg['edges'] = []  # Will fail count check, but sources cap fires first
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'sources_count_out_of_range')


class TestAliasRules(unittest.TestCase):

    def _base(self):
        return copy.deepcopy(_strip_meta(FIXTURES['valid_two_source_inner']))

    def test_alias_too_long_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['alias'] = 'a' * 64  # 64 chars; max is 63
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'alias_invalid')

    def test_alias_starts_with_digit_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['alias'] = '1sales'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'alias_invalid')

    def test_alias_with_newline_rejected(self):
        """fullmatch + no MULTILINE flag rejects newlines."""
        cfg = self._base()
        cfg['sources'][0]['alias'] = 'sales\n'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'alias_invalid')

    def test_alias_case_insensitive_duplicate_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['alias'] = 'Sales'
        cfg['sources'][1]['alias'] = 'SALES'
        cfg['edges'][0]['from_alias'] = 'Sales'
        cfg['edges'][0]['to_alias'] = 'SALES'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'alias_duplicate')

    def test_every_reserved_alias_rejected(self):
        for word in RESERVED_ALIASES:
            cfg = self._base()
            cfg['sources'][0]['alias'] = word
            cfg['edges'][0]['from_alias'] = word
            cfg['edges'][0]['conditions'][0]['left_expr'] = f'{word}.customer_id'
            with self.subTest(word=word):
                with self.assertRaises(JoinMultiValidationError) as ctx:
                    validate_frame_join_multi_config(cfg)
                self.assertEqual(ctx.exception.reason, 'alias_reserved')

    def test_positional_table_alias_rejected(self):
        # `table1`/`table2`/... are positional names the executor exposes in the expression
        # namespace; an alias matching one would be silently shadowed (an expression `table1.col`
        # would read the first source, not this alias). Reject them.
        for word in ('table1', 'table2', 'TABLE1', 'table42'):
            cfg = self._base()
            cfg['sources'][0]['alias'] = word
            cfg['edges'][0]['from_alias'] = word
            cfg['edges'][0]['conditions'][0]['left_expr'] = f'{word}.customer_id'
            with self.subTest(word=word):
                with self.assertRaises(JoinMultiValidationError) as ctx:
                    validate_frame_join_multi_config(cfg)
                self.assertEqual(ctx.exception.reason, 'alias_reserved')


class TestSourceField(unittest.TestCase):
    """source must be an identifier-shaped table id — accepts the canonical analyzetable_<uuid>
    that table_find/table_upsert return (and every other step uses), while still closing the
    apply_variables + path-lookup surface in workflow_runner's transform_handler.get_frame
    (no dots, slashes, braces, or whitespace reach SQL emit)."""

    def _base(self):
        return copy.deepcopy(_strip_meta(FIXTURES['valid_two_source_inner']))

    def test_path_style_source_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['source'] = 'folder/path/to/table'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_not_table_id')

    def test_source_with_format_braces_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['source'] = 'tab{tenant_secret}'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_not_table_id')

    def test_source_with_dot_dot_rejected(self):
        cfg = self._base()
        cfg['sources'][0]['source'] = 'tab..secrets'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_not_table_id')

    def test_canonical_analyzetable_source_accepted(self):
        # Regression for the reported bug: the canonical analyzetable_<uuid> id that
        # table_find/table_upsert return — and that frame_extract/frame_join_inner|outer
        # already accept — must validate. The old `tab`-prefix regex rejected it.
        cfg = self._base()
        cfg['sources'][0]['source'] = 'analyzetable_53867b49-c8d9-4ceb-8272-79017344b3bb'
        validate_frame_join_multi_config(cfg)  # no raise

    def test_valid_table_prefix_accepted(self):
        cfg = self._base()
        cfg['sources'][0]['source'] = 'tabABC123_xyz-456'
        validate_frame_join_multi_config(cfg)  # no raise


class TestConditionShapes(unittest.TestCase):

    def _three_source(self):
        return copy.deepcopy(_strip_meta(FIXTURES['valid_three_source_tree']))

    def test_unknown_operator_rejected(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0]['operator'] = 'NEAR'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'operator_invalid')

    def test_unknown_left_expr_column_rejected(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0]['left_expr'] = 'sales.nonexistent_col'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'left_expr_column_unknown')

    def test_pattern_too_long_rejected(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0] = {
            'left_expr': 'sales.customer_id',
            'operator': 'LIKE',
            'pattern': 'x' * (MAX_PATTERN_LEN + 1),
        }
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'pattern_too_long')

    def test_in_values_count_zero_rejected(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0] = {
            'left_expr': 'sales.customer_id',
            'operator': 'IN',
            'in_values': [],
        }
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'in_values_count_out_of_range')

    def test_in_values_element_too_large(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0] = {
            'left_expr': 'sales.customer_id',
            'operator': 'IN',
            'in_values': ['x' * (MAX_IN_VALUE_BYTES + 100)],
        }
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'in_value_too_large')

    def test_in_values_non_primitive(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0] = {
            'left_expr': 'sales.customer_id',
            'operator': 'IN',
            'in_values': [{'nested': 'dict'}],
        }
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'in_value_not_primitive')

    def test_between_with_column_bounds(self):
        cfg = self._three_source()
        cfg['edges'][0]['conditions'][0] = {
            'left_expr': 'sales.customer_id',
            'operator': 'BETWEEN',
            'between_low': 'cust.id',
            'right_expr': 'cust.id',
        }
        validate_frame_join_multi_config(cfg)  # no raise


class TestDialectGating(unittest.TestCase):

    def test_full_outer_passes_when_no_dialect_specified(self):
        """Save-time hook has no dialect; FULL OUTER passes there. Executor catches it."""
        cfg = _strip_meta(FIXTURES['invalid_full_outer_on_databend'])
        validate_frame_join_multi_config(cfg, dialect=None)  # no raise

    def test_full_outer_passes_on_starrocks(self):
        cfg = _strip_meta(FIXTURES['invalid_full_outer_on_databend'])
        validate_frame_join_multi_config(cfg, dialect='starrocks')  # no raise

    def test_full_outer_rejected_on_databend(self):
        cfg = _strip_meta(FIXTURES['invalid_full_outer_on_databend'])
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg, dialect='databend')
        self.assertEqual(ctx.exception.reason, 'full_outer_unsupported_on_dialect')


class TestErrorShape(unittest.TestCase):

    def test_error_carries_field_locator(self):
        cfg = copy.deepcopy(_strip_meta(FIXTURES['valid_two_source_inner']))
        cfg['sources'][0]['alias'] = '1bad'
        try:
            validate_frame_join_multi_config(cfg)
            self.fail('expected JoinMultiValidationError')
        except JoinMultiValidationError as e:
            self.assertEqual(e.code, 'JOIN_VALIDATION')
            self.assertEqual(e.field, 'sources[0].alias')
            self.assertEqual(e.reason, 'alias_invalid')


class TestOperatorEnum(unittest.TestCase):
    """The closed enum is the security contract — anything outside is rejected."""

    def test_no_expr_operator(self):
        self.assertNotIn('expr', OPERATORS)

    def test_no_regexp(self):
        self.assertNotIn('REGEXP', OPERATORS)

    def test_no_null_safe_equality(self):
        self.assertNotIn('<=>', OPERATORS)

    def test_join_types_are_exactly_four(self):
        self.assertEqual(JOIN_TYPES, frozenset({'inner', 'left', 'full', 'cross'}))


class TestRound11ContractGaps(unittest.TestCase):
    """Round-11 found that the validator didn't enforce fields the executor consumes.
    Each test below exercises one of the new validator rules."""

    def _base(self):
        return copy.deepcopy(_strip_meta(FIXTURES['valid_two_source_inner']))

    def test_target_frame_required(self):
        cfg = self._base()
        del cfg['target_frame']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_frame_invalid')

    def test_target_frame_rejects_non_identifier(self):
        cfg = self._base()
        cfg['target_frame'] = 'folder/path/table'  # slash isn't an identifier char
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_frame_invalid')

    def test_target_frame_canonical_id_accepted(self):
        cfg = self._base()
        cfg['target_frame'] = 'analyzetable_27d0dd09-971f-4392-a84a-9426ba76342a'
        validate_frame_join_multi_config(cfg)  # no raise

    def test_target_columns_target_required(self):
        cfg = self._base()
        del cfg['target_columns'][0]['target']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_name_invalid')

    def test_target_columns_target_duplicate_rejected(self):
        cfg = self._base()
        cfg['target_columns'][1]['target'] = cfg['target_columns'][0]['target']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_name_duplicate')

    def test_target_columns_dtype_required(self):
        cfg = self._base()
        del cfg['target_columns'][0]['dtype']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_dtype_invalid')

    def test_target_columns_dtype_must_be_in_enum(self):
        cfg = self._base()
        cfg['target_columns'][0]['dtype'] = 'bogus_type'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_dtype_invalid')

    def test_target_columns_agg_must_be_in_enum(self):
        cfg = self._base()
        cfg['target_columns'][0]['agg'] = 'not_a_real_agg'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_agg_invalid')

    def test_target_column_expression_accepted(self):
        # Expression target columns are allowed: they render via the executor's eval_expression
        # with the join aliases in scope, so a multi-alias CASE can be expressed. No source_alias
        # is required (an expression isn't tied to a single source).
        cfg = self._base()
        cfg['target_columns'].append({
            'target': 'computed', 'dtype': 'text',
            'expression': "case((sales.id.isnot(None), sales.id), else_='x')",
        })
        validate_frame_join_multi_config(cfg)  # no raise

    def test_target_column_expression_with_source_rejected(self):
        # An expression AND a source on the same column is two modes — must be exactly one.
        cfg = self._base()
        cfg['target_columns'][0]['expression'] = 'sales.id * 2'  # tc[0] already has `source`
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_mode_required')

    def test_target_column_whitespace_expression_rejected(self):
        # A whitespace-only expression is truthy, so the executor's `if expression:` would select
        # it and crash compile() with SyntaxError. Reject it at validation (matches having).
        cfg = self._base()
        cfg['target_columns'][0]['expression'] = '   '
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_expression_empty')

    def test_target_column_empty_string_expression_ignored(self):
        # Empty string is falsy in the executor too (`if expression:`), so it's treated as no
        # expression — the column stays valid on its existing `source` mode.
        cfg = self._base()
        cfg['target_columns'][0]['expression'] = ''
        validate_frame_join_multi_config(cfg)  # no raise

    def test_target_columns_mode_required(self):
        cfg = self._base()
        # Remove both `source` and `constant` — no mode key.
        del cfg['target_columns'][0]['source']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'target_column_mode_required')

    def test_source_columns_dtype_required(self):
        cfg = self._base()
        del cfg['sources'][0]['source_columns'][0]['dtype']
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_column_dtype_invalid')

    def test_source_columns_dtype_must_be_in_enum(self):
        cfg = self._base()
        cfg['sources'][0]['source_columns'][0]['dtype'] = 'bogus'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_column_dtype_invalid')

    def test_user_supplied_datastore_dialect_rejected_at_save_time(self):
        """Save-time hook (dialect=None) must reject user-supplied datastore_dialect to
        prevent FULL OUTER bypass on Databend tenants."""
        cfg = self._base()
        cfg['datastore_dialect'] = 'starrocks'
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg, dialect=None)
        self.assertEqual(ctx.exception.reason, 'datastore_dialect_not_user_settable')

    def test_user_supplied_datastore_dialect_accepted_at_execute_time(self):
        """Execute-time validator (dialect=<framework-injected>) accepts the value since
        the framework merged it in."""
        cfg = self._base()
        cfg['datastore_dialect'] = 'starrocks'  # framework injected
        validate_frame_join_multi_config(cfg, dialect='starrocks')  # no raise


class TestRound12HavingAndSourceWhereGaps(unittest.TestCase):
    """Round-12 reconfirmed contract gaps for `having` and `source_where` paths that prior
    rounds documented but never closed. Each test exercises one rule that round-12 added."""

    def _base(self):
        return copy.deepcopy(_strip_meta(FIXTURES['valid_two_source_inner']))

    def test_whitespace_only_having_rejected(self):
        cfg = self._base()
        cfg['having'] = '   '
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'having_empty_or_whitespace')

    def test_empty_string_having_accepted(self):
        """Empty string having is equivalent to no having — accepted (validator treats as absent)."""
        cfg = self._base()
        cfg['having'] = ''
        validate_frame_join_multi_config(cfg)

    def test_valid_having_passes(self):
        cfg = _strip_meta(FIXTURES['valid_with_having_and_source_where'])
        validate_frame_join_multi_config(cfg)

    def test_alias_qualified_source_where_rejected(self):
        """source_where='alias.col > 0' fails at execute time because eval_expression's
        safe_dict only has `table1`. Validator rejects to prevent the cryptic runtime error."""
        cfg = self._base()
        cfg['sources'][0]['source_where'] = 'sales.customer_id > 0'  # 'sales' is the alias
        with self.assertRaises(JoinMultiValidationError) as ctx:
            validate_frame_join_multi_config(cfg)
        self.assertEqual(ctx.exception.reason, 'source_where_alias_qualified_ref')

    def test_table1_qualified_source_where_accepted(self):
        """table1.col is the canonical form per get_safe_dict's table_numbering_start=1."""
        cfg = self._base()
        cfg['sources'][0]['source_where'] = 'table1.customer_id > 0'
        validate_frame_join_multi_config(cfg)

    def test_bare_column_source_where_accepted(self):
        """Bare column names should work; eval_expression's safe_dict exposes them."""
        cfg = self._base()
        cfg['sources'][0]['source_where'] = 'customer_id > 0'
        validate_frame_join_multi_config(cfg)

    def test_whitespace_only_source_where_treated_as_absent(self):
        """Whitespace-only source_where is silently dropped (executor's `if where:` skips empty)."""
        cfg = self._base()
        cfg['sources'][0]['source_where'] = '   '
        validate_frame_join_multi_config(cfg)  # no raise — treated as absent


if __name__ == '__main__':
    unittest.main()
