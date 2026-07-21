# coding=utf-8
"""Sandbox regression tests for the expression engine (sc-22664).

``eval_expression`` / ``eval_rule`` compile and ``eval()`` user-authored
strings. Before this, the eval globals carried no ``__builtins__`` key, so
CPython injected the real builtins module and any expression author could run
``__import__('subprocess').getoutput(...)``. These tests pin both halves of the
fix: the pinned ``__builtins__`` and the static ``_assert_safe_expression``
guard — and, just as importantly, that legitimate SQL expressions still pass.
"""
import unittest

import sqlalchemy

from plaidcloud.utilities import sql_expression as se

__author__ = 'Simon Moscrop'
__copyright__ = '© Copyright 2026, PlaidCloud, Inc.'
__license__ = 'Apache 2.0'


def _table():
    md = sqlalchemy.MetaData()
    return sqlalchemy.Table(
        'analyzetable_t', md,
        sqlalchemy.Column('a', sqlalchemy.INTEGER),
        sqlalchemy.Column('b', sqlalchemy.INTEGER),
        sqlalchemy.Column('flag', sqlalchemy.BOOLEAN),
        # Customers really do have underscore-led column names (sc-23186).
        sqlalchemy.Column('_MajorAccountFlag', sqlalchemy.TEXT),
        schema='anlz',
    )


# Each executes arbitrary code (or leaks internals) if it reaches a naked
# eval(). The guard rejects every one; the ``{...}`` subset is also stopped a
# layer earlier by apply_variables (undefined template token).
ATTACKS = [
    "__import__('subprocess').getoutput('id')",           # direct builtins RCE
    "__import__('os').system('id')",
    "().__class__.__mro__[1].__subclasses__()",           # traversal, no builtins
    "().__class__.__bases__[0].__subclasses__()",
    "'{0.__class__.__init__.__globals__}'.format(())",     # str.format getattr leak
    "'{0.__class__}'.format_map({})",
    "(lambda: 1)()",                                       # lambda escape aid
    "[c for c in ().__class__.__mro__]",                  # comprehension + dunder
    "__builtins__",
]

# The brace-free subset reaches the eval path (apply_variables passes them
# through), so the guard is the sole thing standing between them and eval().
BRACE_FREE_ATTACKS = [a for a in ATTACKS if '{' not in a]

# Non-dunder attribute-traversal escapes (sc-22664): each reaches
# os/subprocess/io/sys.modules purely through non-underscore attrs rooted at the
# raw ``sqlalchemy`` module — invisible to a denylist. The allowlist blocks them
# by confining a sqlalchemy-rooted chain and rejecting unlisted AST nodes.
TRAVERSAL_ATTACKS = [
    "sqlalchemy.log.logging.os.system('id')",
    "sqlalchemy.exc.compat.platform.os.system('id')",
    "sqlalchemy.engine.create.inspect.os.system('id')",
    "sqlalchemy.engine.base.sys.modules['os'].system('id')",
    "sqlalchemy.engine.create.inspect.importlib.import_module('os').system('id')",
    "sqlalchemy.util.concurrency.asyncio.base_events.subprocess.call(['x'])",
    "str(sqlalchemy.log.logging.os.popen('id').read())",
    "sqlalchemy.exc.compat.importlib_metadata.import_module('os').system('id')",
    "sqlalchemy.log.logging.io.open('/etc/hostname').read()",
    "sqlalchemy.engine.create.inspect.tokenize.open('/etc/hostname').read()",
    "str(sqlalchemy.engine.base.sys.modules)",
    "sqlalchemy.util.langhelpers.sys.modules",
    "sqlalchemy.util.langhelpers.sys.modules['os'].system('id')",
    # AST-node-shaped wrappers around the same call (ternary / chained compare /
    # list+subscript / starred / bytes-decode) — all now rejected node types or
    # blocked sqlalchemy roots.
    "sqlalchemy.log.logging.os.system('id') if 1 < 2 else 0",
    "0 <= sqlalchemy.log.logging.os.system('id') <= 0",
    "[sqlalchemy.log.logging.os.system('id')][0]",
    "sqlalchemy.connectors.asyncio.asyncio.base_events.subprocess.call(*[['x']])",
    "sqlalchemy.log.logging.os.system('touch ' + b'x'.decode())",
]

# Raw-SQL splice via select()'s result methods (sc-23125). Allowlisting
# ``select`` (needed for the facet distinct-count) hands the sandbox a Select,
# whose prefix_with/suffix_with/with_hint/with_statement_hint coerce an
# arbitrary string into literal SQL — the same primitive text() gives. These
# live on a Call *result* (root None), so the sqlalchemy-root allowlist can't
# see them; the guard blocks the method names unconditionally. The last case
# reaches back to the Select via a subquery's ``.element`` back-reference.
RAW_SQL_SPLICE_ATTACKS = [
    "sqlalchemy.select(get_column(table, 'a')).suffix_with('UNION SELECT secret').subquery()",
    "sqlalchemy.select(get_column(table, 'a')).prefix_with('/*x*/ EVIL').subquery()",
    "sqlalchemy.select(get_column(table, 'a')).with_hint(table, 'EVIL').subquery()",
    "sqlalchemy.select(get_column(table, 'a')).with_statement_hint('EVIL').subquery()",
    "sqlalchemy.select(get_column(table, 'a')).subquery().element.suffix_with('EVIL').subquery()",
]

# These build ordinary SQLAlchemy expressions and MUST still pass the guard.
LEGIT = [
    "'foobar'",
    "None",
    "table.a + table.b",
    "(table.a == 1) & (table.b == 2)",
    "~table.flag",
    "table.a.in_([1, 2, 3])",
    "get_column(table, 'a')",
    "cast(v(1), integer)",
    "case((table.a.isnot(None), table.a), else_=0)",
    "func.coalesce(table.a, 0)",
    "func._if(table.a > 1, 'y', 'n')",      # Databend underscore SQL function
    "func.format(table.a, 2)",              # real SQL FORMAT function
    "abs(table.a)",
    "sqlalchemy.cast(table.a, sqlalchemy.Integer)",   # curated sqlalchemy.* attrs
    "table.a * 100 / table.b",
    "func.import_col(get_column(table, 'a'), 'text', '', False)",
    # Table Explorer facet distinct-count (sc-23125): sqlalchemy.select must
    # be allowlisted or the facet view never loads.
    "sqlalchemy.select(func.count(func.distinct(get_column(table, 'a')))).subquery()",
]

# Saved customer expressions the first allowlist broke (sc-23186): `is`,
# `is not` and `not` were never deliberately excluded, and a step that had run
# for years started failing with "Is is not permitted in an expression."
IDENTITY_AND_NOT = [
    "get_column(table, 'a') is None",
    "get_column(table, 'a') is not None",
    "not get_column(table, 'a')",
    "case((get_column(table, 'a') is None, false), else_=table.b)",
    "table.a is None and table.b is not None",
]


class TestExpressionSandboxBlocks(unittest.TestCase):
    def test_guard_rejects_every_attack(self):
        # The guard is the authoritative layer — assert it vetoes all of them,
        # including the str.format payloads that apply_variables would also stop.
        for attack in ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se._assert_safe_expression(attack)

    def test_eval_path_never_executes_an_attack(self):
        # End-to-end: no attack survives eval_expression (some are stopped by
        # apply_variables' brace handling, the rest by the guard) — none run.
        for attack in ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(Exception):
                    se.eval_expression(attack, None, [])

    def test_brace_free_attacks_hit_the_guard_specifically(self):
        for attack in BRACE_FREE_ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se.eval_expression(attack, None, [])

    def test_attacks_rejected_in_rule_path(self):
        with self.assertRaises(se.SQLExpressionError):
            se.eval_rule("__import__('os').system('id')", {}, [])

    def test_guard_rejects_every_traversal_attack(self):
        # Non-dunder attribute-traversal escapes rooted at ``sqlalchemy`` —
        # every one bypassed the old underscore-only denylist.
        for attack in TRAVERSAL_ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se._assert_safe_expression(attack)

    def test_traversal_attacks_never_execute_via_eval_path(self):
        for attack in TRAVERSAL_ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se.eval_expression(attack, None, [])

    def test_sqlalchemy_text_raw_sql_is_blocked(self):
        # sqlalchemy.text injects raw SQL; not in the curated attr allowlist.
        with self.assertRaises(se.SQLExpressionError):
            se._assert_safe_expression("sqlalchemy.text('DROP TABLE t')")

    def test_guard_rejects_raw_sql_splice_off_select(self):
        # sc-23125: select() is allowlisted, but its raw-text splice methods
        # (and the subquery .element back-ref to them) must stay blocked.
        for attack in RAW_SQL_SPLICE_ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se._assert_safe_expression(attack)

    def test_raw_sql_splice_never_executes_via_eval_path(self):
        table = _table()
        for attack in RAW_SQL_SPLICE_ATTACKS:
            with self.subTest(attack=attack):
                with self.assertRaises(se.SQLExpressionError):
                    se.eval_expression(attack, None, [table])

    def test_builtins_are_pinned_and_dangerous_ones_absent(self):
        safe = se.get_safe_dict([])
        self.assertIn('__builtins__', safe)
        builtins = safe['__builtins__']
        for banned in ('__import__', 'open', 'eval', 'exec', 'compile', 'getattr'):
            self.assertNotIn(banned, builtins)


class TestExpressionSandboxAllows(unittest.TestCase):
    def test_legitimate_expressions_pass_the_guard(self):
        for expr in LEGIT:
            with self.subTest(expr=expr):
                # Guard must not veto it; evaluation itself is covered elsewhere.
                se._assert_safe_expression(expr)

    def test_legitimate_expression_still_evaluates(self):
        table = _table()
        result = se.eval_expression("table.a + table.b", {}, [table])
        self.assertEqual(se.eval_expression("get_column(table, 'a')", {}, [table]).name, 'a')
        # Produces a real SQLAlchemy element, not a Python value.
        self.assertIsInstance(result, sqlalchemy.sql.elements.ColumnElement)

    def test_identity_and_not_operators_pass_the_guard(self):
        # sc-23186: pure operators, no escape surface — the sandbox must not
        # veto them just because they are usually a semantic no-op on a Column.
        for expr in IDENTITY_AND_NOT:
            with self.subTest(expr=expr):
                se._assert_safe_expression(expr)

    def test_case_with_is_none_branch_still_evaluates(self):
        # The exact customer shape (sc-23186), end to end through eval.
        table = _table()
        result = se.eval_expression(
            "case((get_column(table, 'a') is None, false), else_=table.b)", {}, [table],
        )
        self.assertIsInstance(result, sqlalchemy.sql.elements.ColumnElement)

    def test_is_none_on_a_column_is_a_dead_branch(self):
        # Pins WHY `is None` is allowed but useless: Python identity against a
        # Column is always False, so authors need `.is_(None)` for a real null
        # test. If SQLAlchemy ever overloads this, the guidance changes.
        table = _table()
        self.assertFalse(se.eval_expression("get_column(table, 'a') is None", {}, [table]))
        self.assertIsInstance(
            se.eval_expression("get_column(table, 'a').is_(None)", {}, [table]),
            sqlalchemy.sql.elements.ColumnElement,
        )

    def test_underscore_column_is_reachable_as_an_attribute(self):
        # sc-23186: `func.rtrim(table._MajorAccountFlag)` is what the UI writes
        # for an underscore-led column; the guard must not read it as an
        # internals escape. Needs the eval context to tell column from internal.
        table = _table()
        result = se.eval_expression(
            "func.rtrim(table._MajorAccountFlag)", {}, [table],
        )
        self.assertIsInstance(result, sqlalchemy.sql.elements.ColumnElement)

    def test_underscore_internals_still_blocked_on_the_same_table(self):
        # The exemption is column-name-scoped, not a blanket underscore pass:
        # an internal that is NOT a column stays blocked, as does any chained
        # underscore attr and every dunder.
        table = _table()
        safe = se.get_safe_dict([table])
        for expr in (
            "table._sa_instance_state",
            "table._MajorAccountFlag._parententity",
            "table.__class__",
            "func.rtrim(table.a)._compiler_dispatch",
        ):
            with self.subTest(expr=expr):
                with self.assertRaises(se.SQLExpressionError):
                    se._assert_safe_expression(expr, safe)

    def test_column_named_after_an_internal_does_not_expose_it(self):
        # The exemption is by resolved identity, not by name: a column called
        # `_index` does NOT shadow ColumnCollection._index, so allowing it by
        # name would hand the expression SQLAlchemy internals (and, through
        # `_all_columns[0].table.metadata`, the schema graph).
        md = sqlalchemy.MetaData()
        hostile = sqlalchemy.Table(
            'analyzetable_h', md,
            *[sqlalchemy.Column(n, sqlalchemy.TEXT)
              for n in ('_index', '_collection', '_all_columns', '_colset')],
            schema='anlz',
        )
        safe = se.get_safe_dict([hostile])
        for expr in ('table._index', 'table._all_columns',
                     'table._all_columns[0].table.metadata'):
            with self.subTest(expr=expr):
                with self.assertRaises(se.SQLExpressionError):
                    se._assert_safe_expression(expr, safe)
        # …and the column itself stays reachable the safe way.
        se._assert_safe_expression("get_column(table, '_index')", safe)

    def test_underscore_column_blocked_without_the_eval_context(self):
        # No safe_dict = no way to tell column from internal, so the guard
        # stays closed rather than guessing.
        with self.assertRaises(se.SQLExpressionError):
            se._assert_safe_expression("func.rtrim(table._MajorAccountFlag)")

    def test_facet_distinct_count_subquery_evaluates(self):
        # sc-23125: the Table Explorer facet's runtime-generated distinct-count
        # expression must eval to a scalar subquery, not raise on `select`.
        table = _table()
        result = se.eval_expression(
            "sqlalchemy.select(func.count(func.distinct(get_column(table, 'a')))).subquery()",
            {}, [table],
        )
        self.assertIsInstance(result, sqlalchemy.sql.selectable.Subquery)


if __name__ == '__main__':
    unittest.main()
