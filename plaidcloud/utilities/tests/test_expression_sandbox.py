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
        schema='anlz',
    )


# Each of these executes arbitrary code (or leaks internals) if it reaches a
# naked eval(). Every one is rejected by the static guard directly. A subset
# containing ``{...}`` is *also* intercepted one layer earlier by
# apply_variables (it reads the braces as an undefined template token), so the
# eval_expression path blocks them either way — see the two test methods.
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

# Non-dunder attribute-traversal escapes (sc-22664, escalated). Each reaches
# os / subprocess / io / sys.modules purely through non-underscore attributes
# rooted at the raw ``sqlalchemy`` module — the old underscore-only denylist
# never fired. The allowlist guard blocks them by confining a sqlalchemy-rooted
# chain to curated type/expression helpers and rejecting unlisted AST nodes.
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


if __name__ == '__main__':
    unittest.main()
