# coding=utf-8
"""sc-23158 WS-J4: taking the dialect from the caller compiles the same SQL.

`Connection` used to derive its dialect from an unqualified `analyze.query.dialect()`
— the tenant's own lakehouse. It now prefers the value the caller was handed, which
for a workflow step is the `datastore_dialect` stamped on its run payload for *this*
project.

A tenant has one lakehouse today, so the two sources carry the same string and every
compiled statement must be byte-identical to what the derived path produced. That is
what this asserts, over the statement shapes the transforms that consume
`conn.dialect` actually emit — the filtered/aggregated select `frame_melt`,
`frame_pivot` and `rule_assignment` build, the `INSERT … FROM SELECT` the allocations
write through, a join, a UNION and a parameterised predicate.

Parameterised over every engine the fleet runs that this interpreter can load, plus
`postgresql`, which SQLAlchemy always registers — so the suite is never vacuous in an
environment carrying no warehouse drivers.
"""

import unittest
import uuid
from unittest.mock import MagicMock, patch

import sqlalchemy as sa
from sqlalchemy.dialects import registry
from sqlalchemy.exc import NoSuchModuleError

from plaidcloud.utilities import query
from plaidcloud.utilities.dialects import REQUIRED_DIALECTS
from plaidcloud.utilities.query import Connection
from plaidcloud.utilities.tests.test_query import make_mock_rpc

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2026, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'


def _loadable_dialects():
    names = ['postgresql'] + sorted(REQUIRED_DIALECTS)
    out = []
    for name in names:
        try:
            registry.load(name)
        except (NoSuchModuleError, ImportError):
            continue
        out.append(name)
    return out


_META = sa.MetaData()
_SOURCE = sa.Table(
    'analyzetable_source', _META,
    sa.Column('account', sa.Text),
    sa.Column('region', sa.Text),
    sa.Column('amount', sa.Numeric),
    schema='anlz_proj',
)
_DRIVER = sa.Table(
    'analyzetable_driver', _META,
    sa.Column('region', sa.Text),
    sa.Column('weight', sa.Numeric),
    schema='anlz_proj',
)
_TARGET = sa.Table(
    'analyzetable_target', _META,
    sa.Column('account', sa.Text),
    sa.Column('amount', sa.Numeric),
    schema='anlz_proj',
)


def _statements():
    grouped = (
        sa.select(_SOURCE.c.account, sa.func.sum(_SOURCE.c.amount).label('amount'))
        .where(_SOURCE.c.region == sa.bindparam('region'))
        .group_by(_SOURCE.c.account)
        .having(sa.func.sum(_SOURCE.c.amount) > 0)
        .order_by(_SOURCE.c.account)
        .limit(500)
    )
    joined = sa.select(
        _SOURCE.c.account,
        (_SOURCE.c.amount * _DRIVER.c.weight).label('allocated'),
    ).select_from(_SOURCE.join(_DRIVER, _SOURCE.c.region == _DRIVER.c.region))
    unioned = sa.union_all(
        sa.select(_SOURCE.c.account, _SOURCE.c.amount),
        sa.select(_TARGET.c.account, _TARGET.c.amount),
    )
    cased = sa.select(
        sa.case((_SOURCE.c.amount.is_(None), sa.literal_column("'none'")),
                else_=sa.cast(_SOURCE.c.amount, sa.Text)).label('amount'),
        _SOURCE.c.account.in_(['4000', '4100', '4200']).label('kept'),
    )
    return {
        'grouped_select': grouped,
        'join': joined,
        'union_all': unioned,
        'case_and_in': cased,
        'insert_from_select': _TARGET.insert().from_select(
            [_TARGET.c.account, _TARGET.c.amount],
            sa.select(_SOURCE.c.account, _SOURCE.c.amount),
        ),
    }


def _connection(dialect_name, passed):
    """A Connection whose dialect came from the caller (passed) or from the RPC."""
    rpc = make_mock_rpc(dialect_name=dialect_name)
    with patch.object(query, 'Dimensions') as dims:
        dims.return_value = MagicMock()
        return Connection(
            project=str(uuid.uuid4()), rpc=rpc,
            dialect=dialect_name if passed else None,
        )


class TestDialectSourceParity(unittest.TestCase):

    def test_at_least_one_dialect_is_exercised(self):
        self.assertIn('postgresql', _loadable_dialects())

    def test_compiled_sql_is_identical_whichever_source_named_the_dialect(self):
        statements = _statements()
        for dialect_name in _loadable_dialects():
            derived = _connection(dialect_name, passed=False)
            passed = _connection(dialect_name, passed=True)
            self.assertEqual(derived.dialect.name, passed.dialect.name)
            for label, statement in statements.items():
                with self.subTest(dialect=dialect_name, statement=label):
                    before_sql, before_params = derived._compiled(statement)
                    after_sql, after_params = passed._compiled(statement)
                    self.assertEqual(before_sql, after_sql)
                    self.assertEqual(before_params, after_params)
