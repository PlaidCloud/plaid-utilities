# coding=utf-8
"""The StarRocks float-cast capability, and why it is registered here.

The failure this guards has no error attached to it: the `CAST(… AS FLOAT)` is
removed from the statement and the numbers come back wrong on a green step. So
the defect is pinned as well as the fix — a test that only asserts the patched
dialect works would still pass if the registration were dropped and every
consumer fell back to the vendor class.
"""

import subprocess
import sys
import warnings

import sqlalchemy
from sqlalchemy.dialects import registry

from plaidcloud.utilities import dialects


def float_cast_statement():
    table = sqlalchemy.table('t', sqlalchemy.column('a'))
    return sqlalchemy.select(sqlalchemy.cast(table.c.a, sqlalchemy.Float))


def test_the_starrocks_name_resolves_to_the_patched_dialect():
    assert registry.load('starrocks')._support_float_cast is True


def test_a_float_cast_survives_compilation():
    dialect = registry.load('starrocks')(paramstyle='pyformat')

    assert 'CAST(t.a AS FLOAT)' in str(float_cast_statement().compile(dialect=dialect))


def test_the_unpatched_vendor_dialect_is_what_this_prevents():
    """Pins the defect. Without the registration every consumer gets this."""
    from starrocks.dialect import StarRocksDialect

    vendor = StarRocksDialect(paramstyle='pyformat')
    with warnings.catch_warnings():
        warnings.simplefilter('ignore')
        sql = str(float_cast_statement().compile(dialect=vendor))

    assert vendor._support_float_cast is False
    assert 'CAST' not in sql


def test_importing_query_alone_arms_the_registration():
    """`query.Connection` is the consumer this exists for, and it must not have to
    know about any of it.

    Run in a fresh interpreter: in-process this proves nothing, because importing
    this test module has already imported `dialects` and armed the registration.
    That import-order dependence is the whole defect — workflow-runner armed it as
    a side effect of importing its transform handler, so a `Connection` built
    without that import got the vendor class.
    """
    probe = (
        'import plaidcloud.utilities.query;'
        'from sqlalchemy.dialects import registry;'
        'print(registry.load("starrocks")._support_float_cast)'
    )
    result = subprocess.run(
        [sys.executable, '-c', probe],
        capture_output=True, text=True, timeout=180, check=True,
    )

    assert result.stdout.strip() == 'True'


def test_a_broken_registration_is_reported_rather_than_raised(monkeypatch):
    """The build gate must keep answering with its message, not a traceback.

    A name we register resolves through our own module, so a bad module path or a
    renamed class raises `AttributeError` from inside `registry.load` — which the
    gate would otherwise let escape into a Docker build log.
    """
    monkeypatch.setattr(
        dialects, 'REQUIRED_DIALECTS', {'starrocks': 'starrocks'})
    registry.register('starrocks', 'plaidcloud.utilities.starrocks_dialect', 'NoSuchClass')
    try:
        failures = list(dialects.unloadable_dialects())
    finally:
        registry.register(
            'starrocks', 'plaidcloud.utilities.starrocks_dialect', 'PlaidStarRocksDialect')

    assert [dialect for dialect, _, _ in failures] == ['starrocks']
