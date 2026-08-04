# coding=utf-8
"""The build-time driver guard: what it requires, and what its failure says."""

import pytest
from sqlalchemy.exc import NoSuchModuleError

from plaidcloud.utilities import dialects

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2026, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


def test_every_required_dialect_loads_here():
    """The `test` extra installs all four drivers, so the real check must pass unmocked."""
    assert dialects.assert_dialects_available('plaidcloud-utilities CI', 'pyproject.toml') is None


def test_greenplum_is_not_required():
    """Greenplum is not a lakehouse engine; requiring its dead driver started sc-23700."""
    assert 'greenplum' not in dialects.REQUIRED_DIALECTS


def test_failure_names_the_image_the_dialect_the_package_and_the_requirements_file(monkeypatch):
    monkeypatch.setattr(dialects, 'REQUIRED_DIALECTS', {'starrocks': 'starrocks'})
    monkeypatch.setattr(
        dialects.registry, 'load',
        lambda name: (_ for _ in ()).throw(NoSuchModuleError(f"Can't load plugin: {name}")),
    )

    with pytest.raises(RuntimeError) as excinfo:
        dialects.assert_dialects_available('workflow-report', 'requirements-report.txt')

    message = str(excinfo.value)
    for expected in ('workflow-report', "'starrocks'", 'requirements-report.txt', "Can't load plugin"):
        assert expected in message, f'{expected!r} missing from the error a 3am reader gets:\n{message}'
