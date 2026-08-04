# coding=utf-8
"""The build-time driver guard: what it requires, and what its failure says."""

import runpy

import pytest
from sqlalchemy.exc import NoSuchModuleError

from plaidcloud.utilities import dialects

_MODULE = 'plaidcloud.utilities.dialects'


def _unloadable(name):
    raise NoSuchModuleError(f"Can't load plugin: {name}")

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
    monkeypatch.setattr(dialects.registry, 'load', _unloadable)

    with pytest.raises(RuntimeError) as excinfo:
        dialects.assert_dialects_available('workflow-report', 'requirements-report.txt')

    message = str(excinfo.value)
    for expected in ('workflow-report', "'starrocks'", 'requirements-report.txt', "Can't load plugin"):
        assert expected in message, f'{expected!r} missing from the error a 3am reader gets:\n{message}'


def test_the_build_step_exits_with_the_message_and_no_traceback(monkeypatch):
    """What a Dockerfile RUN line actually does — a traceback here buries the message."""
    monkeypatch.setattr('sqlalchemy.dialects.registry.load', _unloadable)
    monkeypatch.setattr('sys.argv', ['dialects', 'workflow-solver', 'requirements-solver.txt'])

    with pytest.raises(SystemExit) as excinfo:
        runpy.run_module(_MODULE, run_name='__main__')

    assert 'workflow-solver' in str(excinfo.value)
    assert 'requirements-solver.txt' in str(excinfo.value)


def test_the_build_step_passes_when_every_driver_is_installed(monkeypatch, capsys):
    monkeypatch.setattr('sys.argv', ['dialects', 'rpc', 'requirements.txt'])

    runpy.run_module(_MODULE, run_name='__main__')

    assert capsys.readouterr().out.startswith('rpc: all SQL dialect drivers load')
