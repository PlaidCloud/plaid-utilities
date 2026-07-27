# coding=utf-8
"""Contract tests for data_helpers' optional-IPython handling (sc-23365)."""

import importlib
import sys

import pytest

from plaidcloud.utilities import data_helpers


def _block_ipython(monkeypatch):
    """Make `import IPython...` fail, as if the optional [jupyter] extra were not installed."""
    monkeypatch.setitem(sys.modules, 'IPython', None)
    monkeypatch.setitem(sys.modules, 'IPython.core', None)
    monkeypatch.setitem(sys.modules, 'IPython.core.display', None)


def test_data_helpers_imports_without_ipython(monkeypatch):
    """The module must import with IPython absent -- it is an optional extra, not a base dep."""
    _block_ipython(monkeypatch)
    monkeypatch.delitem(sys.modules, 'plaidcloud.utilities.data_helpers', raising=False)
    importlib.import_module('plaidcloud.utilities.data_helpers')


def test_jupyter_table_without_ipython_raises_actionable_error(monkeypatch):
    """Calling jupyter_table without IPython points the caller at the [jupyter] extra."""
    _block_ipython(monkeypatch)
    with pytest.raises(ImportError, match=r'plaidcloud-utilities\[jupyter\]'):
        data_helpers.jupyter_table('<b>hi</b>')
