# coding=utf-8
"""The StarRocks dialect the runner compiles against.

The vendored ``starrocks`` package subclasses SQLAlchemy's MySQL dialect, and
MySQL gates ``CAST(x AS FLOAT/DOUBLE)`` on ``_support_float_cast`` — a property
derived from ``server_version_info``, which is only populated by connecting.
The runner compiles with an unconnected dialect
(``TransformHandler._get_dialect``), so that property reads False and
``MySQLCompiler.visit_cast`` drops the cast from the SQL *and* raises an
SAWarning. The dropped cast is silent wrong output; the SAWarning is collected
by ``TransformHandler.post`` and turns a correct step's badge to ``warn``.

StarRocks accepts both casts at every version we run, so the capability is
pinned on rather than inferred. It stays a subclass because the vendor class is
third-party — patching it in place would change behaviour for every consumer in
the process, including the dialect's own alembic paths.
"""

from starrocks.dialect import StarRocksDialect

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2014-2026, PlaidCloud, Inc'
__license__ = 'Proprietary'


class PlaidStarRocksDialect(StarRocksDialect):
    _support_float_cast = True
