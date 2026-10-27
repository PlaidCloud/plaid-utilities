"""Data API for Panel apps: ``table("sales").filter(region=...).page(1, 200, sort=["-amount"])``.

Every statement is an ordinary SQLAlchemy Select sent through ``Connection.execute``; ORDER BY, LIMIT,
OFFSET and GROUP BY travel in the SQL text and the server applies the caller's row-level security.
Inside a Panel app a bare ``PlaidConnection()`` already authenticates as the opening user, so no
token handling happens here.
"""
import logging
import os

import sqlalchemy

logger = logging.getLogger(__name__)

PROJECT_ENV = 'PLAID_PANEL_PROJECT_ID'
_AGGREGATES = {'sum': sqlalchemy.func.sum, 'count': sqlalchemy.func.count, 'avg': sqlalchemy.func.avg,
               'min': sqlalchemy.func.min, 'max': sqlalchemy.func.max}


def table(name, project=None, conn=None):
    """A query handle on table `name`. `project` (id, name or path) overrides ``PLAID_PANEL_PROJECT_ID``."""
    if conn is None:
        from plaidcloud.utilities.connect import PlaidConnection
        project = project or os.environ.get(PROJECT_ENV)
        if not project:
            logger.warning(
                '%s is not set and no project was passed; using the connection default project. '
                'Pass table(name, project=...) to name one.', PROJECT_ENV,
            )
        conn = PlaidConnection(project_id=project) if project else PlaidConnection()
    return Table(conn, conn.get_table(name))


class Table:
    """Immutable: ``filter`` returns a new handle."""

    def __init__(self, conn, sa_table, conditions=()):
        self._conn = conn
        self._table = sa_table
        self._conditions = tuple(conditions)

    def _col(self, name):
        try:
            return self._table.c[name]
        except KeyError:
            raise ValueError(f'Table {self._table.name!r} has no column {name!r}') from None

    def filter(self, **values):
        """Equality per column; a list/tuple/set matches any member. ``None``, ``''`` and empty
        collections mean "no filter", so a blank FilterBar widget does not narrow the data."""
        conditions = list(self._conditions)
        for name, value in values.items():
            col = self._col(name)
            if value is None or value == '' or (isinstance(value, (list, tuple, set)) and not value):
                continue
            conditions.append(col.in_(list(value)) if isinstance(value, (list, tuple, set)) else col == value)
        return Table(self._conn, self._table, conditions)

    def _select(self, *entities):
        stmt = sqlalchemy.select(*entities)
        for condition in self._conditions:
            stmt = stmt.where(condition)
        return stmt

    def page(self, n=1, size=200, sort=(), key=None):
        """Page `n` (1-based) of `size` rows as a DataFrame.

        `sort` is column names, ``-name`` for descending. Offset paging is only stable under a total
        order, so `key` (the unique column(s), default every column) is appended after `sort`.
        """
        if n < 1 or size < 1:
            raise ValueError('page n and size must be >= 1')
        order = []
        seen = set()
        for item in sort:
            name = item.lstrip('-')
            order.append(self._col(name).desc() if item.startswith('-') else self._col(name))
            seen.add(name)
        key = [key] if isinstance(key, str) else (key or [c.name for c in self._table.c])
        order += [self._col(name) for name in key if name not in seen]
        stmt = self._select(self._table).order_by(*order).limit(size).offset((n - 1) * size)
        return self._conn.execute(stmt, return_df=True)

    def count(self):
        """Rows matching the filters."""
        rows = self._conn.execute(self._select(sqlalchemy.func.count().label('n')))
        return int(rows[0]['n'])

    def aggregate(self, by=(), limit=1000, **aggs):
        """``aggregate(by=['region'], total=('amount', 'sum'))`` -> one row per group, ordered by `by`.
        With no `by` it is a single totals row over the filtered set."""
        groups = [self._col(name) for name in by]
        measures = [_AGGREGATES[fn](self._col(col)).label(label) for label, (col, fn) in aggs.items()]
        stmt = self._select(*groups, *measures).group_by(*groups).order_by(*groups).limit(limit)
        return self._conn.execute(stmt, return_df=True)
