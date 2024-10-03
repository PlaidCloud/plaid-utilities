# coding=utf-8

import sqlalchemy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.ddl import _DropView, CreateColumn
try:
    from sqlalchemy.sql.ddl import _CreateBase as CreateBase
except:
    from sqlalchemy.sql.ddl import _CreateDropBase as CreateBase

from sqlalchemy.sql.compiler import Compiled
from sqlalchemy.sql import sqltypes


__author__ = 'Patrick Buxton'
__copyright__ = 'Copyright 2024-2024, Tartan Solutions, Inc'
__credits__ = ['Patrick Buxton']
__license__ = 'Apache 2.0'
__maintainer__ = 'Patrick Buxton'
__email__ = 'patrick.buxton@tartansolutions.com'


class CreateView(CreateBase):
    """
    Prepares a CREATE VIEW statement.

    See parameters in :class:`~sqlalchemy.sql.ddl.DDL`.

    Parameters
    ----------
    element: sqlalchemy.Table
        The view to create (sqlalchemy has no View construct)
    selectable: sqlalchemy.Selectable
        A query that evaluates to a table.
        This table defines the columns and rows in the view.
    or_replace: boolean
        If True, this definition will replace an existing definition.
        Otherwise, an exception will be raised if the view exists.
    options: dict
        Specify optional parameters for a view. For Postgresql, it translates
        into 'WITH ( view_option_name [= view_option_value] [, ... ] )'
    """

    __visit_name__ = "create_view"

    def __init__(
        self,
        element,
        selectable,
        materialized=False,
        if_not_exists=False,
        or_replace=False,
        options=None,
    ):
        super(CreateView, self).__init__(element, if_not_exists=if_not_exists)

        self.columns = [CreateColumn(column) for column in element.columns]
        self.selectable = selectable
        self.materialized = materialized
        self.or_replace = or_replace
        self.options = options


class DropView(_DropView):
    """
    Prepares a DROP VIEW statement.

    See parameters in :class:`~sqlalchemy.sql.ddl.DDL`.

    Parameters
    ----------
    element: sqlalchemy.Table
        The view to drop (sqlalchemy has no View construct)
    cascade: boolean
        Also drop any dependent views.
    if_exists: boolean
        Do nothing if the view does not exist.
        An exception will be raised for nonexistent views if not set.
    """

    __visit_name__ = "drop_view"

    def __init__(
        self,
        element,
        materialized=False,
        cascade=False,
        if_exists=False
    ):
        super(DropView, self).__init__(element, if_exists=if_exists)
        self.materialized = materialized
        self.cascade = cascade

# generic - postgres, greenplum
@compiles(CreateView)
def visit_create_view(create, compiler, **kw):
    view = create.element
    preparer = compiler.preparer
    text = "\nCREATE "
    if create.or_replace:
        text += "OR REPLACE "
    if create.materialized:
        text += "MATERIALIZED "
    text += "VIEW "
    if create.if_not_exists:
        text += "IF NOT EXISTS "
    text += "%s " % preparer.format_table(view)
    if create.columns:
        column_names = [preparer.format_column(col.element)
                        for col in create.columns]
        text += "("
        text += ', '.join(column_names)
        text += ") "

    if create.options:
        ops = []
        for opname, opval in create.options.items():
            ops.append('='.join([str(opname), str(opval)]))

        text += 'WITH (%s) ' % (', '.join(ops))

    compiled_selectable = (
        create.selectable
        if isinstance(create.selectable, Compiled)
        else compiler.sql_compiler.process(create.selectable, literal_binds=True)
    )
    text += "AS %s\n\n" % compiled_selectable
    return text


@compiles(CreateView, 'starrocks')
def visit_create_view_starrocks(create, compiler, **kw):
    view = create.element
    preparer = compiler.preparer
    text = "\nCREATE "
    if create.or_replace and not create.materialized:
        text += "OR REPLACE "
    if create.materialized:
        text += "MATERIALIZED "
    text += "VIEW "
    if create.if_not_exists:
        text += "IF NOT EXISTS "
    text += "%s " % preparer.format_table(view)
    if create.columns and not create.materialized:
        column_names = [preparer.format_column(col.element)
                        for col in create.columns]
        text += "("
        text += ', '.join(column_names)
        text += ") "

    opts = dict(
        (k[len(compiler.dialect.name) + 1 :].upper(), v)
        for k, v in view.kwargs.items()
        if k.startswith("%s_" % compiler.dialect.name)
    )

    if view.comment:
        comment = compiler.sql_compiler.render_literal_value(
            view.comment, sqltypes.String()
        )
        text += f"\nCOMMENT {comment}\n"
    if create.materialized:
        # ToDo distributed by
        if "REFRESH" in opts:
            text += f"\nREFRESH {opts['REFRESH']}\n" # - immediate/deferred async/async start every x/manual
        # ToDo partition by
        # ToDo order by

        if create.options:
            ops = []
            for opname, opval in create.options.items():
                ops.append(f'"{str(opname)}"="{str(opval)}"')
            text += 'PROPERTIES (%s) ' % (', '.join(ops))

    compiled_selectable = (
        create.selectable
        if isinstance(create.selectable, Compiled)
        else compiler.sql_compiler.process(create.selectable, literal_binds=True)
    )
    text += "AS %s\n\n" % compiled_selectable
    return text


@compiles(DropView)
def _drop_view(drop, compiler, **kw):
    text = "\nDROP "
    if drop.materialized:
        text += "MATERIALIZED "
    text += "VIEW "
    if drop.if_exists:
        text += "IF EXISTS "
    text += compiler.preparer.format_table(drop.element)
    if drop.cascade:
        text += " CASCADE"
    return text


@compiles(DropView, 'starrocks', 'databend')
def _drop_view(drop, compiler, **kw):
    text = "\nDROP "
    if drop.materialized:
        text += "MATERIALIZED "
    text += "VIEW "
    if drop.if_exists:
        text += "IF EXISTS "
    text += compiler.preparer.format_table(drop.element)
    # if drop.cascade:
    #     text += " CASCADE"
    return text

#
# def view_exists(ddl, target, connection, **kw):
#     return ddl.name in sa.inspect(connection).get_view_names()
#
#
# def view_doesnt_exist(ddl, target, connection, **kw):
#     return not view_exists(ddl, target, connection, **kw)
#
#
# def view(name, metadata, selectable):
#     t = table(name)
#
#     t._columns._populate_separate_keys(
#         col._make_proxy(t) for col in selectable.selected_columns
#     )
#
#     sa.event.listen(
#         metadata,
#         "after_create",
#         CreateView(name, selectable).execute_if(callable_=view_doesnt_exist),
#     )
#     sa.event.listen(
#         metadata, "before_drop", DropView(name).execute_if(callable_=view_exists)
#     )
#     return t