#!/usr/bin/env python
# coding=utf-8

"""Utility library for sqlalchemy metaprogramming used in analyze transforms"""

import re
import uuid
from copy import deepcopy

from toolz.functoolz import juxt, compose, curry
from toolz.functoolz import identity as ident
from toolz.dicttoolz import merge, valfilter, assoc

import sqlalchemy
import sqlalchemy.orm
import sqlalchemy.dialects

from plaidcloud.rpc.type_conversion import sqlalchemy_from_dtype
from plaidcloud.utilities.stringtransforms import apply_variables
from plaidcloud.utilities import sqlalchemy_functions as sf  # Not unused import, it creates the SQLalchemy functions used


__author__ = 'Adams Tower'
__maintainer__ = 'Adams Tower <adams.tower@tartansolutions.com>'
__copyright__ = '© Copyright 2017-2023, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'

# TODO: move transform functions here, document them, and refactor their api

MAGIC_COLUMN_MAPPING = {
    'path': ':::DOCUMENT_PATH:::',
    'file_name': ':::FILE_NAME:::',
    'tab_name': ':::TAB_NAME:::',
    'last_modified': ':::LAST_MODIFIED:::',
    'source_row_number': ':::SOURCE_ROW_NUMBER:::',
    'source_table_name': ':::SOURCE_TABLE_NAME:::',
}

CSV_TYPE_DELIMITER = '::'
SCHEMA_PREFIX = 'anlz'
table_dot_column_regex = re.compile(r'^table(\d*)\..*')

class SQLExpressionError(Exception):
    # Will typically be caught by
    # workflow_runner.function.utility.transform_handler and converted into a
    # UserError
    pass


filter_nulls = curry(valfilter, lambda v: v is not None)


def eval_expression(expression: str, variables: dict|None, tables: list[sqlalchemy.Table], extra_keys: dict = None, disable_variables: bool = False, table_numbering_start: int= 1):
    safe_dict = get_safe_dict(tables, extra_keys, table_numbering_start=table_numbering_start)

    try:
        expression_with_variables = apply_variables(expression, variables)
    except:
        if disable_variables:
            expression_with_variables = expression
        else:
            raise

    compiled_expression = compile(expression_with_variables, '<string>', 'eval')

    try:
        return eval(compiled_expression, safe_dict)
    except Exception as e:
        message = str(e)
        raise SQLExpressionError(
            'Error in expression:\n'
            + '    {}\n'.format(expression)
            + message
        )


def on_clause(table_a: sqlalchemy.Table, table_b: sqlalchemy.Table, join_map: list[dict], special_null_handling: bool = False):
    """ Given two analyze tables, and a map of join keys with the structure:
    [{'a_column': COLUMN_NAME, 'b_column: COLUMN_NAME}...]
    returns a sqlalchemy clause filtering on those join keys, suitable for
    passing to sqlalchemy.join

    If special_null_handling is set to True, it will generate a clause suitable
    for a WHERE clause, so that it can be used in an anti-join subquery for
    example. Specifically, it will have extra checks to join on null columns.

    Example:

        join_query = select_query.select_from(sqlalchemy.join(
            table_a, table_b,
            on_clause(table_a, table_b, self.config['join_map'])
        ))

    """

    def column_a(jm):
        return next((c for c in table_a.columns if c.name == jm['a_column']), getattr(table_a.columns, jm['a_column']))

    def column_b(jm):
        return next((c for c in table_b.columns if c.name == jm['b_column']), getattr(table_b.columns, jm['b_column']))

    def column_a_equals_column_b(jm):
        return column_a(jm) == column_b(jm)

    def both_columns_null(jm):
        return sqlalchemy.and_(
            column_a(jm).is_(None),
            column_b(jm).is_(None),
        )

    def my_or(lst):
        # Calls sqlalchemy.or_ on a single argument list instead of an *args list
        return sqlalchemy.or_(*lst)

    if special_null_handling:
        column_join_expression = compose(my_or, juxt(
            column_a_equals_column_b,
            both_columns_null,
        ))
    else:
        column_join_expression = column_a_equals_column_b

    return sqlalchemy.and_(*[
        column_join_expression(jm)
        for jm in join_map
    ])


def get_column_table(source_tables: list[sqlalchemy.Table], target_column_config: dict, source_column_configs: list[list[dict]], table_numbering_start: int = 1):
    """Find the source table associated with a column."""

    if len(source_tables) == 1:  # Shortcut for most simple cases
        return source_tables[0]

    if target_column_config.get('source_table'):
        if target_column_config['source_table'].lower() in ('table a', 'table1'):
            return source_tables[0]
        if target_column_config['source_table'].lower() in ('table b', 'table2'):
            return source_tables[1]

    source_name = target_column_config['source']

    match = table_dot_column_regex.match(source_name)
    if match:  # They gave us a table number. Subtract 1 to find it's index
        if source_name.startswith('table.'):  # special case for just 'table'
            return source_tables[0]

        table_number = int(match.groups()[0])
        return source_tables[table_number - table_numbering_start]

    # None of our shortcuts worked, so look for the first table to have a
    # column of that name.
    for table, columns in zip(source_tables, source_column_configs):
        columnset = {c['source'] for c in columns}
        if source_name in columnset:
            return table

    # If nothing found at all:
    raise SQLExpressionError(f"Mapped source column {source_name} is not in any source tables.")


def process_fn(sort_type: bool|None, cast_type: type[sqlalchemy.types.TypeEngine]|None, agg_type: str|None, name: str, trim_type: bool|None = False):
    """Returns a function to apply to the source/constant/expression of a target column.
    sort_type, cast_type, and agg_type should be None if that kind of processing is not needed, or the appropriate type if it is.
    cast_type should be a sqlalchemy dtype,
    sort_type should be True (for ascending) or False (for descending),
    agg_type should be a string from the 'agg' param of the column.
    trim_type should be a boolean indicating if a trim should be applied

    Processing will always include applying the label in the param 'name'
    """
    if cast_type:
        cast_fn = curry(sqlalchemy.cast, type_=cast_type)
    else:
        cast_fn = ident

    agg_fn = get_agg_fn(agg_type)  # get_agg_fn returns ident for falsy agg_types

    # python has a thousand ways to express ternary branching (True, False, None), and none of them are quite clear
    if sort_type is True:
        sort_fn = sqlalchemy.asc
    elif sort_type is False:
        sort_fn = sqlalchemy.desc
    else:
        sort_fn = ident

    def label_fn(expr: sqlalchemy.ColumnElement):
        return expr.label(name)
    
    if trim_type:
        def trim_fn(expr: sqlalchemy.ColumnElement):
            return sqlalchemy.func.rtrim(sqlalchemy.func.rtrim(expr, '0'), '.')
    else:
        trim_fn = ident # type: ignore

    return compose(label_fn, sort_fn, trim_fn, cast_fn, agg_fn)

#TODO: write tests, though TestGetFromClause already covers this
def constant_from_clause(constant, sort_type: bool|None, cast_type: type[sqlalchemy.types.TypeEngine]|None, name: str, variables: dict = None, disable_variables: bool = False, trim_zeroes: bool = False):
    """Get a representation of a target column based on a constant. See process_fn & get_from_clause for explanation of arguments"""
    if disable_variables:
        var_fn = ident
    else:
        var_fn = curry(apply_variables, variables=variables)
    const = sqlalchemy.literal(var_fn(constant), type_=cast_type)

    # never aggregate
    return process_fn(sort_type, cast_type, None, name, trim_zeroes)(const)

#TODO: write tests, though TestGetFromClause already covers this
def expression_from_clause(expression: str, tables: list[sqlalchemy.Table], sort_type: bool|None, cast_type: type[sqlalchemy.types.TypeEngine]|None, agg_type: str|None, name: str, variables: dict = None, disable_variables: bool = False, table_numbering_start: int = 1, trim_zeroes: bool = False):
    """Get a representation of a target column based on an expression."""
    expr = eval_expression(
        expression.strip(),
        variables,
        tables,
        disable_variables=disable_variables,
        table_numbering_start=table_numbering_start,
    )
    return process_fn(sort_type, cast_type, agg_type, name, trim_zeroes)(expr)

#TODO: write tests, though TestGetFromClause already covers this
def source_from_clause(source: str, tables: list[sqlalchemy.Table], target_column_config: dict, source_column_configs: list[list[dict]], cast: bool, sort_type: bool|None, cast_type: type[sqlalchemy.types.TypeEngine]|None, agg_type: str|None, name: str, table_numbering_start: int = 1, trim_zeroes: bool = False):
    """Get a representation of a target column based on a source column."""
    table = get_column_table(tables, target_column_config, source_column_configs, table_numbering_start=table_numbering_start)

    if '.' in source:
        source_without_table = source.split('.', 1)[1]
    else:
        source_without_table = source  # a couple extra checks, in an error scenario, but flatter code

    #ADT2021: I'm really not sure whether this should also check the "aggregate" param
    if target_column_config.get('agg') == 'count_null':
        col = None
    elif source in table.columns:
        col = table.columns[source]
    elif source_without_table in table.columns:
        col = table.columns[source_without_table]
    else:
        raise SQLExpressionError(f'Cannot find source column {source} in table {table.name}')

    # cast can be turned off
    if cast:
        cancellable_cast_type = cast_type
    else:
        cancellable_cast_type = None

    return process_fn(sort_type, cancellable_cast_type, agg_type, name, trim_zeroes)(col)


def get_from_clause(
    tables: list[sqlalchemy.Table], target_column_config: dict, source_column_configs: list[list[dict]], aggregate: bool = False,
    sort: bool = False, variables: dict = None, cast: bool = True, disable_variables: bool = False, table_numbering_start: int = 1,
    sort_columns: list = None, use_row_number_for_serial: bool = True, trim_zeroes: bool = False
):
    """Given info from a config, returns a sqlalchemy expression representing a single target column."""

    expression = target_column_config.get('expression')
    constant = target_column_config.get('constant')
    source = target_column_config.get('source')

    name = target_column_config.get('target')
    cast_type = sqlalchemy_from_dtype(target_column_config.get('dtype'))

    if aggregate:
        agg_type = target_column_config.get('agg')
    else:
        agg_type = None

    if sort and 'sort' in target_column_config:
        sort_type = target_column_config['sort']['ascending']
    else:
        sort_type = None

    if constant:
        return constant_from_clause(constant, sort_type, cast_type, name, variables, disable_variables, trim_zeroes)
    if expression:
        return expression_from_clause(expression, tables, sort_type, cast_type, agg_type, name, variables, disable_variables, table_numbering_start, trim_zeroes)
    if source:
        return source_from_clause(source, tables, target_column_config, source_column_configs, cast, sort_type, cast_type, agg_type, name, table_numbering_start, trim_zeroes)
    if target_column_config.get('dtype') in {'serial', 'bigserial'}:
        if use_row_number_for_serial:
            return process_fn(sort_type, cast_type, agg_type, name, trim_zeroes)(sqlalchemy.func.row_number().over(order_by=sort_columns or []))
        return None

    if target_column_config.get('dtype') in set(MAGIC_COLUMN_MAPPING.keys()):
        if target_column_config.get('dtype') == 'source_table_name':
            # never aggregate
            return process_fn(sort_type, cast_type, None, name, trim_zeroes)(sqlalchemy.literal(tables[0].name, type_=cast_type))
        return None

    # If we get here...
    raise SQLExpressionError('Target Column {} needs either a Constant, an Expression or a Source Column!'.format(
        target_column_config.get('target')
    ))


def get_project_schema(project_id):
    if project_id.startswith(SCHEMA_PREFIX):
        return project_id

    return f'{SCHEMA_PREFIX}{project_id}'


def get_agg_fn(agg_str):
    """Mapping of aggregation strings to aggregation functions.
       Aggregation strings ending in '_null' will include nulls, but will resolve to the same aggregation name.
    """
    if not agg_str or agg_str in ['group', 'dont_group']:
        return ident

    if agg_str.endswith('_null'):
        return get_agg_fn(agg_str[:-5])

    if agg_str == 'count_distinct':
        return compose(sqlalchemy.func.count, sqlalchemy.func.distinct)

    return getattr(sqlalchemy.func, agg_str)


class Result(object):
    """This lets a user refer to the columns of the result of the initial query from within a HAVING clause"""

    def __init__(
        self, tables, target_columns, source_column_configs,
        aggregate=False, sort=False, variables=None, table_numbering_start=1
    ):
        self.__dict__ = {
            tc['target']: get_from_clause(
                tables,
                tc,
                source_column_configs,
                aggregate,
                sort,
                variables=variables,
                table_numbering_start=table_numbering_start,
            )
            for tc in target_columns
            if tc['dtype'] not in ('serial', 'bigserial')
        }


def get_safe_dict(tables: list[sqlalchemy.Table], extra_keys: dict|None = None, table_numbering_start: int = 1):
    """Returns a dict of 'builtins' and table accessor variables for user
    written expressions."""
    extra_keys = extra_keys or {}

    def get_column(table, col):
        if col in table:
            return table[col]

        # Obtaining the table here would be really ugly. table refers to a
        # table.columns object. We could maybe change it to some extension of whatever the table.columns object is
        raise SQLExpressionError(f'Could not run get_column: column {repr(col)} does not exist.')

    default_keys = {
        'sqlalchemy': sqlalchemy,
        'and_': sqlalchemy.and_,
        'or_': sqlalchemy.or_,
        'not_': sqlalchemy.not_,
        'cast': sqlalchemy.cast,
        'case': sqlalchemy.case,
        'Null': None,
        'null': None,
        'NULL': None,
        'true': True,
        'TRUE': True,
        'false': False,
        'FALSE': False,
        'get_column': get_column,
        # 'func': FuncPlus(),
        'func': sqlalchemy.func,
        'value': sqlalchemy.literal,
        'v': sqlalchemy.literal,
        'bigint': sqlalchemy.BIGINT,
        'Bigint': sqlalchemy.BIGINT,
        'BIGINT': sqlalchemy.BIGINT,
        'float': sqlalchemy.Float,
        'Float': sqlalchemy.Float,
        'FLOAT': sqlalchemy.Float,
        'integer': sqlalchemy.INTEGER,
        'Integer': sqlalchemy.INTEGER,
        'INTEGER': sqlalchemy.INTEGER,
        'smallint': sqlalchemy.SMALLINT,
        'Smallint': sqlalchemy.SMALLINT,
        'SMALLINT': sqlalchemy.SMALLINT,
        'text': sqlalchemy.TEXT,
        'Text': sqlalchemy.TEXT,
        'TEXT': sqlalchemy.TEXT,
        'boolean': sqlalchemy.BOOLEAN,
        'Boolean': sqlalchemy.BOOLEAN,
        'BOOLEAN': sqlalchemy.BOOLEAN,
        'numeric': sqlalchemy.NUMERIC,
        'Numeric': sqlalchemy.NUMERIC,
        'NUMERIC': sqlalchemy.NUMERIC,
        'timestamp': sqlalchemy.TIMESTAMP,
        'Timestamp': sqlalchemy.TIMESTAMP,
        'TIMESTAMP': sqlalchemy.TIMESTAMP,
        'interval': sqlalchemy.Interval,
        'Interval': sqlalchemy.Interval,
        'INTERVAL': sqlalchemy.Interval,
        'date': sqlalchemy.Date,
        'Date': sqlalchemy.Date,
        'DATE': sqlalchemy.Date,
        'time': sqlalchemy.Time,
        'Time': sqlalchemy.Time,
        'TIME': sqlalchemy.Time,
        'binary': sqlalchemy.LargeBinary,
        'Binary': sqlalchemy.LargeBinary,
        'BINARY': sqlalchemy.LargeBinary,
        'largebinary': sqlalchemy.LargeBinary,
        'Largebinary': sqlalchemy.LargeBinary,
        'LargeBinary': sqlalchemy.LargeBinary,
        'LARGEBINARY': sqlalchemy.LargeBinary,
        'uuid': sqlalchemy.dialects.postgresql.UUID,
        'Uuid': sqlalchemy.dialects.postgresql.UUID,
        'UUID': sqlalchemy.dialects.postgresql.UUID,
        'json': sqlalchemy.JSON,
        'Json': sqlalchemy.JSON,
        'JSON': sqlalchemy.JSON,
    }

    # Only put in the table key if we have a table
    # this gives a better error for post-filtering where we use 'result' instead of 'table'
    if tables:
        default_keys['table'] = tables[0].columns
    # Generate table1, table2, ...
    table_keys = {f'table{n}': table.columns for n, table in enumerate(tables, start=table_numbering_start)}

    return merge(default_keys, table_keys, extra_keys)


def get_table_rep(table_id: str, columns: list[dict], schema: str, metadata: sqlalchemy.MetaData|None = None, column_key: str = 'source', alias: str|None = None) -> sqlalchemy.Table|sqlalchemy.FromClause:
    """
    Returns:
        sqlalchemy.Table: object representing an analyze table
    Args:
        table_id (str): the name of the table in the database
        columns: a list of dicts (in transform config style) columns in the analyze table
        schema (str): the schema of the table
        metadata (sqlalchemy.MetaData): a sqlalchemy metadata object, to keep this table representation connected to others
        column_key (str): the key in each column dict under which to look for the column name
        alias (str, optional): If supplied, the SQL query will use the alias to make more human readable
    """
    if not table_id:
        raise SQLExpressionError('Cannot create sqlalchemy representation of a table without a table name.')

    if not metadata:
        metadata = sqlalchemy.MetaData()

    table = sqlalchemy.Table(
        table_id,
        metadata,
        *[
            sqlalchemy.Column(
                sc[column_key],
                sqlalchemy_from_dtype(sc['dtype']),
            )
            for sc in columns
        ],
        schema=schema,
        extend_existing=True,  # If this is the second object representing this
                               # table, update.
                               # If you made it with this function, it should
                               # be no different.
    )

    if alias:
        return table.alias(name=alias)

    return table


def get_table_rep_using_id(table_id: str, columns: list[dict], project_id: str, metadata: sqlalchemy.MetaData|None = None, column_key: str = 'source', alias: str = None) -> sqlalchemy.Table|sqlalchemy.FromClause:
    """
    Returns:
        sqlalchemy.Table: object representing an analyze table
    Args:
        table_id (str): the id of the analyze table
        columns: a list of dicts (in transform config style) columns in the analyze table
        project_id (str): the project id of the project containing the analyze table
        metadata (sqlalchemy.MetaData): a sqlalchemy metadata object, to keep this table representation connected to others
        column_key (str): the key in each column dict under which to look for the column name
        alias (str, optional): If supplied, the SQL query will use the alias to make more human readable
    """

    schema = get_project_schema(project_id)

    return get_table_rep(table_id, columns, schema, metadata, column_key, alias)


def simple_select_query(config: dict, project: str, metadata: sqlalchemy.MetaData|None, variables: dict|None):
    """Returns a select query from a single extract config, with a single
    source table, and standard key names."""

    # Make a sqlalchemy representation of the source table
    from_table = get_table_rep_using_id(
        config['source'], config['source_columns'],
        project, metadata, alias=config.get('source_alias'),
    )

    # Figure out select query
    return get_select_query(
        tables=[from_table],
        source_columns=[config['source_columns']],
        target_columns=config['target_columns'],
        wheres=[config.get('source_where')],
        config=config, variables=variables,
    )


def modified_select_query(config, project, metadata, fmt=None, mapping_fn=None, variables=None):
    """Similar to simple_select_query, but accepts a config with consistently
    modified keys. E.g., source_b, source_columns_b, aggregate_b, etc.

    Can be provided with either a fmt string, like '{}_b', or a mapping_fn,
    like lambda x: x.upper(). In either case, the fmt or mapping_fn should
    convert the standard key (e.g. 'source') to the unusual key (e.g. 'source_b').

    If a modified key is not found in the config, it will default to the value
    of the unmodified key. E.g., if there's no 'source_b', it will take
    'source'. After that it will use the normal default value if there is one.

    If both a mapping_fn and a fmt are provided, fmt will be ignored, and
    mapping_fn will be used.
    """

    # The args we'll be modifying and searching the config for.
    required_args = (
        'source', 'source_columns', 'target_columns', 'source_where',
        'aggregate', 'having', 'use_target_slicer', 'limit_target_start',
        'limit_target_end', 'distinct', 'source_alias',
    )

    # If there's no mapping_fn, turn fmt into a mapping_fn.
    if mapping_fn is None:
        if fmt is None:
            raise SQLExpressionError("modified_select_query must be called with either a"
                                     " fmt or a mapping_fn!")

        # A function that formats a string with the provided fmt.
        def format_with_fmt(s): return fmt.format(s)
        mapping_fn = format_with_fmt

    # Generate a fake config, taking the value of the modified keys from the
    # original config, or if those don't exist the value of the regular keys
    # from the original config.
    cleaned_config = filter_nulls({
        arg: config.get(mapping_fn(arg), config.get(arg))
        for arg in required_args
    })

    return simple_select_query(cleaned_config, project, metadata, variables)


def get_select_query(
    tables: list[sqlalchemy.Table], source_columns: list[list[dict]], target_columns: list[dict], wheres: list[str],
    config: dict = None, variables: dict = None, aggregate: bool = None, having: str = None,
    use_target_slicer: bool = None, limit_target_start: int = None, limit_target_end: int = None,
    distinct: bool = None, count: bool = None, disable_variables: bool = None, table_numbering_start: int = 1,
    use_row_number_for_serial: bool = True, aggregation_type: str = 'group', cast: bool = True, trim_zeroes: bool = False
):
    """Returns a sqlalchemy select query from table objects and an extract
    config (or from the individual parameters in that config). tables,
    source_columns, and wheres should be lists, so that multiple tables can be
    joined. If they have more than one element, tables[n] corresponds to
    source_columns[n] corresponds to wheres[n].

    Args:
        tables:
        source_columns:
        target_columns:
        wheres:
        config:
        variables:
        aggregate:
        having:
        use_target_slicer:
        limit_target_start:
        limit_target_end:
        distinct:
        count:
        disable_variables:
        table_numbering_start:
        use_row_number_for_serial:
        aggregation_type: One of 'group', 'rollup', 'sets'
        cast: if the query should attempt to cast source columns
        trim_zeroes (bool, optional): If True, removes trailing zeroes from numeric fields

    Returns:

    """

    def fill_in(var, var_name, default):
        if var is not None:
            return var
        return config.get(var_name, default)

    config = config or {}
    aggregate = fill_in(aggregate, 'aggregate', False)
    having = fill_in(having, 'having', None)
    use_target_slicer = fill_in(use_target_slicer, 'use_target_slicer', False)
    limit_target_start = fill_in(limit_target_start, 'limit_target_start', 0)
    limit_target_end = fill_in(limit_target_end, 'limit_target_end', 0)
    distinct = fill_in(distinct, 'distinct', False)
    count = fill_in(count, 'count', False)
    disable_variables = fill_in(disable_variables, 'disable_variables', False)
    aggregation_type = fill_in(aggregation_type, 'aggregation_type', 'group')

    # Find any columns for sorting, find these up front such that they may be used if a serial column is present
    columns_to_sort_on = [
        stc
        for stc in target_columns
        if (
            stc.get('dtype') not in ('serial', 'bigserial')
            and stc.get('sort')
            and stc['sort'].get('ascending') is not None
        )
    ]
    sort_columns = None
    if columns_to_sort_on:
        columns_with_order = [tc for tc in columns_to_sort_on if 'order' in tc['sort']]
        columns_without_order = [tc for tc in columns_to_sort_on if 'order' not in tc['sort']]
        sort_order = sorted(columns_with_order, key=lambda tc: tc['sort']['order']) + columns_without_order
        sort_columns = [
            get_from_clause(
                tables,
                tc,
                source_columns,
                aggregate,
                sort=True,
                variables=variables,
                disable_variables=disable_variables,
                table_numbering_start=table_numbering_start,
                cast=cast
            )
            for tc in sort_order
        ]

    # Build SELECT x FROM y section of our select query
    if count:
        # Much simpler for one table.
        # TODO: figure out how to do this for more than one table
        select_query = sqlalchemy.select(sqlalchemy.func.count()).select_from(tables[0])
    else:
        column_select = [
            get_from_clause(
                tables,
                tc,
                source_columns,
                aggregate,
                variables=variables,
                disable_variables=disable_variables,
                table_numbering_start=table_numbering_start,
                sort_columns=sort_columns,
                use_row_number_for_serial=use_row_number_for_serial,
                cast=cast,
                trim_zeroes=trim_zeroes,
            )
            for tc in target_columns
            if (use_row_number_for_serial or tc['dtype'] not in ('serial', 'bigserial'))
        ]

        select_query = sqlalchemy.select(*column_select)

    # Build WHERE section of our select query
    wheres = wheres or []
    combined_wheres = get_combined_wheres(
        wheres, tables, variables, disable_variables, table_numbering_start=table_numbering_start
    )
    if combined_wheres:
        select_query = select_query.where(*combined_wheres)

    # If there are any, build ORDER BY section of our select query
    if sort_columns:
        select_query = select_query.order_by(*sort_columns)

    # Build GROUP BY section of our select query.
    if aggregate:
        grouping_columns = [
            get_from_clause(
                tables,
                tc,
                source_columns,
                False,
                variables=variables,
                cast=False,
                disable_variables=disable_variables,
                table_numbering_start=table_numbering_start,
                use_row_number_for_serial=use_row_number_for_serial,
                trim_zeroes=trim_zeroes,
            )
            for tc in target_columns
            if (
                tc.get('agg') in ('group', 'group_null')
                and not tc.get('constant')
                and (use_row_number_for_serial or not tc.get('dtype') in ('serial', 'bigserial'))
            )
        ]
        if aggregation_type == 'rollup':
            select_query = select_query.group_by(sqlalchemy.func.rollup(*grouping_columns))
        elif aggregation_type == 'sets':
            select_query = select_query.group_by(sqlalchemy.func.grouping_sets(*grouping_columns))
        elif aggregation_type == 'cube':
            select_query = select_query.group_by(sqlalchemy.func.cube(*grouping_columns))
        else:
            select_query = select_query.group_by(*grouping_columns)

    # Build DISTINCT section of our select query
    if distinct:
        # if any([tc for tc in target_columns if not tc.get('distinct')]):
        #     raise Exception('Distinct cannot be used if all columns are not distinct')
        # every other database way
        select_query = select_query.distinct()
        # postgres way of doing distinct (*args) will become CompileError in future
        # select_query = select_query.distinct(
        #     *[
        #         get_from_clause(
        #             tables,
        #             tc,
        #             source_columns,
        #             aggregate,
        #             variables=variables,
        #             disable_variables=disable_variables,
        #             table_numbering_start=table_numbering_start,
        #             use_row_number_for_serial=use_row_number_for_serial,
        #         )
        #         for tc in target_columns
        #         if not tc.get('constant')
        #         and tc.get('distinct')
        #         and (use_row_number_for_serial or not tc.get('dtype') in ('serial', 'bigserial'))
        #     ]
        # )

    # HAVING
    if having:
        # CRL 2020 - HAVING clause is now a second select query to apply post-query filters on.
        select_query = apply_output_filter(select_query, having, variables)

    # Build LIMIT and OFFSET sections of our select query
    if use_target_slicer:
        off = limit_target_start
        lim = limit_target_end - off

        select_query = select_query.limit(lim).offset(off)

    return select_query


def get_insert_query(target_table, target_columns, select_query, use_row_number_for_serial: bool = True):
    """Returns a sqlalchemy insert query, given a table object, target_columns
    config, and a sqlalchemy select query."""
    return target_table.insert().from_select(
        [tc['target'] for tc in target_columns if tc.get('dtype') not in ('serial', 'bigserial') or use_row_number_for_serial],
        select_query,
    )


def get_update_value(tc, table, dtype_map, variables):
    """returns (include, val) where val should be used in the query if include is True, but filtered out if not"""
    dtype = dtype_map.get(tc['source'], 'text')
    if dtype == 'text':
        def conditional_empty_string_fn(val):
            # Transforms None into '', but only if dtype is 'text'
            if val is None:
                return ''
            return val
    else:
        conditional_empty_string_fn = ident

    if tc.get('nullify'):
        return (True, None)
    if tc.get('constant'):
        return (True, conditional_empty_string_fn(sqlalchemy.literal(apply_variables(tc['constant'], variables), type_=sqlalchemy_from_dtype(dtype))))
    if tc.get('expression'):
        return (True, conditional_empty_string_fn(eval_expression(tc['expression'].strip(), variables, [table])))
    if dtype == 'text':
        # Special condition for empty string
        # It works this way because it's impossible to type 'constant': '' in the UI
        return True, ''

    # If none of our conditions are true, then this column shouldn't be included in the values dict
    return False, None


def get_update_query(table, target_columns, wheres, dtype_map, variables=None):
    update_query = sqlalchemy.update(table)

    combined_wheres = get_combined_wheres(wheres, [table], variables)
    if combined_wheres:
        update_query = update_query.where(*combined_wheres)

    # Build values dict
    values = {
        col_name: value
        for col_name, include, value in [
            (tc['source'],) + get_update_value(tc, table, dtype_map, variables)
            for tc in target_columns
        ]
        if include
    }

    return update_query.values(values)


def get_delete_query(table, wheres, variables=None):
    delete_query = sqlalchemy.delete(table)

    combined_wheres = get_combined_wheres(wheres, [table], variables)
    if combined_wheres:
        delete_query = delete_query.where(*combined_wheres)

    return delete_query


def clean_where(w):
    return w.strip().replace('\n', '').replace('\r', '')


def get_combined_wheres(wheres, tables, variables, disable_variables=False, table_numbering_start=1):
    return [
        eval_expression(
            clean_where(where),
            variables,
            tables,
            disable_variables=disable_variables,
            table_numbering_start=table_numbering_start,
        )
        for where in wheres
        if where
    ]


def apply_output_filter(original_query, filter_where: str, variables: dict = None):
    variables = variables or {}
    original_query = original_query.subquery('result')
    where_clause = eval_expression(clean_where(filter_where), variables, [], extra_keys={'result': original_query.columns})
    return sqlalchemy.select(*original_query.columns).where(where_clause)


def import_data_query(
    project_id, target_table_id, source_columns, target_columns, date_format='',
    trailing_negatives=False, config=None, variables=None, temp_table_id=None,
):
    """Provides a SQLAlchemy insert query to transfer data from a text import temporary table into the target table.

    Notes:
    Firstly csv is imported into a temporary text table, then it is extracted into the final table via some
    default conversion/string trimming expression. If an expression is provided, it will override the default expression
    The default expression is:
        func.import_col(col, dtype, date_format, trailing_negs)
    which will provide the necessary transformation for each column based on data type

    Args:
        project_id (str): The unique Project Identifier
        target_table_id (str): The target table for the import
        source_columns (list): The list of source columns
        target_columns (list): The list of target columns
        date_format (str, optional): The default date format
        trailing_negatives (bool, optional): Whether to handle trailing negatives in numbers
        config (dict, optional): The step configuration from which filtering/grouping settings can be used
        variables (dict, optional): Variables to use in the query

    Returns:
        sqlalchemy.sql.expression.Insert: The query to import data from the temporary text table to the target table
    """
    metadata = sqlalchemy.MetaData()
    target_columns = deepcopy(target_columns)  # bandaid to avoid destructiveness

    temp_table_id = temp_table_id or f'temp_{str(uuid.uuid4())}'
    temp_table_columns = [
        {
            'source': s.get('source', s.get('name', s.get('id'))),
            'dtype': 'text',
        }
        for s in source_columns
    ] + [
        {'source': MAGIC_COLUMN_MAPPING[k], 'dtype': k}
        for k in MAGIC_COLUMN_MAPPING
    ]

    target_meta = [{'id': t['target'], 'dtype': t['dtype']} for t in target_columns]

    def processed_target_column(tc):
        def add_expression(tc):
            return assoc(
                tc,
                'expression',
                tc.get('expression')
                or f"""func.import_col(get_column(table, {repr(tc['source'])}), {repr(tc['dtype'])}, '{date_format}', {trailing_negatives or False})""",
            )

        def add_magic_column_source(tc):
            return assoc(
                tc, 'source', MAGIC_COLUMN_MAPPING.get(tc['dtype'], tc.get('source'))
            )
        return compose(
            add_expression,
            add_magic_column_source,
        )(tc)

    processed_target_columns = [processed_target_column(tc) for tc in target_columns]

    from_table = get_table_rep_using_id(
        temp_table_id,
        temp_table_columns,
        project_id,
        metadata,
        alias='text_import',
    )

    config = config or {}
    select_query = get_select_query(
        tables=[from_table],
        source_columns=[temp_table_columns],
        target_columns=processed_target_columns,
        wheres=[config.get('source_where')],
        config=config,
        variables=variables,
    )

    # Get the target table rep
    new_table = get_table_rep_using_id(
        target_table_id,
        target_meta,
        project_id,
        metadata,
        column_key='id',
    )

    # Figure out the insert query, based on the select query
    return get_insert_query(new_table, processed_target_columns, select_query)


def allocate(
    source_query, driver_query, allocate_columns, numerator_columns, denominator_columns, driver_value_column,
    overwrite_cols_for_allocated=True, include_source_columns=None, unique_cte_index=1,
    parent_context_queries: dict = None
):
    """Performs an allocation based on the provided sqlalchemy source and driver data queries

    Args:
        source_query (sqlalchemy.Select): Sqlalchemy query for source data
        driver_query (sqlalchemy.Select): Sqlalchemy query for driver data
        allocate_columns (list): List of columns to apply a shred to
        numerator_columns (list): List of columns to use as numerator
        denominator_columns (list): List of columns to use as denominator
        driver_value_column (str): Column name for the driver value
        overwrite_cols_for_allocated (bool): Whether to overwrite the source columns with the allocated value, if False, cols suffixed with _allocated are created
        include_source_columns (list, optional): Columns for which we should include a *_source column (for reassignments with multiple allocation steps)
        unique_cte_index (int, optional): Unique index to use in the common table expressions if more than one
            allocation will be done within the same query
        parent_context_queries (dict, optional): Dict of queries for use with 'Parent' driver data {col: {'PARENT_CHILD': Selectable, 'LEAVES': Selectable}}

    Returns:
        sqlalchemy.Selectable: A Sqlalchemy query representing the allocation
    """
    all_target_columns = [col.name for col in source_query.selected_columns]
    reassignment_columns = [col.name for col in source_query.selected_columns if col.name in numerator_columns]

    all_driver_columns = [col.name for col in driver_query.selected_columns if col.name in numerator_columns + denominator_columns + [driver_value_column]]

    driver_value_columns = [driver_value_column]  # set up for the *possibility* of multiple driver value columns, not sure it makes sense though
    driver_count = len(driver_value_columns)

    include_source_columns = include_source_columns or []
    parent_context_queries = parent_context_queries or {}

    # Denominator table is SUM of split values with GROUP BY denominator columns
    # Join the Denominator values to the numerator values to get a % split (add an alloc indicator of 1 in a fake column)
    # Outer join the source table to the split % info with a alloc column that is coalesce(driver alloc column, 0).  This produces a 1 if it allocated.
    allocable_col = 'allocable'
    if allocable_col not in all_target_columns:
        source_query = source_query.add_columns(sqlalchemy.literal(1).label(allocable_col))

    def _get_shred_col_name(col):
        return f'shred_{col}' if driver_count > 1 else 'shred'

    def _get_allocated_col_name(col):
        return col if overwrite_cols_for_allocated else f'{col}_allocated'

    def _get_parent_col(col: str = ''):
        return f'Parent{col}'

    def _get_child_col(col: str = ''):
        return f'Child{col}'

    # def _join_parent_dim(sel, left_table, col):
    #     return sel.join_from(
    #         left_table,
    #         parent_cte_dict[col],
    #         parent_cte_dict[col].columns['Child'] == left_table.columns[col]
    #     )

    def _join_parent_leaves(sel, left_table, col):
        return sel.join_from(
            left_table,
            leaves_cte_dict[col],
            leaves_cte_dict[col].columns['Leaf'] == left_table.columns[col]
        )

    def _join_parent_child(sel, col):
        return sel.join_from(
            leaves_cte_dict[col],
            parent_cte_dict[col],
            parent_cte_dict[col].columns['Parent'] == leaves_cte_dict[col].columns['Node']
        )

    cte_source = source_query.cte(f'alloc_source_{unique_cte_index}')
    cte_driver = driver_query.cte(f'alloc_driver_{unique_cte_index}')

    parent_context_columns = parent_context_queries.keys()
    parent_cte_dict = {}
    leaves_cte_dict = {}
    # This assumes that the PARENT_CHILD contains 'Parent' & 'Child' columns
    # and LEAVES contains 'Node', 'Leaf' columns
    if parent_context_queries:
        for col, queries in parent_context_queries.items():
            parent_cte_dict[col] = queries['PARENT_CHILD'].cte(f'parent_{col}_{unique_cte_index}')
            leaves_cte_dict[col] = queries['LEAVES'].cte(f'leaves_{col}_{unique_cte_index}')

        parent_driver_select = sqlalchemy.select(
            * [col for col in cte_driver.columns if col.name not in parent_context_columns]
            + [
                  parent_cte_dict[col].columns[_get_child_col()].label(col)
                  for col in parent_context_columns
              ]
        )
        for col in parent_context_columns:
            parent_driver_select = _join_parent_leaves(parent_driver_select, cte_driver, col)
            parent_driver_select = _join_parent_child(parent_driver_select, col)

        cte_parent_driver = parent_driver_select.cte(f'parent_driver_{unique_cte_index}')

        cte_consol_driver = (
            sqlalchemy.select(
                * [cte_parent_driver.columns[d] for d in numerator_columns + denominator_columns]
                + [sqlalchemy.func.sum(cte_parent_driver.columns[d]).label(d) for d in driver_value_columns]
            )
            .where(cte_parent_driver.columns[driver_value_column] != 0)
            .group_by(* [cte_parent_driver.columns[d] for d in numerator_columns + denominator_columns])
            .cte(f'consol_driver_{unique_cte_index}')
        )
    else:
        cte_consol_driver = (
            sqlalchemy.select(
                * [cte_driver.columns[d] for d in numerator_columns + denominator_columns]
                + [sqlalchemy.func.sum(cte_driver.columns[d]).label(d) for d in driver_value_columns]
            )
            .where(cte_driver.columns[driver_value_column] != 0)
            .group_by(*[cte_driver.columns[d] for d in numerator_columns + denominator_columns])
            .cte(f'consol_driver_{unique_cte_index}')
        )

    cte_denominator = (
        sqlalchemy.select(
            * [cte_consol_driver.columns[d] for d in denominator_columns]
            + [sqlalchemy.func.sum(cte_consol_driver.columns[d]).label(d) for d in driver_value_columns]
        )
        .group_by(*[cte_consol_driver.columns[d] for d in denominator_columns])
        .cte(f'denominator_{unique_cte_index}')
    )

    cte_ratios = (
        sqlalchemy.select(
            * [cte_consol_driver.columns[d] for d in denominator_columns + numerator_columns + driver_value_columns]
            + [
                # set ratio to null if the denominator or numerator is zero, this allows pass-through of value to be allocated
                sqlalchemy.func.cast(
                    (
                        sqlalchemy.func.nullif(sqlalchemy.func.cast(cte_consol_driver.columns[d], sqlalchemy.NUMERIC(38, 20)), 0) /
                        sqlalchemy.func.nullif(sqlalchemy.func.cast(cte_denominator.columns[d], sqlalchemy.NUMERIC(38, 20)), 0)
                    ),
                    sqlalchemy.NUMERIC(38, 20),  # We need a large scale here to calc allocation correctly
                ).label(_get_shred_col_name(d))
                for d in driver_value_columns
            ]
        )
        .select_from(
            sqlalchemy.join(
                cte_consol_driver,
                cte_denominator,
                sqlalchemy.and_(sqlalchemy.true()) if not denominator_columns else
                sqlalchemy.and_(
                    *[cte_consol_driver.columns[dn] == cte_denominator.columns[dn] for dn in denominator_columns]
                ),
            )
        )
        .cte(f'ratios_{unique_cte_index}')
    )

    def _is_source_col(col):
        return col not in set(reassignment_columns + (allocate_columns if overwrite_cols_for_allocated else []))

    ratio_cols = [
        dt
        for dt in numerator_columns + denominator_columns + [driver_value_column]
        if dt in all_driver_columns
        and dt not in set(all_target_columns + reassignment_columns)
    ]

    allocation_select = sqlalchemy.select(
        * [cte_source.columns[tc] for tc in all_target_columns if _is_source_col(tc)]
        + [cte_source.columns[tc].label(f'{tc}_source') for tc in all_target_columns if tc in include_source_columns]
        + [
            sqlalchemy.case(
                (
                    cte_ratios.columns[_get_shred_col_name(driver_value_columns[0])].isnot(sqlalchemy.null()),
                    sqlalchemy.func.coalesce(cte_ratios.columns[rc], cte_source.columns[rc]),
                ),
                else_=cte_source.columns[rc],
            ).label(rc)
            for rc in reassignment_columns
        ]
        + [
            cte_ratios.columns[dt]
            for dt in ratio_cols
        ]
        + [
            sqlalchemy.case(
                (cte_ratios.columns[_get_shred_col_name(driver_value_columns[0])].isnot(sqlalchemy.null()), 1), else_=0
            ).label('alloc_status')
        ]
        + [cte_ratios.columns[_get_shred_col_name(d)] for d in driver_value_columns]
        + [
            sqlalchemy.case(
                # pass through source value if driver value is null (not found, not allocable, divide by zero)
                (cte_ratios.columns[_get_shred_col_name(d)].is_(sqlalchemy.null()), cte_source.columns[ac]),
                else_=cte_ratios.columns[_get_shred_col_name(d)] * cte_source.columns[ac],
            ).label(_get_allocated_col_name(ac))
            for ac in allocate_columns
            for d in driver_value_columns
        ]
    ).where(
        cte_source.columns[allocable_col] == 1
    )

    allocation_select = allocation_select.join_from(
        cte_source,
        cte_ratios,
        sqlalchemy.and_(sqlalchemy.true()) if not denominator_columns else
        sqlalchemy.and_(
            * [
                 cte_source.columns[dn] == cte_ratios.columns[dn]
                 for dn in denominator_columns
             ]
        ),
        isouter=True
    )

    allocation_select = allocation_select.union_all(
        sqlalchemy.select(
            * [cte_source.columns[tc] for tc in all_target_columns if _is_source_col(tc)]
            + [cte_source.columns[tc].label(f'{tc}_source') for tc in all_target_columns if tc in include_source_columns]
            + [cte_source.columns[rc] for rc in reassignment_columns]
            + [
                cte_ratios.columns[dt]
                for dt in ratio_cols
            ]
            + [sqlalchemy.literal(0, type_=sqlalchemy.Integer).label('alloc_status')]
            + [
                sqlalchemy.func.cast(sqlalchemy.literal(None), sqlalchemy.Numeric).label(_get_shred_col_name(d))
                for d in driver_value_columns
            ]
            + [cte_source.columns[ac].label(_get_allocated_col_name(ac)) for ac in allocate_columns]
        )
        .where(cte_source.columns[allocable_col] == 0)
        .join_from(
            cte_source,
            cte_ratios,
            sqlalchemy.false(),
            isouter=True
        )
    )
    return allocation_select



def eval_rule(rule: str, variables: dict, tables: list, extra_keys=None, disable_variables=False, table_numbering_start=1):
    safe_dict = get_safe_dict(tables, extra_keys, table_numbering_start=table_numbering_start)

    try:
        expression_with_variables = apply_variables(rule, variables)
    except:
        if disable_variables:
            expression_with_variables = rule
        else:
            raise

    compiled_expression = compile(expression_with_variables, '<string>', 'eval')

    try:
        return eval(compiled_expression, safe_dict)
    except Exception as e:
        raise SQLExpressionError(
            f'Error in rule evaluation:\n{rule}\n' + str(e)
        )


def apply_rules(source_query, df_rules, rule_id_column, target_columns=None, include_once=True, show_rules=False,
                verbose=True, unmatched_rule='UNMATCHED', condition_column='condition', iteration_column='iteration',
                logger=None):
    """
    If include_once is True, then condition n+1 only applied to records left after condition n.
    Adding target column(s), plural, because we'd want to only run this operation once, even
    if we needed to set multiple columns.

    Args:
        source_query (sqlalchemy.Select): The Query to apply rules on
        df_rules (pandas.DataFrame): A list of rules to apply
        rule_id_column (str): Column name containing the rule id
        target_columns (list of str, optional): The target columns to apply rules on.
        include_once (bool, optional): Should records that match multiple rules
            be included ONLY once? Defaults to `True`
        show_rules (bool, optional): Display the rules in the result data? Defaults to `False`
        verbose (bool, optional): Display the rules in the log messages? Defaults
            to `True`.  This is not overly heavier than leaving it off, so we probably should
            always leave it on unless logging is off altogether.
        unmatched_rule (str, optional): Default rule to write in cases of records not matching any rule
        condition_column (str, optional): Column name containing the rule condition, defaults to 'condition'
        logger (object, optional): Logger to record any output

    Returns:
        tuple:
            sqlalchemy.Selectable: cte of the rules
            sqlalchemy.Selectable: SQLAlchemy query to apply the rules
    """
    target_columns = target_columns or ['value']
    df_rules = df_rules.reset_index(drop=True)
    df_rules['rule_number'] = df_rules.index
    if iteration_column not in df_rules.columns:
        df_rules[iteration_column] = 1

    cte_source = source_query.cte('source')
    # make the rules a values clause CTE => WITH cte_rules as select * from values( (rule1,), (rule2,)
    cte_rules = sqlalchemy.select(
        sqlalchemy.values(
            *[sqlalchemy.column(col, sqlalchemy_from_dtype(df_rules[col].dtype)) for col in df_rules.columns],
            name="rule_values",
        ).data(
            df_rules.values
        )
    ).cte('rules')

    iterations = list(set(df_rules[iteration_column]))
    iterations.sort()
    applied_rules_select = None

    for iteration in iterations:
        iteration_select = None

        if include_once:
            iteration_select = sqlalchemy.select(
                *[col for col in cte_source.columns],
                sqlalchemy.case(
                    *[
                        (eval_rule(rule[condition_column], variables={}, tables=[cte_source]), rule[rule_id_column])
                        for index, rule in df_rules[(df_rules[iteration_column] == iteration) & (df_rules['include'] == True)].iterrows()
                    ],
                    else_=None,
                ).label('rule_id')
            )
        else:
            for index, rule in df_rules[(df_rules[iteration_column] == iteration) & (df_rules['include'] == True)].iterrows():
                rule_select = sqlalchemy.select(
                    *[col for col in cte_source.columns],
                    sqlalchemy.literal(rule[rule_id_column]).label('rule_id')
                ).where(
                    eval_rule(rule[condition_column], variables={}, tables=[cte_source]),
                )
                if iteration_select:
                    iteration_select = iteration_select.union_all(rule_select)
                else:
                    iteration_select = rule_select

        if applied_rules_select:
            applied_rules_select = applied_rules_select.union_all(iteration_select)
        else:
            applied_rules_select = iteration_select

    cte_applied_rules = applied_rules_select.cte('applied_rules')

    final_select = sqlalchemy.select(
        *[col for col in cte_applied_rules.columns if col.name != 'rule_id'],
        sqlalchemy.func.cast(sqlalchemy.null(), sqlalchemy.VARCHAR).label('log'),
        cte_rules.columns['rule_number'],
        cte_rules.columns['rule'] if False else sqlalchemy.func.cast(sqlalchemy.null(), sqlalchemy.VARCHAR).label('rule'),
        cte_applied_rules.columns['rule_id'],
        *[cte_rules.columns[t] for t in target_columns]
    ).select_from(
        sqlalchemy.join(
            cte_applied_rules,
            cte_rules,
            cte_applied_rules.columns['rule_id'] == cte_rules.columns[rule_id_column],
            isouter=True,
        )
    )

    return cte_rules, final_select
