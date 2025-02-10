# coding=utf-8
# pylint: disable=function-redefined

import sqlalchemy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement, GenericFunction, ReturnTypeFromArgs, sum
from sqlalchemy.types import Numeric
from sqlalchemy.sql.expression import FromClause
from sqlalchemy.sql import case, func

from toolz.dicttoolz import dissoc
from plaidcloud.rpc.type_conversion import postgres_to_python_date_format, python_to_postgres_date_format, date_format_from_datetime_format
from plaidcloud.rpc.database import PlaidDate, PlaidTimestamp

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2022, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


class elapsed_seconds(FunctionElement):
    type = Numeric()
    name = 'elapsed_seconds'

@compiles(elapsed_seconds)
def compile_es(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return 'EXTRACT(EPOCH FROM COALESCE(%s, NOW())-%s)' % (compiler.process(func.cast(end_date, sqlalchemy.DateTime)), compiler.process(func.cast(start_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'hana')
def compile_es_hana(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "Seconds_between(%s, COALESCE(%s, NOW()))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'mssql')
def compile_es_mssql(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "datediff(ss, %s, COALESCE(%s, NOW()))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'databend')
def compile_es_databend(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "(CAST(COALESCE(%s, NOW()) AS INT64 - CAST(%s AS INT64)) / 1000000" % (compiler.process(func.cast(end_date, sqlalchemy.DateTime)), compiler.process(func.cast(start_date, sqlalchemy.DateTime)))


class avg(ReturnTypeFromArgs):
    pass

@compiles(avg)
def compile_avg(element, compiler, **kw):
    return compiler.visit_function(element)

@compiles(avg, 'hana')
def compile_avg_hana(element, compiler, **kw):
    # Upscale Integer Types, otherwise it blows the calculation
    if isinstance(element.type, sqlalchemy.Integer) or isinstance(element.type, sqlalchemy.SmallInteger):
        return 'avg(cast({} AS BIGINT))'.format(compiler.process(element.clauses))
    else:
        return compiler.visit_function(element)

@compiles(sum, 'hana')
def compile_sum_hana(element, compiler, **kwargs):
    # Upscale Integer Types, otherwise it blows the calculation
    if isinstance(element.type, sqlalchemy.Integer) or isinstance(element.type, sqlalchemy.SmallInteger):
        return 'sum(cast({} AS BIGINT))'.format(compiler.process(element.clauses))
    else:
        return compiler.visit_function(element)


class variance(ReturnTypeFromArgs):
    pass

@compiles(variance)
def compile_variance(element, compiler, **kw):
    return compiler.visit_function(element)

@compiles(variance, 'hana')
def compile_variance_hana(element, compiler, **kw):
    # Upscale Integer Types, otherwise it blows the calculation
    if isinstance(element.type, sqlalchemy.Integer) or isinstance(element.type, sqlalchemy.SmallInteger):
        return 'var(cast({} AS BIGINT))'.format(compiler.process(element.clauses))
    else:
        return 'var({})'.format(compiler.process(element.clauses))


# N.B. Names custom_values because there is a new `values` method being added to sqlalchemy
# so I'm avoiding a future collision
class custom_values(FromClause):
    named_with_column = True

    def __init__(self, columns, *args, **kw):
        self._column_args = columns
        self.list = args
        self.alias_name = self.name = kw.pop("alias_name", None)
        self._is_lateral = kw.pop("is_lateral", False)

    def _populate_column_collection(self):
        for c in self._column_args:
            c._make_proxy(self)

    @property
    def _from_objects(self):
        return [self]

@compiles(custom_values)
def compile_custom_values(element, compiler, asfrom=False, **kw):
    columns = element.columns
    v = "VALUES %s" % ", ".join(
        "(%s)"
        % ", ".join(
            compiler.visit_column(elem) if isinstance(elem, sqlalchemy.sql.expression.ColumnClause) else
            compiler.visit_cast(elem) if isinstance(elem, sqlalchemy.sql.expression.Cast) else
            compiler.render_literal_value(elem, column.type)
            for elem, column in zip(tup, columns)
        )
        for tup in element.list
    )
    if asfrom:
        if element.alias_name:
            v = "(%s) AS %s (%s)" % (
                v,
                element.alias_name,
                (", ".join(compiler.visit_column(c, include_table=False) for c in element.columns)),
            )
        else:
            v = "(%s)" % v
        if element._is_lateral:
            v = "LATERAL %s" % v
    return v


class import_col(GenericFunction):
    name = 'import_col'
    inherit_cache = False

@compiles(import_col)
def compile_import_col(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    date_format = date_format.value
    trailing_negs = trailing_negs.value
    return compiler.process(
        import_cast(col, dtype, date_format, trailing_negs) if dtype == 'text' else
        case(
            (func.regexp_replace(col, r'\s*', '') == '', 0.0 if dtype == 'numeric' else None),
            else_=import_cast(col, dtype, date_format, trailing_negs)
        ),
        **kw
    )


class import_cast(GenericFunction):
    name = 'import_cast'
    inherit_cache = False

@compiles(import_cast)
def compile_import_cast(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    datetime_format = date_format.value
    if datetime_format and '%' in datetime_format:
        datetime_format = python_to_postgres_date_format(datetime_format)
    trailing_negs = trailing_negs.value

    if dtype == 'date':
        return compiler.process(func.to_date(col, datetime_format), **kw)
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(col, datetime_format), **kw)
    elif dtype == 'time':
        return compiler.process(func.to_timestamp(col, 'HH24:MI:SS'), **kw)
    elif dtype == 'interval':
        return compiler.process(col, **kw) + '::interval'
    elif dtype == 'boolean':
        return compiler.process(col, **kw) + '::boolean'
    elif dtype in ['integer', 'bigint', 'smallint', 'numeric']:
        if trailing_negs:
            return compiler.process(func.to_number(col, '9999999999999999999999999D9999999999999999999999999MI'), **kw)
        return compiler.process(func.cast(col, sqlalchemy.Numeric), **kw)
    else:
        #if dtype == 'text':
        return compiler.process(col, **kw)

@compiles(import_cast, 'hana')
def compile_import_cast_hana(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    datetime_format = date_format.value
    if datetime_format and '%' in datetime_format:
        datetime_format = python_to_postgres_date_format(datetime_format)
    # trailing_negs = trailing_negs.value

    if dtype == 'text':
        return compiler.process(col)
    elif dtype == 'date':
        return compiler.process(func.to_date(func.to_nvarchar(col), datetime_format))
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(func.to_nvarchar(col), 'YYYY-MM-DD HH24:MI:SS'))
    elif dtype == 'interval':
        return compiler.process(col) + '::interval'
    elif dtype == 'boolean':
        return compiler.process(
            sqlalchemy.case(
                (func.to_nvarchar(col) == 'True', sqlalchemy.literal(1, sqlalchemy.Integer)),
                (func.to_nvarchar(col) == 'False', sqlalchemy.literal(0, sqlalchemy.Integer)),
                else_=col
            )
        )
    elif dtype == 'integer':
        return compiler.process(func.to_int(func.to_nvarchar(col)))
    elif dtype == 'bigint':
        return compiler.process(func.to_bigint(func.to_nvarchar(col)))
    elif dtype == 'smallint':
        return compiler.process(func.to_smallint(func.to_nvarchar(col)))
    elif dtype == 'numeric':
        return compiler.process(func.to_decimal(func.to_nvarchar(col), 38, 10))


@compiles(import_cast, 'databend')
def compile_import_cast_databend(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    datetime_format = date_format.value
    trailing_negs = trailing_negs.value
    # N.B. Not adjusting the datetime_format here, it is done in safe_to_date/safe_to_timestamp directly

    if dtype == 'date':
        return compiler.process(func.to_date(col, datetime_format))
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(col, datetime_format), **kw)
    elif dtype == 'time':
        return compiler.process(func.to_timestamp(col, '%H:%M:%S'), **kw)
    elif dtype == 'interval':
        return compiler.process(func.to_interval(col), **kw)
    elif dtype == 'boolean':
        return compiler.process(
            func.to_boolean(
                func.cast(
                    sqlalchemy.case(
                        (func.to_string(col) == 't', sqlalchemy.literal('TRUE', sqlalchemy.String)),
                        (func.to_string(col) == '1', sqlalchemy.literal('TRUE', sqlalchemy.String)),
                        (func.to_string(col) == 'f', sqlalchemy.literal('FALSE', sqlalchemy.String)),
                        (func.to_string(col) == '0', sqlalchemy.literal('FALSE', sqlalchemy.String)),
                        else_=col
                    ),
                    sqlalchemy.String,
                )
            ),
            **kw
        )
    elif dtype in ['integer', 'bigint', 'smallint', 'numeric']:
        expr = func.regexp_replace(col, r'\s*', '')
        if trailing_negs:
            expr = sqlalchemy.case(
                (func.regexp_like(expr, '^[0-9]*\\.?[0-9]*-$'), func.concat('-', func.replace(expr, '-', ''))),
                else_=expr
            )
        if dtype == 'integer':
            return compiler.process(func.to_int32(expr))
        elif dtype == 'bigint':
            return compiler.process(func.to_int64(expr))
        elif dtype == 'smallint':
            return compiler.process(func.to_int16(expr))
        elif dtype == 'numeric':
            return compiler.process(
                func.cast(
                    sqlalchemy.case(
                        (func.to_string(expr) == 'NaN', None),
                        else_=expr,
                    ),
                    sqlalchemy.Numeric(38, 10),
                )
            )
    else:
        #if dtype == 'text':
        return compiler.process(col, **kw)


class safe_to_timestamp(GenericFunction):
    name = 'to_timestamp'


@compiles(safe_to_timestamp)
def compile_safe_to_timestamp(element, compiler, **kw):
    full_args = list(element.clauses)
    if len(full_args) == 1:
        date_format = 'YYYY-MM-DD HH24:MI:SS'
        text = full_args[0]
        args = []
    else:
        text, date_format, *args = full_args

    text = func.cast(text, sqlalchemy.Text)
    date_format = func.cast(date_format, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"to_timestamp({compiler.process(text)}, {compiler.process(date_format)}, {compiled_args})"

    return f"to_timestamp({compiler.process(text)}, {compiler.process(date_format)})"


@compiles(safe_to_timestamp, 'databend')
def compile_safe_to_timestamp_databend(element, compiler, **kw):
    full_args = list(element.clauses)
    if len(full_args) == 1:
        datetime_format = 'YYYY-MM-DD HH24:MI:SS'
        text = full_args[0]
        args = []
    else:
        text, datetime_format, *args = full_args
        datetime_format = datetime_format.value

    text = func.cast(text, sqlalchemy.Text)
    if datetime_format and '%' not in datetime_format:
        datetime_format = postgres_to_python_date_format(datetime_format)
    datetime_format = func.cast(datetime_format, sqlalchemy.Text)
    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"to_timestamp({compiler.process(text)}, {compiler.process(datetime_format)}, {compiled_args})"

    return f"to_timestamp({compiler.process(text)}, {compiler.process(datetime_format)})"


# Disabling safe_to_char - input can be date, integer, float, interval (not just date)
# class safe_to_char(GenericFunction):
#     name = 'to_char'
#
# @compiles(safe_to_char)
# def compile_safe_to_char(element, compiler, **kw):
#     timestamp, format, *args = list(element.clauses)
#
#     if not isinstance(timestamp.type, sqlalchemy.DateTime):
#         timestamp = func.to_timestamp(timestamp)
#     format = func.cast(format, sqlalchemy.Text)
#
#     if args:
#         compiled_args = ', '.join([compiler.process(arg) for arg in args])
#         return f"to_char({compiler.process(timestamp)}, {compiler.process(format)}, {compiled_args})"
#
#     return f"to_char({compiler.process(timestamp)}, {compiler.process(format)})"


class safe_extract(GenericFunction):
    name = 'extract'


# This one should work with databend, assuming timestamp types are the same
@compiles(safe_extract)
def compile_safe_extract(element, compiler, **kw):
    field, timestamp, *args = list(element.clauses)

    field = field.effective_value
    if not isinstance(timestamp.type, (sqlalchemy.TIMESTAMP, sqlalchemy.DateTime, sqlalchemy.Date, sqlalchemy.Interval, PlaidDate, PlaidTimestamp)):
        timestamp = func.to_timestamp(timestamp)

    return compiler.process(sqlalchemy.sql.expression.extract(field, timestamp, *args))


def _squash_to_numeric(text):
    return func.cast(
        func.nullif(
            func.numericize(text),
            ''
        ),
        sqlalchemy.Numeric
    )


class sql_metric_multiply(GenericFunction):
    name = 'metric_multiply'

@compiles(sql_metric_multiply)
def compile_sql_metric_multiply(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    number_abbreviations = {
        'D': 10,  #deka
        'H': 10**2,  #hecto
        'K': 10**3,  #kilo
        'M': 10**6,  #mega/million
        'B': 10**9,  #billion
        'G': 10**9,  #giga
        'T': 10**12,  #tera/trillion
        'P': 10**15,  #peta
        'E': 10**18,  #exa

        # JSON can't encode integers larger than 64-bits, so we caN't send queries between machines with this many zeroes
        # 'Z': 10**21,  #zetta
        # 'Y': 10**24,  #yotta
    }

    arg, = list(element.clauses)

    exp = func.trim(func.cast(arg, sqlalchemy.Text))

    def apply_multiplier(text, multiplier):
        # This takes the string, converts it to a numeric, applies the multiplier, then casts it back to string
        # Needs to get cast back as string in case it is nested inside the integerize or numericize operations
        return func.cast(
            _squash_to_numeric(text) * multiplier,
            sqlalchemy.Text
        )

    exp = sqlalchemy.case(*[
        (exp.endswith(abrev), apply_multiplier(exp, number_abbreviations[abrev]))
        for abrev in number_abbreviations
    ], else_=exp)

    return compiler.process(exp, **kw)


class sql_numericize(GenericFunction):
    name = 'numericize'
    inherit_cache = False

@compiles(sql_numericize)
def compile_sql_numericize(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    def sql_only_numeric(text):
        # Returns substring of numeric values only (-, ., numbers, scientific notation)
        cast_text = func.cast(text, sqlalchemy.Text)
        trim_text = func.trim(cast_text)  # trim so that when we check for a sign at the beginning, we ignore spaces
        return func.coalesce(
            func.substring(trim_text, r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))'),  # check for valid scientific notation
            func.substring(trim_text, r'(^[+\-][0-9\.]+)'),  # check for a number prefixed with a sign
            func.nullif(
                func.regexp_replace(trim_text, r'[^0-9\.]+', '', 'g'),  # remove all the non-numeric characters
                ''
            )
        )

    return compiler.process(sql_only_numeric(arg), **kw)


@compiles(sql_numericize, 'databend')
def compile_sql_numericize_databend(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    def sql_only_numeric(text):
        # Returns substring of numeric values only (-, ., numbers, scientific notation)
        cast_text = func.cast(text, sqlalchemy.Text)
        trim_text = func.trim(cast_text)  # trim so that when we check for a sign at the beginning, we ignore spaces
        return func.coalesce(
            func.regexp_substr(trim_text, r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))'),  # check for valid scientific notation
            func.regexp_substr(trim_text, r'(^[+\-][0-9\.]+)'),  # check for a number prefixed with a sign
            func.nullif(
                func.regexp_replace(trim_text, r'[^0-9\.]+', '', 1, 0),  # remove all the non-numeric characters
                ''
            )
        )

    return compiler.process(sql_only_numeric(arg), **kw)


class sql_integerize_round(GenericFunction):
    name = 'integerize_round'

@compiles(sql_integerize_round)
def compile_sql_integerize_round(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    return compiler.process(func.cast(_squash_to_numeric(arg), sqlalchemy.Integer), **kw)


class sql_integerize_truncate(GenericFunction):
    name = 'integerize_truncate'

@compiles(sql_integerize_truncate)
def compile_sql_integerize_truncate(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    return compiler.process(func.cast(func.trunc(_squash_to_numeric(arg)), sqlalchemy.Integer), **kw)


@compiles(sql_integerize_truncate, 'databend')
def compile_sql_integerize_truncate_databend(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    return compiler.process(func.cast(func.truncate(_squash_to_numeric(arg)), sqlalchemy.Integer), **kw)

#
# class sql_left(GenericFunction):
#     name = 'left'
#
# @compiles(sql_left)
# def compile_sql_left(element, compiler, **kw):
#     # TODO: add docstring. Figure out what this does.
#     # seems to find a substring from 1 to count. I'm not sure why or what that's used for.
#
#     # Postgres supports negative numbers, while this doesn't.
#     # This MIGHT be an issue in the future, but for now, this works
#     # well enough.
#     text, count, = list(element.clauses)
#
#     def sql_left(text, count):
#         cast_text = func.cast(text, sqlalchemy.Text)
#         cast_count = func.cast(count, sqlalchemy.Integer)
#         return sqlalchemy.cast(
#             func.substring(cast_text, 1, cast_count),
#             sqlalchemy.Text,
#         )
#
#     return compiler.process(sql_left(text, count), **kw)
#

class sql_slice_string(GenericFunction):
    name = 'slice_string'
    inherit_cache = False

@compiles(sql_slice_string)
def compile_sql_slice_string(element, compiler, **kw):
    """Provides string slicing functionality similar to that in python

    """
    text, *args = list(element.clauses)
    cast_text = func.cast(text, sqlalchemy.Text)
    start = 0
    count = None

    if len(args) > 0:
        start = args[0]
        if isinstance(start, sqlalchemy.sql.elements.Null):
            start = 0
        else:
            start = start.value

        if len(args) > 1:
            if not isinstance(args[1], sqlalchemy.sql.elements.Null):
                count = args[1].value

    if start >= 0:
        start = start + 1  # if python zero-based???
        if not count:
            return compiler.process(
                sqlalchemy.cast(
                    func.substring(cast_text, start),
                    sqlalchemy.Text,
                )
            )
        # count = count.value
        if count > 0:
            return compiler.process(
                sqlalchemy.cast(
                    func.substring(cast_text, start, count),
                    sqlalchemy.Text,
                )
            )
        else:
            return compiler.process(
                func.left(
                    sqlalchemy.cast(
                        func.substring(cast_text, start),
                        sqlalchemy.Text,
                    ),
                    count,
                )
            )

    else:
        if not count:
            return compiler.process(
                func.right(
                    cast_text,
                    -start,
                )
            )
        # count = count.value
        if count < 0:
            return compiler.process(
                func.left(
                    func.right(
                        cast_text,
                        -start,
                    ),
                -count,
                )
            )
        raise NotImplementedError


# This should work with databend, assuming types are fine. length and lpad are available
class sql_zfill(GenericFunction):
    name = 'zfill'

@compiles(sql_zfill)
def compile_sql_zfill(element, compiler, **kw):
    field, width, *args = list(element.clauses)
    field = func.cast(field, sqlalchemy.Text)
    width = func.cast(width, sqlalchemy.Integer)
    if args:
        char = func.cast(args[0], sqlalchemy.Text)
    else:
        char = '0'

    true_width = func.greatest(width, func.length(field))
    return compiler.process(
        func.lpad(field, true_width, char)
    )

class sql_normalize_whitespace(GenericFunction):
    name = 'normalize_whitespace'

WEIRD_WHITESPACE_CHARS = [
    'n',     # newline
    'r',     # carriage return
    'f',     # form feed
    'u000B', # line tabulation
    'u0085', # next line
    'u2028', # line separator
    'u2029', # paragraph separator
    'u00A0', # non-breaking space
]

@compiles(sql_normalize_whitespace)
def compile_sql_normalize_whitespace(element, compiler, **kw):
    field, *args = list(element.clauses)
    field = func.cast(field, sqlalchemy.Text)

    ww_re = '[' + ''.join(['\\' + c for c in WEIRD_WHITESPACE_CHARS]) + ']+'

    return compiler.process(
        func.regexp_replace(field, ww_re, ' ', 'g')
    )

@compiles(sql_normalize_whitespace, 'databend')
def compile_sql_normalize_whitespace(element, compiler, **kw):
    field, *args = list(element.clauses)
    field = func.cast(field, sqlalchemy.Text)

    ww_re = '[' + ''.join(['\\' + c for c in WEIRD_WHITESPACE_CHARS]) + ']+'

    return compiler.process(
        func.regexp_replace(field, ww_re, ' ', 1, 0)
    )

class safe_unix_to_timestamp(GenericFunction):
    name = 'unix_to_timestamp'

@compiles(safe_unix_to_timestamp)
def compile_safe_unix_to_timestamp(element, compiler, **kw):
    timestamp, *args = list(element.clauses)
    timestamp = func.cast(timestamp, sqlalchemy.Integer)

    return f"to_timestamp({compiler.process(timestamp)})"


class safe_to_date(GenericFunction):
    # This exists to make to_date behave as Silvio expects in the case of empty date strings.
    # See ALYZ-2428
    name = 'to_date'

@compiles(safe_to_date)
def compile_safe_to_date(element, compiler, **kw):
    text, *args = list(element.clauses)
    if len(args):
        date_format = args[0].value
        if date_format and '%' in date_format:
            date_format = python_to_postgres_date_format(date_format)
        return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)}, {compiler.process(func.cast(date_format, sqlalchemy.Text))})"

    return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)})"


@compiles(safe_to_date, 'databend')
def compile_safe_to_date_databend(element, compiler, **kw):
    text, *args = list(element.clauses)
    if len(args):
        date_format = args[0].value
        if date_format and '%' not in date_format:
            date_format = date_format_from_datetime_format(date_format)
            date_format = postgres_to_python_date_format(date_format)
        return f"to_date(to_timestamp({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)}, {compiler.process(func.cast(date_format, sqlalchemy.Text))}))"

    return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)})"


class safe_round(GenericFunction):
    name = 'round'

@compiles(safe_round)
def compile_safe_round(element, compiler, **kw):
    # This exists to cast text to numeric prior to rounding
    all_args = list(element.clauses)
    if len(all_args) == 1:
        number, = all_args
        digits = None
        args = []
    else:
        number, digits, *args = all_args

    number = func.cast(number, sqlalchemy.Numeric)
    if digits is not None:
        digits = func.cast(digits, sqlalchemy.Integer)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
    else:
        compiled_args = None

    if digits is not None:
        compiled_digits = compiler.process(digits)
    else:
        compiled_digits = None

    compiled_number = compiler.process(number)
    all_compiled_args = ', '.join(arg for arg in [compiled_number, compiled_digits, compiled_args] if arg is not None)
    return f"round({all_compiled_args})"


class safe_ltrim(GenericFunction):
    name = 'ltrim'

@compiles(safe_ltrim)
def compile_safe_ltrim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"ltrim({compiler.process(text)}, {compiled_args})"

    return f"ltrim({compiler.process(text)})"

@compiles(safe_ltrim, 'databend')
def compile_safe_ltrim_databend(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"TRIM(LEADING {compiled_args} FROM {compiler.process(text)})"

    return f"TRIM(LEADING ' ' FROM {compiler.process(text)})"


class safe_rtrim(GenericFunction):
    name = 'rtrim'

@compiles(safe_rtrim)
def compile_safe_rtrim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"rtrim({compiler.process(text)}, {compiled_args})"

    return f"rtrim({compiler.process(text)})"


@compiles(safe_rtrim, 'databend')
def compile_safe_rtrim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"TRIM(TRAILING {compiled_args} FROM {compiler.process(text)})"

    return f"TRIM(TRAILING ' ' FROM {compiler.process(text)})"


class safe_trim(GenericFunction):
    name = 'trim'

@compiles(safe_trim)
def compile_safe_trim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"trim({compiler.process(text)}, {compiled_args})"

    return f"trim({compiler.process(text)})"


@compiles(safe_trim, 'databend')
def compile_safe_trim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args and (len(args) > 1 or args[0].value != ''):
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"TRIM(BOTH {compiled_args} FROM {compiler.process(text)})"

    return f"TRIM({compiler.process(text)})"


class sql_only_ascii(GenericFunction):
    name = 'ascii'

@compiles(sql_only_ascii)
def compile_sql_only_ascii(element, compiler, **kw):
    # Remove non-ascii characters
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(text, r'[^[:ascii:]]+', '', 'g'),
        **kw
    )

@compiles(sql_only_ascii, 'databend')
def compile_sql_only_ascii_databend(element, compiler, **kw):
    # Remove non-ascii characters
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(text, r'[^[:ascii:]]+', '', 1, 0),
        **kw
    )


class safe_upper(GenericFunction):
    name = 'upper'

@compiles(safe_upper)
def compile_safe_upper(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"upper({compiler.process(text)}, {compiled_args})"

    return f"upper({compiler.process(text)})"


class safe_lower(GenericFunction):
    name = 'lower'

@compiles(safe_lower)
def compile_safe_lower(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"lower({compiler.process(text)}, {compiled_args})"

    return f"lower({compiler.process(text)})"


class sql_set_null(GenericFunction):
    name = 'null_values'

@compiles(sql_set_null)
def compile_sql_set_null(element, compiler, **kw):
    val, *null_values = list(element.clauses)

    # Turn val into null if it's in null_values
    return compiler.process(
        sqlalchemy.case(*[
            (val == nv, None)
            for nv in null_values
        ], else_=val),
        **kw,
    )


class sql_safe_divide(GenericFunction):
    name = 'safe_divide'

@compiles(sql_safe_divide)
def compile_safe_divide(element, compiler, **kw):
    """Divides numerator by denominator, returning NULL if the denominator is 0.
    """
    numerator, denominator, divide_by_zero_value = list(element.clauses)
    numerator = func.cast(numerator, sqlalchemy.Numeric)
    denominator = func.cast(denominator, sqlalchemy.Numeric)

    basic_safe_divide = numerator / func.nullif(denominator, 0)
    # NOTE: in SQL, x/NULL = NULL, for all x.

    # Skip the coalesce if it's not necessary
    return compiler.process(
        basic_safe_divide if divide_by_zero_value is None else func.coalesce(basic_safe_divide, divide_by_zero_value)
    )

DATE_ADD_UNITS = ['years', 'months', 'weeks', 'days', 'hours', 'minutes', 'seconds']

class sql_date_add(GenericFunction):
    name = 'date_add'

    def __init__(self, *clauses, **kwargs):
        self.additions = {
            unit: kwargs.get(unit, 0)
            for unit in DATE_ADD_UNITS
        }

        kwargs = dissoc(kwargs, *DATE_ADD_UNITS)

        super().__init__(*clauses, **kwargs)

@compiles(sql_date_add)
def compile_sql_date_add(element, compiler, **kw):
    dt, *args = list(element.clauses)
    a = {
        unit: func.cast(val, sqlalchemy.Integer)
        for unit, val in element.additions.items()
    }

    dt = func.cast(dt, sqlalchemy.DateTime)
    interval = func.make_interval(*[a[unit] for unit in DATE_ADD_UNITS])

    return compiler.process(dt + interval)

### Databend

# Still need to check this one
class sql_to_char(GenericFunction):
    name = 'to_char'

@compiles(sql_to_char, 'databend')
def compile_to_char_databend(element, compiler, **kw):
    # These already in use format strings are supported*
    # 'YYYYMMDD'
    # 'YYYY-MM-DD'
    # 'LFM999,999,999,999D00'
    # '999,999,999'
    # '999,999,999.9'
    # '000000'
    # 'FM9999999999999.00'
    # '999,999,999.999999999'
    # ''
    # 'IYYY-IW'
    # 'YYYYMM'
    #
    # *except commas and FMs will be ignored. L and D will be replaced by $ and . respectively, regardless of locale
    source, *args = list(element.clauses)
    if args:
        format_, *args = args
        format_ = format_.effective_value
    else:
        format_ = None

    if format_ is None:
        return compiler.process(
            func.to_string(source)
        )

    if '0' in format_ or '9' in format_:
        format_ = format_.replace('L', '$').replace('D', '.')
        return f'to_char({compiler.process(source)}, \'{format_}\')'
    else:
        if format_ and '%' not in format_:
            format_ = postgres_to_python_date_format(format_)
        # This is probably a format for formatting a date
        return compiler.process(
            func.to_string(source, format_)
        )


class sql_to_number(GenericFunction):
    name = 'to_number'

# Need to come back to this one
@compiles(sql_to_number, 'databend')
def compile_to_number(element, compiler, **kw):
    # It seems like all the uses of this in expressions are using the format string '999999'
    string, _ = list(element.clauses)
    return compiler.process(
        func.to_int64(string)
    )

class sql_transaction_timestamp(GenericFunction):
    name = 'transaction_timestamp'

@compiles(sql_transaction_timestamp, 'databend')
def compile_transaction_timestamp(element, compiler, **kw):
    # Not available in databend
    return compiler.process(
        func.now()
    )

class sql_strpos(GenericFunction):
    name = 'strpos'

@compiles(sql_strpos, 'databend')
def compile_strpos(element, compiler, **kw):
    string, substring = list(element.clauses)
    return compiler.process(
        func.locate(substring, string)
    )

class sql_string_to_array(GenericFunction):
    name = 'string_to_array'

@compiles(sql_string_to_array, 'databend')
def compile_string_to_array(element, compiler, **kw):
    string, delimiter, *args = list(element.clauses)
    # null_string is not supported

    split_array = func.split(
        string,
        sqlalchemy.case(
            (sqlalchemy.or_(delimiter == '', delimiter == None), ''),
            else_=delimiter
        )
    )
    return compiler.process(split_array)
