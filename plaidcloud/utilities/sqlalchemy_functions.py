# coding=utf-8
# pylint: disable=function-redefined

from functools import reduce

import sqlalchemy
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement, GenericFunction, ReturnTypeFromArgs, sum
from sqlalchemy.types import Numeric
from sqlalchemy.sql.expression import FromClause
from sqlalchemy.sql import case, func

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

class elapsed_seconds(FunctionElement):
    type = Numeric()
    name = 'elapsed_seconds'

@compiles(elapsed_seconds)
def compile(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return 'EXTRACT(EPOCH FROM COALESCE(%s, NOW())-%s)' % (compiler.process(func.cast(end_date, sqlalchemy.DateTime)), compiler.process(func.cast(start_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'hana')
def compile(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "Seconds_between(%s, COALESCE(%s, NOW()))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'mssql')
def compile(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "datediff(ss, %s, COALESCE(%s, NOW()))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))


class avg(ReturnTypeFromArgs):
    pass

@compiles(avg)
def compile(element, compiler, **kw):
    return compiler.visit_function(element)

@compiles(avg, 'hana')
def compile(element, compiler, **kw):
    # Upscale Integer Types, otherwise it blows the calculation
    if isinstance(element.type, sqlalchemy.Integer) or isinstance(element.type, sqlalchemy.SmallInteger):
        return 'avg(cast({} AS BIGINT))'.format(compiler.process(element.clauses))
    else:
        return compiler.visit_function(element)

@compiles(sum, 'hana')
def compile(element, compiler, **kwargs):
    # Upscale Integer Types, otherwise it blows the calculation
    if isinstance(element.type, sqlalchemy.Integer) or isinstance(element.type, sqlalchemy.SmallInteger):
        return 'sum(cast({} AS BIGINT))'.format(compiler.process(element.clauses))
    else:
        return compiler.visit_function(element)


class variance(ReturnTypeFromArgs):
    pass

@compiles(variance)
def compile(element, compiler, **kw):
    return compiler.visit_function(element)

@compiles(variance, 'hana')
def compile(element, compiler, **kw):
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

@compiles(import_cast)
def compile_import_cast(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    date_format = date_format.value
    trailing_negs = trailing_negs.value

    if dtype == 'date':
        return compiler.process(func.to_date(col, date_format), **kw)
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(col, date_format), **kw)
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
    date_format = date_format.value
    # trailing_negs = trailing_negs.value

    if dtype == 'text':
        return compiler.process(col)
    elif dtype == 'date':
        return compiler.process(func.to_date(func.to_nvarchar(col), date_format))
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(func.to_nvarchar(col), 'HH24:MI:SS'))
    elif dtype == 'interval':
        return compiler.process(col) + '::interval'
    elif dtype == 'boolean':
        return compiler.process(
            func.case(
                (func.to_nvarchar(col) == 'True', 1),
                (func.to_nvarchar(col) == 'False', 0),
                else_=None
            )
        )
    elif dtype == 'integer':
        return compiler.process(func.to_int(func.to_nvarchar(col)))
    elif dtype == 'bigint':
        return compiler.process(func.to_bigint(func.to_nvarchar(col)))
    elif dtype == 'smallint':
        return compiler.process(func.to_smallint(func.to_nvarchar(col)))
    elif dtype == 'numeric':
        return compiler.process(func.to_decimal(func.to_nvarchar(col), 34, 10))


class safe_to_timestamp(GenericFunction):
    name = 'to_timestamp'

@compiles(safe_to_timestamp)
def compile_safe_to_timestamp(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.to_nvarchar(text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"to_timestamp({text}, {compiled_args})"

    return f"to_timestamp({text})"


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

@compiles(sql_numericize)
def compile_sql_numericize(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    def sql_only_numeric(text):
        # Returns substring of numeric values only (-, ., numbers, scientific notation)
        cast_text = func.cast(text, sqlalchemy.Text)
        return func.coalesce(
            func.substring(cast_text, r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))'),  # check for valid scientific notation
            func.nullif(
                func.regexp_replace(cast_text, r'[^0-9\.\+\-]+', '', 'g'),  # remove all the non-numeric characters
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


class sql_left(GenericFunction):
    name = 'left'

@compiles(sql_left)
def compile_sql_left(element, compiler, **kw):
    # TODO: add docstring. Figure out what this does.
    # seems to find a substring from 1 to count. I'm not sure why or what that's used for.

    # Postgres supports negative numbers, while this doesn't.
    # This MIGHT be an issue in the future, but for now, this works
    # well enough.
    text, count, = list(element.clauses)

    def sql_left(text, count):
        cast_text = func.cast(text, sqlalchemy.Text)
        cast_count = func.cast(count, sqlalchemy.Integer)
        return sqlalchemy.cast(
            func.substring(cast_text, 1, cast_count),
            sqlalchemy.Text,
        )

    return compiler.process(sql_left(text, count), **kw)


class safe_to_date(GenericFunction):
    name = 'to_date'

@compiles(safe_to_date)
def compile_safe_to_date(element, compiler, **kw):
    # This exists to make to_date behave as Silvio expects in the case of empty
    # date strings.
    #
    # See ALYZ-2428
    text, date_format = list(element.clauses)

    # PJM Can we use func.nullif(text) instead??
    # return "CASE WHEN (%s = '') THEN NULL ELSE %s END" % (compiler.process(text), compiler.visit_function(element))

    return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)}, {compiler.process(func.cast(date_format, sqlalchemy.Text))})"


class safe_round(GenericFunction):
    name = 'round'

@compiles(safe_round)
def compile_safe_round(element, compiler, **kw):
    # This exists to cast text to numeric prior to rounding

    number, digits, *args = list(element.clauses)
    number = func.cast(number, sqlalchemy.Numeric)
    digits = func.cast(digits, sqlalchemy.Integer)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"round({compiler.process(number)}, {compiler.process(digits)}, {compiled_args})"

    return f"round({compiler.process(number)}, {compiler.process(digits)})"


class safe_ltrim(GenericFunction):
    name = 'ltrim'

@compiles(safe_ltrim)
def compile_safe_ltrim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"ltrim({compiler.process(text)}, {compiled_args})"

    return f"ltrim({compiler.process(text)})"


class safe_rtrim(GenericFunction):
    name = 'rtrim'

@compiles(safe_rtrim)
def compile_safe_rtrim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"rtrim({compiler.process(text)}, {compiled_args})"

    return f"rtrim({compiler.process(text)})"


class safe_trim(GenericFunction):
    name = 'trim'

@compiles(safe_trim)
def compile_safe_trim(element, compiler, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"trim({compiler.process(text)}, {compiled_args})"

    return f"trim({compiler.process(text)})"


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
    numerator = func.cast(number, sqlalchemy.Numeric)
    denominator = func.cast(digits, sqlalchemy.Integer)

    basic_safe_divide = numerator / func.nullif(denominator, 0)
    # NOTE: in SQL, x/NULL = NULL, for all x.

    # Skip the coalesce if it's not necessary
    return compiler.process(
        basic_safe_divide if divide_by_zero_value is None else func.coalesce(basic_safe_divide, divide_by_zero_value)
    )
