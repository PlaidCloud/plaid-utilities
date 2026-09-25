# coding=utf-8
# pylint: disable=function-redefined

import re
import warnings
from contextlib import contextmanager
from contextvars import ContextVar

import sqlalchemy
from sqlalchemy.exc import SAWarning, CompileError
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement, GenericFunction, ReturnTypeFromArgs, sum, mode as sa_mode
from sqlalchemy.types import Numeric, Boolean, Double
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


# ---------------------------------------------------------------------------
# Postgres date-format tokens → Snowflake format models (sc-23158 WS-B3)
# ---------------------------------------------------------------------------
#: Every token in plaid-rpc's _PG_PY_FORMAT_MAPPING conversion table (plus the
#: uppercase name variants its composite entries use) maps to a Snowflake
#: format element, or to None when Snowflake has no equivalent — those raise at
#: compile time. Snowflake renders unrecognized format text literally, so a
#: silent passthrough of a real Postgres token would produce wrong output, not
#: an error. Verified against
#: docs.snowflake.com/en/sql-reference/date-time-input-output:
#:   - bare HH is a synonym for HH24 on Snowflake but means HH12 in Postgres,
#:     so it must be translated, never passed through
#:   - UUUU is Snowflake's ISO 4-digit year (Postgres IYYY)
#:   - no full-weekday-name (Day), day-of-year (DDD), ISO week (IW),
#:     day-of-week-number (D), or timezone-name (TZ) elements exist
#:   - FF<n> renders fractional seconds; FF6 = microseconds (Postgres US)
#:   - TZH/TZM are the signed UTC-offset elements (Postgres tz / %z)
_SNOWFLAKE_DATE_FORMAT_TOKENS = {
    'IYYY': 'UUUU',
    'YYYY': 'YYYY',
    'YY': 'YY',
    'Month': 'MMMM',
    'MONTH': 'MMMM',
    'Mon': 'MON',
    'MON': 'MON',
    'MM': 'MM',
    'DDD': None,
    'DD': 'DD',
    'Day': None,
    'DAY': None,
    'Dy': 'DY',
    'DY': 'DY',
    'D': None,
    'HH24': 'HH24',
    'HH12': 'HH12',
    'HH': 'HH12',
    'MI': 'MI',
    'SS': 'SS',
    'AM': 'AM',
    'PM': 'PM',
    'US': 'FF6',
    'TZ': None,
    'tz': 'TZHTZM',
    'IW': None,
}

_SNOWFLAKE_TOKENS_BY_LENGTH = sorted(_SNOWFLAKE_DATE_FORMAT_TOKENS, key=len, reverse=True)


def postgres_to_snowflake_date_format(pg_format):
    """Translates a Postgres date-format string to a Snowflake format model.

    Double-quoted literals pass through verbatim (both engines honor them).
    Tokens from the plaid-rpc conversion-table vocabulary with no Snowflake
    format element raise CompileError — the never-silently-wrong guarantee
    holds for that vocabulary. Postgres tokens outside it (Q, WW, W, J, CC,
    lowercase forms, …) pass through as literal text, the same treatment the
    databend/starrocks translators give out-of-table tokens.
    """
    out = []
    i = 0
    while i < len(pg_format):
        char = pg_format[i]
        if char == '"':
            end = pg_format.find('"', i + 1)
            if end == -1:
                out.append(pg_format[i:])
                break
            out.append(pg_format[i:end + 1])
            i = end + 1
            continue
        for token in _SNOWFLAKE_TOKENS_BY_LENGTH:
            if pg_format.startswith(token, i):
                mapped = _SNOWFLAKE_DATE_FORMAT_TOKENS[token]
                if mapped is None:
                    raise CompileError(
                        f"Date format token {token!r} in {pg_format!r} has no Snowflake format element"
                    )
                out.append(mapped)
                i += len(token)
                break
        else:
            out.append(char)
            i += 1
    return ''.join(out)


# ---------------------------------------------------------------------------
# Postgres date-format tokens → StarRocks (MySQL) format specifiers
# ---------------------------------------------------------------------------
#: StarRocks' str_to_date / str2date / date_format take MySQL specifiers, which
#: collide with Python's strftime on the two most common tokens: on StarRocks
#: %M is the full month name and %i is the minute, the exact opposite of
#: Python, where %M is the minute. Handing a Python format to str_to_date
#: therefore parses to NULL and to date_format renders 'January' where the
#: minutes belong. Verified live on StarRocks 4.1.3 and against
#: docs.starrocks.io/docs/sql-reference/sql-functions/date-time-functions/date_format:
#:   - %i minutes, %M full month name, %W full weekday name, %b abbreviated
#:     month, %a abbreviated weekday, %h hour 01-12, %H hour 00-23, %f
#:     microseconds, %j day of year
#:   - %v/%x are the ISO-8601 (Monday-first) week and its week-numbering year
#:   - no timezone specifier of any kind exists
#:   - StarRocks renders an unrecognized specifier as the bare character, so a
#:     silent passthrough of a real Postgres token would produce wrong output,
#:     not an error — every token in plaid-rpc's conversion-table vocabulary
#:     therefore maps to a specifier or to None, and None raises at compile time
_STARROCKS_DATE_FORMAT_TOKENS = {
    'IYYY': '%x',
    'YYYY': '%Y',
    'YY': '%y',
    'Month': '%M',
    'MONTH': '%M',
    'Mon': '%b',
    'MON': '%b',
    'MM': '%m',
    'DDD': '%j',
    'DD': '%d',
    'Day': '%W',
    'DAY': '%W',
    'Dy': '%a',
    'DY': '%a',
    # Postgres D is 1-7 with Sunday=1; StarRocks %w is 0-6 with Sunday=0.
    'D': None,
    'HH24': '%H',
    'HH12': '%h',
    'HH': '%h',
    'MI': '%i',
    'SS': '%S',
    'AM': '%p',
    'PM': '%p',
    'US': '%f',
    'TZ': None,
    'tz': None,
    'IW': '%v',
}

_STARROCKS_TOKENS_BY_LENGTH = sorted(_STARROCKS_DATE_FORMAT_TOKENS, key=len, reverse=True)


def postgres_to_starrocks_date_format(pg_format):
    """Translates a Postgres date-format string to StarRocks (MySQL) specifiers.

    Postgres double-quoted literals become bare text — StarRocks has no
    quoting mechanism in a format string, so "T" must render as T for
    '2026-08-03T14:22:31' to parse. A literal % inside such a section is
    escaped to %%.

    Tokens from the plaid-rpc conversion-table vocabulary with no StarRocks
    specifier raise CompileError. Postgres tokens outside it (Q, WW, W, J, CC,
    lowercase forms, …) pass through as literal text, the same treatment the
    databend/snowflake translators give out-of-table tokens.
    """
    out = []
    i = 0
    while i < len(pg_format):
        char = pg_format[i]
        if char == '"':
            end = pg_format.find('"', i + 1)
            if end == -1:
                # Swallowing the tail as literal text would leave real tokens
                # untranslated and the format would parse to NULL at run time.
                raise CompileError(
                    f"Unterminated quoted literal in date format {pg_format!r}"
                )
            out.append(pg_format[i + 1:end].replace('%', '%%'))
            i = end + 1
            continue
        for token in _STARROCKS_TOKENS_BY_LENGTH:
            if pg_format.startswith(token, i):
                mapped = _STARROCKS_DATE_FORMAT_TOKENS[token]
                if mapped is None:
                    raise CompileError(
                        f"Date format token {token!r} in {pg_format!r} has no StarRocks format specifier"
                    )
                out.append(mapped)
                i += len(token)
                break
        else:
            out.append(char)
            i += 1
    return ''.join(out)


def _starrocks_date_format(datetime_format):
    """Normalizes any inbound format to StarRocks specifiers.

    Callers pass either a Python strftime format or a Postgres format; the
    Python one is routed through Postgres first so a single token table covers
    both, exactly as the snowflake compilers do.
    """
    if not datetime_format:
        return datetime_format
    if '%' in datetime_format:
        datetime_format = python_to_postgres_date_format(datetime_format)
    return postgres_to_starrocks_date_format(datetime_format)


#: Classes with NO verified StarRocks rendering at all -- an explicit,
#: importable counterpart to _STARROCKS_DEFAULT_OK below, populated by
#: _mark_starrocks_unsupported (for an already-defined class) and by
#: _register_geom_fn's starrocks_unsupported= kwarg (for a class it creates).
#: This is the machine-readable interface the epic's Tier-3 compatibility
#: derivation (plan §6, "Tier 3, blocked") reads instead of re-deriving the
#: same set by grepping @compiles bodies for a raised CompileError, which
#: would also catch functions that raise for reasons OTHER than "no StarRocks
#: equivalent" (e.g. a malformed call).
_STARROCKS_UNSUPPORTED = {}


def _mark_starrocks_unsupported(func_cls, *, starrocks_unsupported):
    """Register an explicit StarRocks refusal on an already-defined
    GenericFunction/ReturnTypeFromArgs class.

    Named starrocks_unsupported= to match _register_geom_fn's kwarg below --
    that helper also creates its class, so it can't be reused here directly,
    but every call site is still discoverable by the same keyword, and both
    paths land in the one _STARROCKS_UNSUPPORTED mapping.
    """
    @compiles(func_cls, 'starrocks')
    def _compile_starrocks(element, compiler, _msg=starrocks_unsupported, **kw):
        raise CompileError(_msg)
    _STARROCKS_UNSUPPORTED[func_cls] = starrocks_unsupported
    return func_cls


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

@compiles(elapsed_seconds, 'starrocks')
def compile_es_starrocks(element, compiler, **kw):
    start_date, end_date = list(element.clauses)
    return "seconds_diff(%s, COALESCE(%s, NOW()))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))

@compiles(elapsed_seconds, 'snowflake')
def compile_es_snowflake(element, compiler, **kw):
    # Snowflake cannot subtract timestamps directly and has no NOW();
    # DATEDIFF(second, start, end) = end - start (whole seconds, matching the
    # mssql variant). Side-find: the starrocks variant's seconds_diff(a, b)
    # is a - b, so it yields the opposite sign — pre-existing, left untouched.
    start_date, end_date = list(element.clauses)
    return "datediff(second, %s, COALESCE(%s, CURRENT_TIMESTAMP))" % (compiler.process(func.cast(start_date, sqlalchemy.DateTime)), compiler.process(func.cast(end_date, sqlalchemy.DateTime)))


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

    def _populate_column_collection(self, *args, **kw):
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


#: Typed-staging compile switch (sc-23281). When set, `import_col` compiles to
#: the bare staging column: the Parquet converter already parsed, typed and
#: coerced every value at conversion time (blank -> 0.0 for numeric/currency,
#: blank -> NULL for temporal/integer/boolean, dates parsed per date_format,
#: trailing negatives folded), so the text-parsing CASE machinery below must
#: not run against an already-typed column (`to_timestamp(<timestamp>, fmt)`
#: breaks outright). A contextvar rather than an argument because the
#: workflow-runner ships a pickled expression tree built against the OLD
#: signature; the flag is observed here, in the import worker, at compile
#: time — so old and new runners need no change and there is no deploy skew.
#: `asyncio.to_thread` copies the context, so setting it around query
#: execution propagates into worker threads.
typed_staging = ContextVar('typed_staging', default=False)


@contextmanager
def typed_staging_compilation():
    token = typed_staging.set(True)
    try:
        yield
    finally:
        typed_staging.reset(token)


class import_col(GenericFunction):
    name = 'import_col'
    inherit_cache = False

@compiles(import_col)
def compile_import_col(element, compiler, **kw):
    col, dtype, date_format, trailing_negs = list(element.clauses)
    if typed_staging.get():
        if dtype.value == 'interval':
            # Interval has no Arrow mapping, so the Parquet converter stages
            # it as TEXT (parquet_conversion.stages_as_text) — the projection
            # must keep the old else-branch cast (col::interval /
            # to_interval), the verified old text-staging shape. A bare
            # passthrough would rely on the engine implicitly casting string
            # staging into an Interval temp column, which was never verified
            # (sc-23281 m-5). No blank CASE: the converter already turned
            # blanks into real NULLs.
            return compiler.process(
                import_cast(col, dtype.value, date_format.value, trailing_negs.value), **kw)
        # Bare column, not CAST(col AS target): the staging column already
        # carries the exact target type from the coercion contract, so a cast
        # is a no-op at best and at worst re-introduces per-dialect cast
        # quirks (e.g. Snowflake bare-NUMERIC rounding) on correct values.
        # date_format and trailing_negs are deliberately ignored — both were
        # consumed by the converter.
        return compiler.process(col, **kw)
    dtype = dtype.value
    date_format = date_format.value
    trailing_negs = trailing_negs.value
    return compiler.process(
        import_cast(col, dtype, date_format, trailing_negs) if dtype == 'text' else
        case(
            (func.regexp_replace(col, r'\s*', '') == '', 0.0 if dtype in ('numeric', 'currency') else None),
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
    elif dtype == 'currency':
        return compiler.process(func.to_decimal(func.to_nvarchar(col), 18, 4))


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
    elif dtype in ['integer', 'bigint', 'smallint', 'numeric', 'currency']:
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
        elif dtype == 'currency':
            return compiler.process(
                func.cast(
                    sqlalchemy.case(
                        (func.to_string(expr) == 'NaN', None),
                        else_=expr,
                    ),
                    sqlalchemy.Numeric(18, 4),
                )
            )
    else:
        #if dtype == 'text':
        return compiler.process(col, **kw)

@compiles(import_cast, 'starrocks')
def compile_import_cast_starrocks(element, compiler, **kw):
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
        # sc-30414: mirror the Databend variant's 't'/'f'/'1'/'0' mapping.
        # A bare CAST('t' AS BOOLEAN) is NULL on StarRocks, so the three
        # spellings Databend accepts silently imported as NULL instead.
        return compiler.process(
            func.cast(
                sqlalchemy.case(
                    (func.cast(col, sqlalchemy.Text) == 't', sqlalchemy.literal('TRUE', sqlalchemy.String)),
                    (func.cast(col, sqlalchemy.Text) == '1', sqlalchemy.literal('TRUE', sqlalchemy.String)),
                    (func.cast(col, sqlalchemy.Text) == 'f', sqlalchemy.literal('FALSE', sqlalchemy.String)),
                    (func.cast(col, sqlalchemy.Text) == '0', sqlalchemy.literal('FALSE', sqlalchemy.String)),
                    else_=col
                ),
                Boolean,
            ),
            **kw
        )
    elif dtype in ['integer', 'bigint', 'smallint', 'numeric', 'currency']:
        # sc-30414. Two divergences from the Databend variant, both silent:
        #   * Databend strips whitespace ANYWHERE in the value first
        #     (regexp_replace(col, '\s*', '')), so '1 234' imports as 1234.
        #     StarRocks was casting the raw text, which yields NULL.
        #   * trailing negatives went to to_number(col, '...MI'), but the
        #     StarRocks to_number variant IGNORES the mask (it is a plain
        #     DECIMAL cast), so '123-' imported as NULL rather than -123.
        # Rebuild both out of functions StarRocks actually has.
        expr = func.regexp_replace(col, r'\s*', '')
        if trailing_negs:
            expr = sqlalchemy.case(
                (func.regexp(expr, '^[0-9]*\\.?[0-9]*-$') == 1, func.concat('-', func.replace(expr, '-', ''))),
                else_=expr
            )
        return compiler.process(func.cast(expr, Numeric(18, 4) if dtype == 'currency' else Numeric(38, 10)), **kw)
    else:
        #if dtype == 'text':
        return compiler.process(col, **kw)

@compiles(import_cast, 'snowflake')
def compile_import_cast_snowflake(element, compiler, **kw):
    # Modeled on the databend variant. Date formats are adjusted in
    # safe_to_date/safe_to_timestamp directly (WS-B3 translator), not here.
    col, dtype, date_format, trailing_negs = list(element.clauses)
    dtype = dtype.value
    datetime_format = date_format.value
    trailing_negs = trailing_negs.value

    if dtype == 'date':
        return compiler.process(func.to_date(col, datetime_format), **kw)
    elif dtype == 'timestamp':
        return compiler.process(func.to_timestamp(col, datetime_format), **kw)
    elif dtype == 'time':
        return compiler.process(func.to_timestamp(col, 'HH24:MI:SS'), **kw)
    elif dtype == 'interval':
        # Snowflake's INTERVAL data type is Public Preview, not GA — the loud
        # refusal stands until it ships.
        raise CompileError('Snowflake has no GA INTERVAL data type; interval columns cannot be imported')
    elif dtype == 'boolean':
        # TO_BOOLEAN natively accepts true/t/yes/y/on/1 and false/f/no/n/off/0,
        # case-insensitive — a superset of the databend variant's t/1/f/0 mapping.
        return compiler.process(func.to_boolean(func.cast(col, sqlalchemy.String)), **kw)
    elif dtype in ['integer', 'bigint', 'smallint', 'numeric', 'currency']:
        expr = func.regexp_replace(col, r'\s*', '')
        if trailing_negs:
            expr = sqlalchemy.case(
                (func.regexp_like(expr, '^[0-9]*\\.?[0-9]*-$'), func.concat('-', func.replace(expr, '-', ''))),
                else_=expr
            )
        if dtype == 'integer':
            return compiler.process(func.cast(expr, sqlalchemy.Integer), **kw)
        elif dtype == 'bigint':
            return compiler.process(func.cast(expr, sqlalchemy.BigInteger), **kw)
        elif dtype == 'smallint':
            return compiler.process(func.cast(expr, sqlalchemy.SmallInteger), **kw)
        elif dtype == 'numeric':
            # N.B. the numeric/currency branches call compiler.process without
            # **kw — a faithful replica of the databend variant (literal_binds
            # does not propagate there either); fix both together if ever fixed.
            return compiler.process(
                func.cast(
                    sqlalchemy.case(
                        (func.to_string(expr) == 'NaN', None),
                        else_=expr,
                    ),
                    sqlalchemy.Numeric(38, 10),
                )
            )
        elif dtype == 'currency':
            return compiler.process(
                func.cast(
                    sqlalchemy.case(
                        (func.to_string(expr) == 'NaN', None),
                        else_=expr,
                    ),
                    sqlalchemy.Numeric(18, 4),
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


@compiles(safe_to_timestamp, 'starrocks')
def compile_safe_to_timestamp_starrocks(element, compiler, **kw):
    full_args = list(element.clauses)
    if len(full_args) == 1:
        datetime_format = 'YYYY-MM-DD HH24:MI:SS'
        text = full_args[0]
        args = []
    else:
        text, datetime_format, *args = full_args
        datetime_format = datetime_format.value

    text = func.cast(text, sqlalchemy.Text)
    datetime_format = func.cast(_starrocks_date_format(datetime_format), sqlalchemy.Text)
    if args:
        compiled_args = ', '.join([compiler.process(arg) for arg in args])
        return f"str_to_date({compiler.process(text)}, {compiler.process(datetime_format)}, {compiled_args})"

    return f"str_to_date({compiler.process(text)}, {compiler.process(datetime_format)})"


@compiles(safe_to_timestamp, 'snowflake')
def compile_safe_to_timestamp_snowflake(element, compiler, **kw):
    full_args = list(element.clauses)
    if len(full_args) == 1:
        # Already a valid Snowflake format model — no translation needed.
        datetime_format = 'YYYY-MM-DD HH24:MI:SS'
        text = full_args[0]
        args = []
    else:
        text, datetime_format, *args = full_args
        datetime_format = datetime_format.value
        if datetime_format and '%' in datetime_format:
            datetime_format = python_to_postgres_date_format(datetime_format)
        if datetime_format:
            datetime_format = postgres_to_snowflake_date_format(datetime_format)

    text = func.cast(text, sqlalchemy.Text)
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


# Intentionally overrides SQLAlchemy's built-in `extract` so func.extract picks up
# the timestamp-coercing variant below. Silence the expected override SAWarning.
with warnings.catch_warnings():
    warnings.filterwarnings(
        "ignore",
        message="The GenericFunction 'extract' is already registered.*",
        category=SAWarning,
    )

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


# The generic EXTRACT(<field> FROM <expr>) the default renders is not safe on
# StarRocks: MySQLCompiler.extract_map passes 'dow'/'epoch'/'doy' straight
# through, and StarRocks then treats EXTRACT(unit FROM expr) as a call to a
# builtin function literally named `unit` — which doesn't exist for those
# three (verified live, paul-dev StarRocks 3: "No matching function with
# signature: dow(datetime)" / "epoch(datetime)" / "doy(datetime)"). And
# EXTRACT(week FROM expr) parses but silently uses WEEK's default mode 0
# (Sunday-first), one off from Databend's ISO week (verified live:
# 2026-01-05 → Databend week=2, StarRocks default week=1, WEEK(ts, 3)=2) —
# exactly the MySQL-family-vs-Databend week-start trap this closes.
# year/quarter/month/day/hour/minute/second are plain EXTRACT() on both
# (verified live) so they keep the generic rendering.
_STARROCKS_EXTRACT_PASSTHROUGH = frozenset({
    'year', 'quarter', 'month', 'day', 'hour', 'minute', 'second',
})

#: safe_extract fields with NO verified StarRocks rendering. Kept as an
#: explicit, importable mapping — not just an `else: raise` — so the epic's
#: Tier-3 compatibility derivation (plan §6, "Tier 3, blocked") can enumerate
#: safe_extract's unsupported fields the same way it reads
#: starrocks_unsupported= off _register_geom_fn/_STARROCKS_UNSUPPORTED below:
#: a single class can't use that whole-function mechanism here because most
#: fields DO work. 'epoch'/'epoch_second' are the only fields actually
#: reachable through the expression-catalogue vocabulary (year, quarter,
#: month, week, day, hour, minute, second, dow, doy —
#: plaid/app/ai/expression_catalogue.json:1839) that aren't handled above;
#: anything outside that whole vocabulary still hits the `else` fail-closed.
SAFE_EXTRACT_STARROCKS_UNSUPPORTED_FIELDS = {
    'epoch': "EXTRACT(epoch FROM ...) reaches a StarRocks function literally named epoch(), which doesn't exist "
             '(verified live, paul-dev StarRocks 3: "No matching function with signature: epoch(datetime)")',
    'epoch_second': 'same as epoch — StarRocks has no epoch()/epoch_second() function',
}


@compiles(safe_extract, 'starrocks')
def compile_safe_extract_starrocks(element, compiler, **kw):
    field, timestamp, *args = list(element.clauses)

    field = field.effective_value
    if not isinstance(timestamp.type, (sqlalchemy.TIMESTAMP, sqlalchemy.DateTime, sqlalchemy.Date, sqlalchemy.Interval, PlaidDate, PlaidTimestamp)):
        timestamp = func.to_timestamp(timestamp)

    if field in _STARROCKS_EXTRACT_PASSTHROUGH:
        return compiler.process(sqlalchemy.sql.expression.extract(field, timestamp, *args), **kw)
    elif field == 'week':
        # WEEK(expr, 3): MySQL/StarRocks mode 3 is Monday-first, and (per the
        # MySQL WEEK() mode table) "Week 1 is the first week with 4 or more
        # days in this year" — the ISO 8601 definition — matching Databend's
        # default week numbering (verified live, both for 2026-01-05: Databend
        # EXTRACT(week)=2, StarRocks WEEK(ts,3)=2; StarRocks WEEK(ts,0)
        # (Sunday-first, non-ISO, the EXTRACT default)=1, confirming the two
        # modes actually diverge and mode 3 is the one that agrees).
        return compiler.process(func.week(timestamp, 3), **kw)
    elif field == 'dow':
        # DAYOFWEEK() is documented as 1(Sun)-7(Sat); Databend's EXTRACT(dow)
        # is documented as 0(Sun)-6(Sat) — DAYOFWEEK()-1 converts one
        # convention to the other by definition. Also verified live across a
        # full Sun-Sat week (2026-01-04 Sun .. 2026-01-10 Sat), Databend
        # EXTRACT(dow) vs StarRocks DAYOFWEEK()-1, values by date:
        #   01-04 Sun: 0/0   01-05 Mon: 1/1   01-06 Tue: 2/2  01-07 Wed: 3/3
        #   01-08 Thu: 4/4   01-09 Fri: 5/5   01-10 Sat: 6/6
        # — identical on every day including Sunday, the one day a
        # Mon=1..Sun=7 convention (StarRocks' to_day_of_week, NOT used here)
        # would have disagreed on.
        return compiler.process(func.dayofweek(timestamp) - 1, **kw)
    elif field == 'doy':
        # Verified live: 2026-01-05 -> Databend EXTRACT(doy)=5, StarRocks
        # DAYOFYEAR()=5.
        return compiler.process(func.dayofyear(timestamp), **kw)
    elif field in SAFE_EXTRACT_STARROCKS_UNSUPPORTED_FIELDS:
        raise CompileError(SAFE_EXTRACT_STARROCKS_UNSUPPORTED_FIELDS[field])
    else:
        raise CompileError(f"safe_extract({field!r}, ...) has no verified StarRocks equivalent")


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


@compiles(sql_metric_multiply, 'snowflake')
def compile_sql_metric_multiply_snowflake(element, compiler, **kw):
    # The default's _squash_to_numeric casts through bare NUMERIC —
    # NUMBER(38, 0) on Snowflake — so '1.5K' would round to 2 before the
    # multiplier applies (→ 2000, silently wrong). Fail loud until a
    # scale-preserving variant ships.
    raise CompileError('metric_multiply has no Snowflake variant yet; the default rendering rounds decimals away (bare-NUMERIC squash is NUMBER(38, 0))')


_mark_starrocks_unsupported(
    sql_metric_multiply,
    # Same defect class as the Snowflake variant above, confirmed live
    # (paul-dev StarRocks 3): bare `CAST(x AS DECIMAL)` rounds to scale 0
    # regardless of precision (CAST(1.5 AS DECIMAL) = 2, CAST(2.5 AS DECIMAL)
    # = 3), so _squash_to_numeric rounds away the fraction BEFORE the
    # multiplier applies — CAST('1.5' AS DECIMAL) = 2, so '1.5K' would
    # multiply to 2000 instead of 1500 (confirmed end to end live:
    # CAST(CAST('1.5' AS DECIMAL) * 1000 AS CHAR) = '2000'). Fail loud until
    # a scale-preserving variant ships.
    starrocks_unsupported="metric_multiply has no StarRocks variant yet; the default rendering rounds decimals "
                          "away (bare CAST ... AS DECIMAL rounds to scale 0)")


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

@compiles(sql_numericize, 'starrocks')
def compile_sql_numericize_starrocks(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    arg, = list(element.clauses)

    def sql_only_numeric(text):
        # Returns substring of numeric values only (-, ., numbers, scientific notation)
        cast_text = func.cast(text, sqlalchemy.Text)
        trim_text = func.trim(cast_text)  # trim so that when we check for a sign at the beginning, we ignore spaces
        return func.coalesce(
            func.nullif(
                func.regexp_extract(trim_text, r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))', 0),  # check for valid scientific notation
                '',
            ),
            func.nullif(
                func.regexp_extract(trim_text, r'(^[+\-][0-9\.]+)', 0),  # check for a number prefixed with a sign
                '',
            ),
            func.nullif(
                func.regexp_replace(trim_text, r'[^0-9\.]+', ''),  # remove all the non-numeric characters
                ''
            )
        )

    return compiler.process(sql_only_numeric(arg), **kw)

@compiles(sql_numericize, 'snowflake')
def compile_sql_numericize_snowflake(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    # Snowflake: REGEXP_SUBSTR(subject, pattern) returns the first whole match
    # (NULL when none); 3-arg REGEXP_REPLACE replaces all occurrences
    # (occurrence defaults to 0) — a Postgres-style 'g' 4th argument would
    # error as an invalid <position>.
    arg, = list(element.clauses)

    def sql_only_numeric(text):
        # Returns substring of numeric values only (-, ., numbers, scientific notation)
        cast_text = func.cast(text, sqlalchemy.Text)
        trim_text = func.trim(cast_text)  # trim so that when we check for a sign at the beginning, we ignore spaces
        return func.coalesce(
            func.regexp_substr(trim_text, r'([+\-]?(\d+\.?\d*[Ee][+\-]?\d+))'),  # check for valid scientific notation
            func.regexp_substr(trim_text, r'(^[+\-][0-9\.]+)'),  # check for a number prefixed with a sign
            func.nullif(
                func.regexp_replace(trim_text, r'[^0-9\.]+', ''),  # remove all the non-numeric characters
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


@compiles(sql_integerize_truncate, 'starrocks')
def compile_sql_integerize_truncate_starrocks(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    # sc-30414. StarRocks was sharing the Databend rendering, whose
    # _squash_to_numeric casts through a BARE Numeric. That is DECIMAL(18, 3)
    # on Databend but DECIMAL(10, 0) on StarRocks, which ROUNDS the fraction
    # away BEFORE truncate() ever runs: integerize_truncate('2.7') answered 3
    # on StarRocks and 2 on Databend -- same expression, no error, different
    # value. Pin the intermediate scale so truncate() sees the decimals, the
    # same fix the Snowflake variant below already carries for the identical
    # bare-NUMERIC trap. (38, 10) rather than Databend's (18, 3) because it is
    # the scale every other pinned cast in this module uses; the two agree on
    # every value with <= 3 decimal places, and past that Databend is the one
    # rounding at its own DECIMAL(18, 3) ceiling.
    arg, = list(element.clauses)

    squashed = func.cast(func.nullif(func.numericize(arg), ''), sqlalchemy.Numeric(38, 10))
    return compiler.process(func.cast(func.truncate(squashed), sqlalchemy.Integer), **kw)


@compiles(sql_integerize_truncate, 'snowflake')
def compile_sql_integerize_truncate_snowflake(element, compiler, **kw):
    """
    Turn common number formatting into a number. use metric abbreviations, remove stuff like $, etc.
    """
    # Bare NUMERIC is NUMBER(38, 0) on Snowflake, and Snowflake casts round
    # half away from zero — _squash_to_numeric's plain-NUMERIC cast would round
    # '2.7' to 3 BEFORE trunc. Cast to (38, 10) so trunc sees the decimals.
    arg, = list(element.clauses)

    squashed = func.cast(func.nullif(func.numericize(arg), ''), sqlalchemy.Numeric(38, 10))
    return compiler.process(func.cast(func.trunc(squashed), sqlalchemy.Integer), **kw)

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

#: StarRocks uses RE2, which rejects Java/PCRE `\uXXXX` escapes and spells
#: code points `\x{XXXX}`. The single-letter control escapes (\n \r \f) are
#: valid as-is.
STARROCKS_WW_RE = '[' + ''.join(
    '\\' + c if len(c) == 1 else '\\x{' + c[1:] + '}'
    for c in WEIRD_WHITESPACE_CHARS
) + ']+'

@compiles(sql_normalize_whitespace, 'starrocks')
def compile_sql_normalize_whitespace_starrocks(element, compiler, **kw):
    field, *args = list(element.clauses)
    field = func.cast(field, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(field, STARROCKS_WW_RE, ' ')
    )

#: Snowflake regex is POSIX ERE plus only the documented \d/\s/\w-style Perl
#: shorthands — no \uXXXX (Java) or \x{XXXX} (RE2) code-point escapes — so the
#: class is spelled with the literal characters, which any POSIX bracket
#: expression accepts.
SNOWFLAKE_WW_RE = '[' + ''.join(
    {'n': '\n', 'r': '\r', 'f': '\f'}[c] if len(c) == 1 else chr(int(c[1:], 16))
    for c in WEIRD_WHITESPACE_CHARS
) + ']+'

@compiles(sql_normalize_whitespace, 'snowflake')
def compile_sql_normalize_whitespace_snowflake(element, compiler, **kw):
    # 3-arg regexp_replace: Snowflake replaces all occurrences by default; the
    # Postgres 'g' flag the default emits would error as an invalid <position>.
    field, *args = list(element.clauses)
    field = func.cast(field, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(field, SNOWFLAKE_WW_RE, ' ')
    )

class safe_unix_to_timestamp(GenericFunction):
    name = 'unix_to_timestamp'

@compiles(safe_unix_to_timestamp)
def compile_safe_unix_to_timestamp(element, compiler, **kw):
    timestamp, *args = list(element.clauses)
    timestamp = func.cast(timestamp, sqlalchemy.Integer)

    return f"to_timestamp({compiler.process(timestamp)})"


@compiles(safe_unix_to_timestamp, 'starrocks')
def compile_safe_unix_to_timestamp_starrocks(element, compiler, **kw):
    # StarRocks has no to_timestamp(); FROM_UNIXTIME(seconds) is the
    # equivalent, but bare FROM_UNIXTIME resolves in the SESSION time zone,
    # not UTC -- Databend's to_timestamp(int) is UTC. Verified live at
    # session time_zone='Etc/UTC' (the query default): FROM_UNIXTIME(x) =
    # '2026-01-05 13:45:30', matching Databend's to_timestamp(x). Under
    # time_zone='America/New_York' (StarRocks SET_VAR hint), the SAME bare
    # FROM_UNIXTIME(x) shifted to '08:45:30' -- 5 hours off -- while
    # CONVERT_TZ(FROM_UNIXTIME(x), @@session.time_zone, 'UTC') stayed at
    # '13:45:30' under both sessions. CONVERT_TZ pins the result to UTC
    # regardless of the connection's session time zone, matching Databend
    # unconditionally rather than only when the session happens to be UTC.
    #
    # Known, narrower gap than the pre-fix state (still an improvement, not
    # a regression -- the prior default reached a StarRocks function that
    # doesn't exist at all): negative epochs and values > 253402300799
    # (year 9999) return NULL on StarRocks where Databend returns a real
    # date (verified live: FROM_UNIXTIME(-86400) = NULL on StarRocks,
    # to_timestamp(-86400) = '1969-12-31' on Databend). Out of scope here --
    # unix_to_timestamp's realistic domain is post-1970 business-data
    # timestamps.
    timestamp, *args = list(element.clauses)
    timestamp = func.cast(timestamp, sqlalchemy.Integer)

    return (
        "convert_tz(from_unixtime(%s), @@session.time_zone, 'UTC')"
        % compiler.process(timestamp)
    )


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

@compiles(safe_to_date, 'starrocks')
def compile_safe_to_date_starrocks(element, compiler, **kw):
    text, *args = list(element.clauses)
    if len(args):
        date_format = args[0].value
        if date_format:
            if '%' in date_format:
                date_format = python_to_postgres_date_format(date_format)
            date_format = date_format_from_datetime_format(date_format)
            date_format = postgres_to_starrocks_date_format(date_format)
        return f"str2date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)}, {compiler.process(func.cast(date_format, sqlalchemy.Text))})"

    return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)})"

@compiles(safe_to_date, 'snowflake')
def compile_safe_to_date_snowflake(element, compiler, **kw):
    # TO_DATE(<string>, <format>) parses time elements in the format and
    # discards them, so the full (translated) format passes straight through.
    text, *args = list(element.clauses)
    if len(args):
        date_format = args[0].value
        if date_format and '%' in date_format:
            date_format = python_to_postgres_date_format(date_format)
        if date_format:
            date_format = postgres_to_snowflake_date_format(date_format)
        return f"to_date({compiler.process(func.nullif(func.trim(func.cast(text, sqlalchemy.Text)), ''), **kw)}, {compiler.process(func.cast(date_format, sqlalchemy.Text))})"

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

    number = func.cast(number, sqlalchemy.Numeric(38, 10))
    # Starrocks does not like this and it seems overkill
    # if digits is not None:
    #     digits = func.cast(digits, sqlalchemy.Integer)

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


#: StarRocks' 2-arg LTRIM/RTRIM/TRIM(str, chars) strips a CHARACTER SET —
#: repeatedly removing any of the individual characters in `chars` — not the
#: literal repeated substring Databend's TRIM(LEADING/TRAILING chars FROM str)
#: strips. Confirmed live (paul-dev StarRocks 3 vs a Databend tenant, same
#: input): LTRIM('454312', '54') -> '312' on StarRocks (strips leading chars
#: that are '5' or '4') but Databend's TRIM(LEADING '54' FROM '454312') is
#: unchanged (the string doesn't literally start with "54"). An anchored
#: REGEXP_REPLACE of the escaped literal, repeated, reproduces Databend's
#: contract instead (verified live: matches on '454312'/'54', '12345'/'54',
#: 'ababcd'/'ab', '00123'/'0', '12300'/'0').
def _starrocks_trim_pattern(chars, *, leading, trailing):
    escaped = re.escape(chars)
    parts = []
    if leading:
        parts.append(f"^({escaped})+")
    if trailing:
        parts.append(f"({escaped})+$")
    return '|'.join(parts)


def _starrocks_strip_chars(element, compiler, *, leading, trailing, bare_name, **kw):
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    if args:
        if len(args) > 1:
            raise CompileError(f'{bare_name} with more than one extra argument has no verified StarRocks rendering')
        chars = args[0].value
        if chars != '':
            if leading and trailing:
                # A single alternated pattern only strips the first match it
                # finds scanning left-to-right (verified live: '^(0)+|(0)+$'
                # against '0012300' stripped only the leading zeros) — apply
                # the two anchors as two passes instead.
                stripped = func.regexp_replace(text, _starrocks_trim_pattern(chars, leading=True, trailing=False), '')
                stripped = func.regexp_replace(stripped, _starrocks_trim_pattern(chars, leading=False, trailing=True), '')
            else:
                stripped = func.regexp_replace(text, _starrocks_trim_pattern(chars, leading=leading, trailing=trailing), '')
            return compiler.process(stripped, **kw)

    return f"{bare_name}({compiler.process(text, **kw)})"


@compiles(safe_ltrim, 'starrocks')
def compile_safe_ltrim_starrocks(element, compiler, **kw):
    return _starrocks_strip_chars(element, compiler, leading=True, trailing=False, bare_name='ltrim', **kw)


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


@compiles(safe_rtrim, 'starrocks')
def compile_safe_rtrim_starrocks(element, compiler, **kw):
    return _starrocks_strip_chars(element, compiler, leading=False, trailing=True, bare_name='rtrim', **kw)


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


@compiles(safe_trim, 'starrocks')
def compile_safe_trim_starrocks(element, compiler, **kw):
    return _starrocks_strip_chars(element, compiler, leading=True, trailing=True, bare_name='trim', **kw)


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

@compiles(sql_only_ascii, 'starrocks')
def compile_sql_only_ascii_starrocks(element, compiler, **kw):
    # Remove non-ascii characters. StarRocks rejects the 4-arg
    # regexp_replace(varchar, varchar, varchar, varchar) form the default emits,
    # so use the 3-arg form. The `[[:ascii:]]` POSIX class is supported by RE2.
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(text, r'[^[:ascii:]]+', ''),
        **kw
    )

#: `[[:ascii:]]` is a PCRE extension, not one of the POSIX classes Snowflake's
#: documented POSIX-ERE engine provides, and Snowflake has no \x{…} escapes —
#: spell the ASCII range with literal characters (NUL excluded: it cannot ride
#: in a string and never survives a VARCHAR anyway).
SNOWFLAKE_NON_ASCII_RE = '[^\x01-\x7f]+'

@compiles(sql_only_ascii, 'snowflake')
def compile_sql_only_ascii_snowflake(element, compiler, **kw):
    # Remove non-ascii characters. 3-arg regexp_replace replaces all
    # occurrences on Snowflake; the default's 4th-position 'g' flag would
    # error as an invalid <position>.
    text, *args = list(element.clauses)
    text = func.cast(text, sqlalchemy.Text)

    return compiler.process(
        func.regexp_replace(text, SNOWFLAKE_NON_ASCII_RE, ''),
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

@compiles(sql_safe_divide, 'starrocks')
def compile_safe_divide_starrocks(element, compiler, **kw):
    """Divides numerator by denominator, returning NULL if the denominator is 0.
    """
    clauses = list(element.clauses)
    numerator = clauses[0]
    denominator = clauses[1]
    divide_by_zero_value = clauses[2] if len(clauses) > 2 else None
    numerator = func.cast(numerator, sqlalchemy.Numeric(38, 10))
    denominator = func.cast(denominator, sqlalchemy.Numeric(38, 10))

    basic_safe_divide = numerator / func.nullif(denominator, 0)
    # NOTE: in SQL, x/NULL = NULL, for all x.

    # Skip the coalesce if it's not necessary
    return compiler.process(
        basic_safe_divide if divide_by_zero_value is None else func.coalesce(basic_safe_divide, divide_by_zero_value)
    )

@compiles(sql_safe_divide, 'snowflake')
def compile_safe_divide_snowflake(element, compiler, **kw):
    """Divides numerator by denominator, returning NULL if the denominator is 0.
    """
    # Operands are cast to (38, 10) because bare NUMERIC is NUMBER(38, 0) on
    # Snowflake. The division renders by hand: under snowflake-sqlalchemy's
    # div_is_floordiv default, SQLAlchemy's truediv rendering wraps the divisor
    # in CAST(... AS NUMERIC) — NUMBER(38, 0) — which would round a fractional
    # denominator (0.4 → 0) and divide by zero past the nullif guard.
    clauses = list(element.clauses)
    numerator = func.cast(clauses[0], sqlalchemy.Numeric(38, 10))
    denominator = func.nullif(func.cast(clauses[1], sqlalchemy.Numeric(38, 10)), 0)
    divide_by_zero_value = clauses[2] if len(clauses) > 2 else None

    quotient = f"{compiler.process(numerator, **kw)} / {compiler.process(denominator, **kw)}"
    # NOTE: in SQL, x/NULL = NULL, for all x.
    if divide_by_zero_value is None:
        return quotient
    return f"coalesce({quotient}, {compiler.process(divide_by_zero_value, **kw)})"

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

@compiles(sql_date_add, 'starrocks')
def compile_sql_date_add_starrocks(element, compiler, **kw):
    dt, *args = list(element.clauses)
    expr = func.cast(dt, sqlalchemy.DateTime)
    starrocks_units = [
        ('years', 'years_add'),
        ('months', 'months_add'),
        ('weeks', 'weeks_add'),
        ('days', 'days_add'),
        ('hours', 'hours_add'),
        ('minutes', 'minutes_add'),
        ('seconds', 'seconds_add'),
    ]
    for unit, fn_name in starrocks_units:
        value = element.additions[unit]
        if isinstance(value, (int, float)) and value == 0:
            continue
        expr = getattr(func, fn_name)(expr, value)
    return compiler.process(expr, **kw)

@compiles(sql_date_add, 'snowflake')
def compile_sql_date_add_snowflake(element, compiler, **kw):
    # No make_interval on Snowflake; compose DATEADD(<part>, <n>, <expr>) per
    # non-zero unit (the starrocks pattern). The unit renders as an unquoted
    # keyword via text() because DATEADD requires a constant date part.
    dt, *args = list(element.clauses)
    expr = func.cast(dt, sqlalchemy.DateTime)
    for unit in DATE_ADD_UNITS:
        value = element.additions[unit]
        if isinstance(value, (int, float)) and value == 0:
            continue
        expr = func.dateadd(sqlalchemy.text(unit[:-1]), value, expr)
    return compiler.process(expr, **kw)

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


#: sc-30414. to_char's Postgres NUMERIC masks (0/9 grouping, currency, fixed
#: decimals) have no StarRocks equivalent. Kept as an importable message — not
#: just an inline `raise` — for the same reason as
#: SAFE_EXTRACT_STARROCKS_UNSUPPORTED_FIELDS above: the whole function DOES
#: work on StarRocks for a date mask and for no mask at all, so the Tier-3
#: derivation cannot use the whole-class _STARROCKS_UNSUPPORTED mechanism.
TO_CHAR_STARROCKS_UNSUPPORTED_NUMERIC_MASK = (
    "to_char(<number>, '<0/9 mask>') has no StarRocks equivalent: StarRocks has no Postgres-style numeric "
    'format model, so the grouping separators, currency symbol and fixed decimal places the mask asks for '
    'would be dropped and the column would hold a different STRING than it does on Databend '
    "(to_char(1234.5, '999,999.99') is ' 1,234.50' on Databend and '1234.5' on StarRocks)"
)


@compiles(sql_to_char, 'starrocks')
def compile_to_char_starrocks(element, compiler, **kw):
    # StarRocks has no Postgres-style to_char. Dates render via date_format
    # (MySQL specifiers); no mask at all casts to a string.
    #
    # sc-30414: a NUMERIC mask used to degrade silently to CAST(... AS CHAR).
    # to_char returns a STRING, so dropping the mask IS a different value, not
    # a cosmetic difference — it fails loudly now, so pre-flight blocks the
    # project instead of the migration rewriting the column's contents.
    source, *args = list(element.clauses)
    if args:
        format_, *args = args
        format_ = format_.effective_value
    else:
        format_ = None

    if format_ is not None and ('0' in format_ or '9' in format_):
        raise CompileError(TO_CHAR_STARROCKS_UNSUPPORTED_NUMERIC_MASK)

    if format_ is None:
        return f"CAST({compiler.process(source)} AS CHAR)"

    return f"date_format({compiler.process(source)}, {compiler.process(sqlalchemy.literal(_starrocks_date_format(format_)))})"


@compiles(sql_to_char, 'snowflake')
def compile_to_char_snowflake(element, compiler, **kw):
    # Rendered via the TO_VARCHAR synonym so this compiler doesn't re-enter
    # itself through func.to_char. Snowflake's numeric format models cover
    # 0/9/,/./D/G/$/S/MI/B/X/TM plus the FM modifier natively; only the locale
    # currency element L needs translating (→ $). Date formats go through the
    # WS-B3 token translator.
    source, *args = list(element.clauses)
    if args:
        format_, *args = args
        format_ = format_.effective_value
    else:
        format_ = None

    if format_ is None:
        return compiler.process(func.to_varchar(source), **kw)

    if '0' in format_ or '9' in format_:
        return compiler.process(func.to_varchar(source, format_.replace('L', '$')), **kw)

    if '%' in format_:
        format_ = python_to_postgres_date_format(format_)
    return compiler.process(func.to_varchar(source, postgres_to_snowflake_date_format(format_)), **kw)


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

@compiles(sql_to_number, 'snowflake')
def compile_to_number_snowflake(element, compiler, **kw):
    # Snowflake TO_NUMBER understands the 0/9/D/G/MI-style masks natively, but
    # without an explicit precision/scale it returns NUMBER(38, 0) — rounding
    # every fractional digit away. Pin (38, 10).
    string, format_ = list(element.clauses)
    return f"to_number({compiler.process(string, **kw)}, {compiler.process(format_, **kw)}, 38, 10)"

_mark_starrocks_unsupported(
    sql_to_number,
    # sc-30414. StarRocks has no to_number(). The variant this replaces cast to
    # DECIMAL(38, 10) and dropped the mask, but Databend's to_number renders
    # to_int64 — an INTEGER. to_number('12.7', '999999') was therefore 12.7 on
    # StarRocks and an integer on Databend: a different type and a different
    # value, silently. A matching rendering would have to reproduce to_int64's
    # own rounding/refusal behaviour on a fractional or unparseable string,
    # which cannot be established without a live Databend/StarRocks pair, so
    # this fails closed instead. Nothing inside this module targets it any
    # more — import_cast's trailing-negatives path was rebuilt in sc-30414 out
    # of regexp/concat/replace rather than routed through here.
    starrocks_unsupported='to_number has no verified StarRocks equivalent: Databend renders it as to_int64 '
                          '(an integer) and StarRocks has no to_number, so any StarRocks rendering returns a '
                          'different type and a different value for a fractional input')


class sql_transaction_timestamp(GenericFunction):
    name = 'transaction_timestamp'

@compiles(sql_transaction_timestamp, 'databend')
def compile_transaction_timestamp(element, compiler, **kw):
    # Not available in databend
    return compiler.process(
        func.now()
    )

@compiles(sql_transaction_timestamp, 'starrocks')
def compile_transaction_timestamp_starrocks(element, compiler, **kw):
    # StarRocks has no transaction_timestamp(); now() is the equivalent.
    return compiler.process(
        func.now()
    )

@compiles(sql_transaction_timestamp, 'snowflake')
def compile_transaction_timestamp_snowflake(element, compiler, **kw):
    # Snowflake has no transaction_timestamp(); func.now() renders the
    # dialect's CURRENT_TIMESTAMP.
    return compiler.process(
        func.now()
    )

class sql_strpos(GenericFunction):
    name = 'strpos'

@compiles(sql_strpos, 'databend', 'starrocks')
def compile_strpos(element, compiler, **kw):
    # Bare strpos() is valid on Databend but reaches a MySQL-protocol engine
    # with no such function on StarRocks; LOCATE(needle, haystack) matches
    # strpos's contract (1-based position, 0 when absent) on both — verified
    # live on StarRocks (paul-dev StarRocks 3).
    string, substring = list(element.clauses)
    return compiler.process(
        func.locate(substring, string)
    )

@compiles(sql_strpos, 'snowflake')
def compile_strpos_snowflake(element, compiler, **kw):
    # Snowflake has no strpos; CHARINDEX(needle, haystack) matches its
    # contract (1-based position, 0 when absent).
    string, substring = list(element.clauses)
    return compiler.process(
        func.charindex(substring, string)
    )

class sql_string_to_array(GenericFunction):
    name = 'string_to_array'

@compiles(sql_string_to_array, 'databend', 'starrocks', 'snowflake')
def compile_string_to_array(element, compiler, **kw):
    # split() returns an ARRAY on Databend, StarRocks and Snowflake;
    # null_string is not supported on any of them. Snowflake documents an
    # empty separator as yielding the whole string as a single element,
    # matching the CASE-normalized delimiter here.
    string, delimiter, *args = list(element.clauses)

    split_array = func.split(
        string,
        sqlalchemy.case(
            (sqlalchemy.or_(delimiter == '', delimiter.is_(None)), ''),
            else_=delimiter
        )
    )
    return compiler.process(split_array)

class quantile_tdigest(GenericFunction):
    type = Double()
    name = "QUANTILE_TDIGEST"
    inherit_cache = True


@compiles(quantile_tdigest)
def default_quantile_tdigest(element, compiler, **kw):
    level, expr = list(element.clauses)
    return f"{element.name}({compiler.process(level, **kw)})({compiler.process(expr, **kw)})"

@compiles(quantile_tdigest, 'snowflake')
def snowflake_quantile_tdigest(element, compiler, **kw):
    # Snowflake's APPROX_PERCENTILE is itself t-digest-based; arguments are
    # (expr, percentile) — reversed from the ClickHouse-style (level)(expr).
    level, expr = list(element.clauses)
    return f"APPROX_PERCENTILE({compiler.process(expr, **kw)}, {compiler.process(level, **kw)})"

_mark_starrocks_unsupported(
    quantile_tdigest,
    # StarRocks' PERCENTILE_APPROX is an approximate-quantile aggregate like
    # QUANTILE_TDIGEST, but StarRocks' docs do not say which sketch algorithm
    # backs it (unlike Snowflake's APPROX_PERCENTILE, documented as
    # t-digest-based). Two different approximate algorithms are not
    # guaranteed to agree on a value — fail loud rather than assume equivalence.
    starrocks_unsupported='QUANTILE_TDIGEST has no verified StarRocks equivalent: PERCENTILE_APPROX exists but '
                          'its underlying algorithm is not documented as t-digest, so value equivalence is unconfirmed')

class quantile_cont(GenericFunction):
    type = Double()
    name = "QUANTILE_CONT"
    inherit_cache = True


@compiles(quantile_cont)
def default_quantile_cont(element, compiler, **kw):
    level, expr = list(element.clauses)
    return f"{element.name}({compiler.process(level, **kw)})({compiler.process(expr, **kw)})"

@compiles(quantile_cont, 'snowflake')
def snowflake_quantile_cont(element, compiler, **kw):
    level, expr = list(element.clauses)
    return f"PERCENTILE_CONT({compiler.process(level, **kw)}) WITHIN GROUP (ORDER BY {compiler.process(expr, **kw)})"

@compiles(quantile_cont, 'starrocks')
def starrocks_quantile_cont(element, compiler, **kw):
    # StarRocks PERCENTILE_CONT(expr, percentile) is the same linear-
    # interpolation definition as Databend's QUANTILE_CONT — verified live,
    # matching values on both odd-count (unambiguous rank) and even-count
    # (interpolated midpoint) fixtures.
    level, expr = list(element.clauses)
    return f"percentile_cont({compiler.process(expr, **kw)}, {compiler.process(level, **kw)})"

class quantile_disc(GenericFunction):
    type = Double()
    name = "QUANTILE_DISC"
    inherit_cache = True


@compiles(quantile_disc)
def default_quantile_disc(element, compiler, **kw):
    level, expr = list(element.clauses)
    return f"{element.name}({compiler.process(level, **kw)})({compiler.process(expr, **kw)})"

@compiles(quantile_disc, 'snowflake')
def snowflake_quantile_disc(element, compiler, **kw):
    level, expr = list(element.clauses)
    return f"PERCENTILE_DISC({compiler.process(level, **kw)}) WITHIN GROUP (ORDER BY {compiler.process(expr, **kw)})"

_mark_starrocks_unsupported(
    quantile_disc,
    # NOT a transparent rename: StarRocks' PERCENTILE_DISC disagrees with
    # Databend's QUANTILE_DISC on tie-breaking at an even-count exact-half
    # percentile — verified live, same 4-row fixture: quantile_disc(0.5) over
    # [1,2,3,4] on Databend = 2 (lower middle), percentile_disc(x, 0.5) over
    # the same rows on StarRocks = 3 (upper middle). Odd-count fixtures (no
    # tie) matched on both, which is what made this easy to miss without a
    # live check. Fail loud rather than emit a value that silently drifts on
    # even-sized groups.
    starrocks_unsupported='QUANTILE_DISC has no verified StarRocks equivalent: PERCENTILE_DISC breaks even-count '
                          'ties toward the upper value where Databend breaks toward the lower one')

class quantile_tdigest_weighted(GenericFunction):
    type = Double()
    name = "QUANTILE_TDIGEST_WEIGHTED"
    inherit_cache = True


@compiles(quantile_tdigest_weighted)
def default_quantile_tdigest_weighted(element, compiler, **kw):
    level, expr, weight = list(element.clauses)
    return f"{element.name}({compiler.process(level, **kw)})({compiler.process(expr, **kw)}, {compiler.process(weight, **kw)})"

@compiles(quantile_tdigest_weighted, 'snowflake')
def snowflake_quantile_tdigest_weighted(element, compiler, **kw):
    # Snowflake has no weighted percentile aggregate — fail loud rather than
    # emit an unweighted approximation that silently changes the statistic.
    raise CompileError('QUANTILE_TDIGEST_WEIGHTED has no Snowflake equivalent (no weighted percentile aggregate)')

_mark_starrocks_unsupported(
    quantile_tdigest_weighted,
    # StarRocks' PERCENTILE_APPROX_WEIGHT exists and takes a weight per the
    # same contract, but it inherits PERCENTILE_APPROX's undocumented
    # algorithm (see quantile_tdigest above) — fail loud rather than assume
    # equivalence with QUANTILE_TDIGEST_WEIGHTED's t-digest sketch.
    starrocks_unsupported='QUANTILE_TDIGEST_WEIGHTED has no verified StarRocks equivalent: '
                          'PERCENTILE_APPROX_WEIGHT exists but its underlying algorithm is not documented as '
                          't-digest, so value equivalence is unconfirmed')


# ---------------------------------------------------------------------------
# Alteryx-converter cross-dialect functions
# ---------------------------------------------------------------------------
# The Alteryx expression converter (plaid app/analyze/utility/
# alteryx_expression_converter.py) emits Databend function names. Other engines
# spell, arg-order, or lack several of them. Each function below leaves the
# Databend/default spelling untouched (built-in GenericFunction rendering) and
# adds only per-dialect specializations — so existing Databend SQL is
# byte-for-byte preserved while each other engine gets valid SQL. StarRocks
# behavior of every emission here was verified live (paul-dev, StarRocks 3 /
# MySQL 8.0.33 protocol); Snowflake emissions are verified against the
# Snowflake function reference (sc-23158 WS-B).

#: Per-dialect pure renames: Databend function name → dialect's name, when the
#: only difference is the spelling (same arguments, same order). Snowflake
#: ships regexp_instr natively (same 1-based-position / 0-on-no-match
#: contract), so it needs no entry there; the add_<unit>s family is an
#: argument reorder on Snowflake (DATEADD takes the unit first), not a rename
#: — see _SNOWFLAKE_DATE_ADD_FUNCS below.
_FUNCTION_RENAMES = {
    'starrocks': {
        'modulo': 'mod',           # Alteryx Mod()
        'ord': 'ascii',            # Alteryx CharToInt() (StarRocks has no ord)
        'today': 'current_date',   # Alteryx DateTimeToday()
        'to_year': 'year', 'to_month': 'month', 'to_day_of_month': 'day',
        'to_hour': 'hour', 'to_minute': 'minute', 'to_second': 'second',
        'add_years': 'years_add', 'add_months': 'months_add', 'add_days': 'days_add',
        'add_hours': 'hours_add', 'add_minutes': 'minutes_add', 'add_seconds': 'seconds_add',
    },
    'snowflake': {
        'modulo': 'mod',           # MOD(expr1, expr2)
        'ord': 'ascii',            # ASCII(<string>)
        'today': 'current_date',   # CURRENT_DATE() — parentheses form is valid
        'to_year': 'year', 'to_month': 'month', 'to_day_of_month': 'day',
        'to_hour': 'hour', 'to_minute': 'minute', 'to_second': 'second',
    },
}


def _register_rename(databend_name, targets_by_dialect):
    func_cls = type(databend_name, (GenericFunction,),
                    {'name': databend_name, 'inherit_cache': True})
    globals()[databend_name] = func_cls

    for target_dialect, target_name in targets_by_dialect.items():
        @compiles(func_cls, target_dialect)
        def _compile(element, compiler, _name=target_name, **kw):
            rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
            return f"{_name}({rendered})"


_RENAME_TARGETS = {}
for _dialect_name, _renames in _FUNCTION_RENAMES.items():
    for _db_name, _target_name in _renames.items():
        _RENAME_TARGETS.setdefault(_db_name, {})[_dialect_name] = _target_name
for _db_name, _targets in _RENAME_TARGETS.items():
    _register_rename(_db_name, _targets)


#: Alteryx converter add_<unit>s(dt, n) → Snowflake DATEADD(<unit>, n, dt) —
#: an argument reorder, not a rename. The unit renders as an unquoted keyword
#: because DATEADD requires a constant date part.
_SNOWFLAKE_DATE_ADD_FUNCS = {
    'add_years': 'year', 'add_months': 'month', 'add_days': 'day',
    'add_hours': 'hour', 'add_minutes': 'minute', 'add_seconds': 'second',
}


def _register_snowflake_dateadd(func_cls, unit):
    @compiles(func_cls, 'snowflake')
    def _compile(element, compiler, _unit=unit, **kw):
        dt, n = list(element.clauses)
        return f"dateadd({_unit}, {compiler.process(n, **kw)}, {compiler.process(dt, **kw)})"


for _fn_name, _unit in _SNOWFLAKE_DATE_ADD_FUNCS.items():
    _register_snowflake_dateadd(globals()[_fn_name], _unit)


class array_tail(GenericFunction):
    """array_tail(array, offset): the array from 1-based `offset` to the end.

    Named rather than reusing `slice` because SQLAlchemy already registers that
    name for the PostgreSQL hstore slice function.
    """
    name = 'array_tail'
    inherit_cache = True

@compiles(array_tail)
def compile_array_tail(element, compiler, **kw):
    rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
    return f'slice({rendered})'

@compiles(array_tail, 'starrocks')
def compile_array_tail_starrocks(element, compiler, **kw):
    rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
    return f'array_slice({rendered})'


class string_agg(GenericFunction):
    name = 'string_agg'
    inherit_cache = True

@compiles(string_agg, 'starrocks')
def compile_string_agg_starrocks(element, compiler, **kw):
    # StarRocks has no string_agg; group_concat is the equivalent, but it takes
    # the delimiter as a SEPARATOR clause — passing it as a second argument
    # concatenates it onto every value instead ('a-,b-,c-' rather than 'a-b-c').
    value, *separator = list(element.clauses)
    rendered = compiler.process(value, **kw)
    if separator:
        rendered += f' SEPARATOR {compiler.process(separator[0], **kw)}'
    return f'group_concat({rendered})'


class titlecase(GenericFunction):
    r"""Alteryx TitleCase() -- upper-case the first letter of each word.

    A word is a run of alphanumerics; every non-alphanumeric character delimits
    one, so `o'brien-smith` titles to `O'Brien-Smith`. That is StarRocks' native
    initcap, and the contract the other dialects render to.

    StarRocks and Snowflake have a conforming builtin (Snowflake's only once
    given an explicit delimiter set). Databricks' initcap splits on whitespace
    alone and would answer `O'brien`; Databend and DuckDB have none at all. Those
    three are rendered by hand. Anything else still raises.
    """
    name = 'titlecase'
    inherit_cache = True


def _titlecase_argument(element, compiler, **kw):
    """The one string argument of a titlecase() call, rendered.

    Guarded because the renders below interpolate it into a larger expression,
    where a second clause would land inside regexp_replace's argument list and
    fail out at the warehouse instead of here.
    """
    clauses = list(element.clauses)
    if len(clauses) != 1:
        raise CompileError(
            f'titlecase (Alteryx TitleCase) takes exactly one argument, got {len(clauses)}.')
    return compiler.process(clauses[0], **kw)


def _render_titlecase_by_words(value, *, word_pattern, group_ref, replace_tail,
                               sentinel, split_fn, transform_fn, join_fn):
    r"""Render Alteryx TitleCase where the dialect's builtins cannot express it.

    Marks each word start with a sentinel, splits there so every chunk is one
    word plus its trailing delimiters, cases the chunk, and rejoins. Delimiters
    are never removed, so they survive verbatim.

    One array element per word: the alternative, one per character looking back
    at the previous one, is equally exact but allocates an array as long as the
    string on every row. `value` is interpolated once, so the column expression
    is evaluated once however deeply it nests.

    The sentinel is U+0001 as a function call rather than a `\x01` escape,
    because dialects disagree on escape processing inside string literals. A
    U+0001 already in the data reads as a word break -- cosmetic, on a character
    no text corpus carries.

    Args:
        value (str): rendered string expression to title-case.
        word_pattern (str): regex capturing one word, spelled for this dialect's
            literal-escaping rules.
        group_ref (str): how the replacement back-references that capture.
        replace_tail (str): extra regexp_replace arguments needed to make the
            replacement global; empty where it already is.
        sentinel (str): expression yielding the U+0001 marker.
        split_fn (str): split-string-to-array function name.
        transform_fn (str): map-over-array function name.
        join_fn (str): join-array-to-string function name.

    Returns:
        str: the rendered title-case expression.
    """
    marked = (f"regexp_replace({value}, '{word_pattern}', "
              f"concat({sentinel}, '{group_ref}'){replace_tail})")
    chunks = f'{split_fn}({marked}, {sentinel})'
    cased = (f'{transform_fn}({chunks}, tc_word -> '
             f'concat(upper(substr(tc_word, 1, 1)), lower(substr(tc_word, 2))))')
    return f"{join_fn}({cased}, '')"


#: Unicode-aware because StarRocks classifies with ICU: `ñ` is a word character
#: there, where ASCII-only `[[:alnum:]]` would title `café ñino` as `Café ÑIno`.
#: Spelled twice because Databend and Databricks unescape backslashes inside
#: string literals and DuckDB does not.
_TITLECASE_WORD_ESCAPED = r'([\\p{L}\\p{N}]+)'
_TITLECASE_WORD_RAW = r'([\p{L}\p{N}]+)'

#: Snowflake's INITCAP delimiter default omits the apostrophe, backtick and
#: equals sign, returning `O'brien` where StarRocks gives `O'Brien`; passing the
#: set explicitly closes that gap across ASCII. Non-ASCII punctuation still fails
#: to delimit there, and cannot be enumerated. The hand-rolled render is no help
#: either: Snowflake's SPLIT yields VARIANT elements, so it would need casts
#: nothing here can check against a live warehouse.
_SNOWFLAKE_TITLECASE_DELIMITERS = ''' \t\n\r\f!?@"^#$&~_,.:;+-*%/|\\[](){}<>'`='''


@compiles(titlecase, 'starrocks')
def compile_titlecase_starrocks(element, compiler, **kw):
    return f'initcap({_titlecase_argument(element, compiler, **kw)})'


@compiles(titlecase, 'snowflake')
def compile_titlecase_snowflake(element, compiler, **kw):
    # A Snowflake literal processes both backslash escapes and doubled quotes:
    # the backslash delimiter has to survive as one character, and the apostrophe
    # must not close the string.
    delimiters = (_SNOWFLAKE_TITLECASE_DELIMITERS
                  .replace('\\', '\\\\').replace("'", "''")
                  .replace('\t', '\\t').replace('\n', '\\n')
                  .replace('\r', '\\r').replace('\f', '\\f'))
    return f"initcap({_titlecase_argument(element, compiler, **kw)}, '{delimiters}')"


@compiles(titlecase, 'databend')
def compile_titlecase_databend(element, compiler, **kw):
    # No initcap under any alias; system.functions carries only
    # upper/lower/ucase/lcase for case handling.
    return _render_titlecase_by_words(
        _titlecase_argument(element, compiler, **kw),
        word_pattern=_TITLECASE_WORD_ESCAPED,
        group_ref='$1',
        replace_tail='',
        sentinel='char(1)',
        split_fn='split',
        transform_fn='array_transform',
        join_fn='array_to_string',
    )


@compiles(titlecase, 'databricks')
def compile_titlecase_databricks(element, compiler, **kw):
    # Rendered by hand precisely *because* Databricks has initcap: Spark's splits
    # on whitespace alone, so it would return `O'brien` silently rather than
    # fail, and a wrong answer is worse than Databend's missing function. Spark's
    # split() takes a regex, but U+0001 carries no regex meaning.
    return _render_titlecase_by_words(
        _titlecase_argument(element, compiler, **kw),
        word_pattern=_TITLECASE_WORD_ESCAPED,
        group_ref='$1',
        replace_tail='',
        sentinel='char(1)',
        split_fn='split',
        transform_fn='transform',
        join_fn='array_join',
    )


@compiles(titlecase, 'duckdb')
def compile_titlecase_duckdb(element, compiler, **kw):
    # The Alteryx isolation harness engine, which has no initcap either -- without
    # a render here TitleCase cannot be measured at all. Its regexp_replace needs
    # the 'g' flag to replace past the first match, and spells the back-reference
    # RE2-style as \1 rather than $1.
    return _render_titlecase_by_words(
        _titlecase_argument(element, compiler, **kw),
        word_pattern=_TITLECASE_WORD_RAW,
        group_ref='\\1',
        replace_tail=", 'g'",
        sentinel='chr(1)',
        split_fn='str_split',
        transform_fn='list_transform',
        join_fn='array_to_string',
    )


@compiles(titlecase)
def compile_titlecase_default(element, compiler, **kw):
    # Every warehouse PlaidCloud targets is specialized above; the bare default
    # still fails loudly so a fifth cannot reach a customer by inheriting a
    # rendering that was never checked against it.
    raise CompileError(
        f'titlecase (Alteryx TitleCase) has no {compiler.dialect.name} rendering; '
        'run this workflow on a supported warehouse (Databend, StarRocks, Snowflake '
        'or Databricks), or replace the TitleCase call.')


class median(GenericFunction):
    """Alteryx Median aggregate."""
    name = 'median'
    inherit_cache = True

@compiles(median, 'starrocks')
def compile_median_starrocks(element, compiler, **kw):
    # StarRocks has no median(). percentile_cont(col, 0.5) is exact and linearly
    # interpolates the two middle values on an even count -- the same answer both
    # Alteryx's Summarize->Median and Databend's median() give. The two
    # alternatives both diverge from Databend on the same data and so cannot be
    # used for a cross-warehouse median: percentile_disc returns an actual member
    # (3, not 2.5, for [1, 2, 3, 10]) and percentile_approx is a t-digest estimate.
    # percentile_cont accepts numeric, DATE and DATETIME (percentile_approx is
    # numeric-only), so this widens the accepted input types as well. Verified
    # live on StarRocks 4.1.3 as a bare GROUP BY aggregate (no OVER clause is
    # needed, unlike the SQL-standard ordered-set form) over INT, DECIMAL and
    # DATE columns, and wrapped in a CAST.
    rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
    return f'percentile_cont({rendered}, 0.5)'


# Statistical mode aggregate (Alteryx Summarize Mode -> agg 'mode' ->
# sql_expression.get_agg_fn -> func.mode). `mode` is SQLAlchemy's own built-in
# GenericFunction (an ordered-set aggregate); attach a StarRocks compiler to it
# rather than override the class, so the Databend/default rendering (mode(col))
# stays byte-identical and the ordered-set within-group form is left intact.
@compiles(sa_mode, 'starrocks')
def compile_mode_starrocks(element, compiler, **kw):
    """Render Alteryx/Databend `mode(col)` as StarRocks approx_top_k.

    StarRocks has no `mode()` aggregate. The exact rewrite is a two-stage
    query -- count per (group, value) in a CTE, then pick the value with the
    highest count -- which cannot be expressed here: this hook fills a single
    aggregate slot in a SELECT list and has no access to the enclosing query's
    GROUP BY keys (they are assembled separately in
    sql_expression.get_select_query). The single-expression stand-in is
    approx_top_k, called with the documented maximum counter_num of 100000.
    Divergences to know about:

    * The winner is EXACT for any group with fewer than counter_num distinct
      values (StarRocks: "Expressions that have fewer than counter_num distinct
      items will yield exact item counts"), and only above that degrades to a
      Space-Saving estimate (count error up to ``2.0 * numRows / counter_num``),
      where Databend's mode() is always exact. Counters are allocated as distinct
      values are encountered, so a low-cardinality column pays nothing for the
      high ceiling.
    * Ties are broken by approx_top_k's internal counter order -- arbitrary but
      deterministic for a given input. Alteryx's Summarize->Mode returns the
      first-encountered value on a tie, and Databend's mode() is likewise
      unspecified, so no target agrees with any other here.

    NULL handling is made to match: approx_top_k counts NULL as its own item and
    will return it as the winner (verified live on StarRocks 4.1.3), where
    Databend/Alteryx ignore nulls. k=2 plus an ``IS NOT NULL`` array_filter fixes
    that -- NULL is at most one item, so the top two always contain the most
    frequent non-null value if the group has one. A group with no non-null value
    yields NULL, as an aggregate over no rows should.
    """
    clauses = list(element.clauses)
    if len(clauses) != 1:
        raise CompileError(
            'mode on StarRocks takes exactly one argument (the column whose most '
            f'frequent value to find); got {len(clauses)}. The ordered-set spelling '
            'mode() WITHIN GROUP (ORDER BY col) arrives with none — there is nothing '
            'to count — so write mode(col) instead.')
    value = compiler.process(clauses[0], **kw)
    # k=2 and counter_num rendered by hand rather than via func.* so literal_binds
    # -- used for view DDL -- reaches them instead of leaving bind placeholders.
    top_k = f'approx_top_k({value}, 2, 100000)'
    # approx_top_k returns ARRAY<STRUCT<item, count>> sorted by count descending.
    # StarRocks' analyzer rejects struct field access applied straight to an
    # aggregate's subscript ("approx_top_k(...)[1].item must appear in the GROUP
    # BY clause"), so project the items out with array_map first.
    items = f'array_map(sr_mode_e -> sr_mode_e.item, {top_k})'
    return f'array_filter({items}, sr_mode_i -> sr_mode_i IS NOT NULL)[1]'


class any_(GenericFunction):
    """Databend any() -- pick an arbitrary value from the group.

    Databend is the only target that spells the "arbitrary value from the group"
    aggregate `any(...)`; the bare default rendering is correct only there.
    Snowflake, Databricks and DuckDB all spell it `any_value(...)` and reject
    `any(...)` (Snowflake has no ANY aggregate at all; DuckDB raises a parser
    error). StarRocks likewise uses `any_value`. Each non-Databend target gets an
    explicit override so the default `any(...)` never reaches a warehouse that
    cannot run it. DuckDB is the isolation harness engine, so without its override
    Summarize First/Last cannot even be measured.
    """
    name = 'any'
    inherit_cache = True

@compiles(any_, 'starrocks')
@compiles(any_, 'snowflake')
@compiles(any_, 'databricks')
@compiles(any_, 'duckdb')
def compile_any_any_value(element, compiler, **kw):
    rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
    return f'any_value({rendered})'


class array_to_string(GenericFunction):
    """Databend array_to_string(array, sep); StarRocks spells it array_join."""
    name = 'array_to_string'
    inherit_cache = True

@compiles(array_to_string, 'starrocks')
def compile_array_to_string_starrocks(element, compiler, **kw):
    rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
    return f'array_join({rendered})'


class to_string(GenericFunction):
    name = 'to_string'
    inherit_cache = True

@compiles(to_string, 'starrocks')
def compile_to_string_starrocks(element, compiler, **kw):
    # StarRocks has no to_string(); CAST(... AS CHAR) is the MySQL-protocol
    # equivalent. Alteryx ToString(number, decimals) rounds to that many places.
    # Rendered by hand (not via func.round/func.cast) so literal_binds — used for
    # view DDL — reaches every argument; safe_round drops it on the digits arg.
    clauses = list(element.clauses)
    rendered = compiler.process(clauses[0], **kw)
    if len(clauses) >= 2:
        rendered = f"round({rendered}, {compiler.process(clauses[1], **kw)})"
    return f"CAST({rendered} AS CHAR)"

@compiles(to_string, 'snowflake')
def compile_to_string_snowflake(element, compiler, **kw):
    # Snowflake has no to_string(); TO_VARCHAR is the equivalent. Rendered by
    # hand for the same literal_binds/view-DDL reason as the StarRocks variant.
    clauses = list(element.clauses)
    rendered = compiler.process(clauses[0], **kw)
    if len(clauses) >= 2:
        rendered = f"round({rendered}, {compiler.process(clauses[1], **kw)})"
    return f"to_varchar({rendered})"


class try_to_float64(GenericFunction):
    name = 'try_to_float64'
    inherit_cache = True

@compiles(try_to_float64, 'starrocks')
def compile_try_to_float64_starrocks(element, compiler, **kw):
    # StarRocks CAST(... AS DOUBLE) yields NULL on unparseable text — the lenient
    # coercion Alteryx ToNumber (and Databend try_to_float64) provides. Rendered
    # explicitly because the StarRocks dialect drops a func.cast(..., Double) as a
    # perceived no-op.
    value = list(element.clauses)[0]
    return f"CAST({compiler.process(value, **kw)} AS DOUBLE)"

@compiles(try_to_float64, 'snowflake')
def compile_try_to_float64_snowflake(element, compiler, **kw):
    # TRY_TO_DOUBLE is the documented Snowflake equivalent: NULL instead of an
    # error when the string doesn't parse.
    value = list(element.clauses)[0]
    return f"try_to_double({compiler.process(value, **kw)})"


class regexp_instr(GenericFunction):
    """Databend regexp_instr(str, pat): 1-based offset of the first match, 0 when
    absent. Defined explicitly rather than generated by _register_rename because
    StarRocks needs a rewrite, not a rename (sc-30414), and Snowflake ships the
    same contract natively."""
    name = 'regexp_instr'
    inherit_cache = True


@compiles(regexp_instr, 'starrocks')
def compile_regexp_instr_starrocks(element, compiler, **kw):
    # sc-30414. StarRocks has no regexp_instr. This used to be a plain rename
    # to regexp(), which returns 1/0 rather than a position -- correct only
    # inside the Alteryx converter's `regexp_instr(col, pat) > 0` idiom, and
    # silently wrong for every other use (`regexp_instr(col, pat) = 3`, or the
    # position fed to substr()). Databend returns the 1-based offset of the
    # first match, 0 when absent, NULL when either argument is NULL.
    #
    # locate(regexp_extract(col, pat, 0), col) reproduces that offset: RE2 is
    # leftmost-match, so the extracted text cannot occur earlier in the string
    # than the match itself. The CASE is keyed on regexp()'s 1/0/NULL rather
    # than on a truthiness test so a NULL argument still yields NULL instead
    # of collapsing to 0.
    clauses = list(element.clauses)
    col, pattern = clauses[0], clauses[1]
    matched = func.regexp(col, pattern)
    return compiler.process(
        sqlalchemy.case(
            (matched == 1, func.locate(func.regexp_extract(col, pattern, 0), col)),
            (matched == 0, sqlalchemy.literal(0)),
            else_=sqlalchemy.null(),
        ),
        **kw
    )


class regexp_substr(GenericFunction):
    name = 'regexp_substr'
    inherit_cache = True

@compiles(regexp_substr, 'starrocks')
def compile_regexp_substr_starrocks(element, compiler, **kw):
    # StarRocks spells first-match extraction regexp_extract(str, pat, 0).
    #
    # sc-30414: regexp_extract returns an EMPTY STRING when nothing matches,
    # where Databend's regexp_substr returns NULL -- so `regexp_substr(x, p)
    # IS NULL` was false on StarRocks for every non-matching row, and a
    # coalesce() over it picked the '' instead of its fallback. nullif('')
    # restores Databend's contract; it is the same wrapper sql_numericize's
    # StarRocks variant already applies around its own regexp_extract calls.
    clauses = list(element.clauses)
    col, pattern = clauses[0], clauses[1]
    return compiler.process(func.nullif(func.regexp_extract(col, pattern, 0), ''), **kw)


#: Databend date_diff(unit, start, end) = end - start. StarRocks has no such
#: unit-parameterized diff but ships <unit>s_diff(a, b) = a - b. The converter
#: emits date_diff(unit, dt2, dt1) to get dt1 - dt2, so map to
#: <unit>s_diff(dt1, dt2). Only these base units appear (the converter composes
#: week/quarter from day/month before calling date_diff); an out-of-contract
#: unit falls through to the default rendering and fails loudly on StarRocks
#: rather than returning a silently mis-scaled count.
_STARROCKS_DATE_DIFF = {
    'second': 'seconds_diff', 'minute': 'minutes_diff', 'hour': 'hours_diff',
    'day': 'days_diff', 'month': 'months_diff', 'year': 'years_diff',
}

class date_diff(GenericFunction):
    name = 'date_diff'
    inherit_cache = True

@compiles(date_diff, 'starrocks')
def compile_date_diff_starrocks(element, compiler, **kw):
    clauses = list(element.clauses)
    if len(clauses) != 3:
        return compiler.visit_function(element)
    unit = str(clauses[0].value).strip().strip("'\"").lower()
    starrocks_fn = _STARROCKS_DATE_DIFF.get(unit)
    if starrocks_fn is None:
        return compiler.visit_function(element)
    # clauses are (unit, dt2, dt1); <unit>s_diff(dt1, dt2) = dt1 - dt2.
    return compiler.process(getattr(func, starrocks_fn)(clauses[2], clauses[1]), **kw)


#: Snowflake DATEDIFF(<part>, a, b) = b - a — the same direction as Databend's
#: date_diff(unit, a, b) — so the arguments pass through unswapped; only the
#: spelling changes and the unit renders as a constant keyword. An
#: out-of-contract unit falls through to the default rendering and fails
#: loudly on Snowflake (no date_diff function) rather than returning a
#: silently mis-scaled count.
_SNOWFLAKE_DATE_DIFF_UNITS = frozenset({
    'second', 'minute', 'hour', 'day', 'week', 'month', 'quarter', 'year',
})

@compiles(date_diff, 'snowflake')
def compile_date_diff_snowflake(element, compiler, **kw):
    clauses = list(element.clauses)
    if len(clauses) != 3:
        return compiler.visit_function(element)
    unit = str(clauses[0].value).strip().strip("'\"").lower()
    if unit not in _SNOWFLAKE_DATE_DIFF_UNITS:
        return compiler.visit_function(element)
    return f"datediff({unit}, {compiler.process(clauses[1], **kw)}, {compiler.process(clauses[2], **kw)})"


# ---------------------------------------------------------------------------
# Dialect-neutral spatial (geometry) functions
# ---------------------------------------------------------------------------
# The Alteryx converter/mapper and the wfr geo executors emit Databend ST_*
# names directly, none of which exist verbatim on StarRocks (MySQL-protocol).
# These custom GenericFunctions give each spatial op ONE dialect-neutral name
# with a per-dialect @compiles: the default/databend form renders the current
# Databend spelling byte-for-byte (so existing Databend SQL is unchanged) and
# the StarRocks form renders the verified StarRocks equivalent. Callers (the
# mapper, wave-2b) emit the neutral name and stop hardcoding a dialect.
#
# StarRocks spellings were verified live (paul-dev, StarRocks 3):
#   st_point, st_geometryfromtext, st_astext, st_contains, st_x, st_y all
#   execute; st_within does NOT exist (use st_contains with swapped args).
#
# Ops with no transparent StarRocks equivalent raise CompileError on StarRocks
# rather than emit a wrong/nonexistent function: the value must be produced by
# degrading to the shapely executor path (wave-2b) BEFORE reaching SQL. Failing
# loud at compile time is the signal that the emission site still needs that
# degradation, and it keeps the Databend path fully working in the meantime.

def _register_geom_fn(neutral_name, databend_name, starrocks_name=None,
                      *, swap_starrocks_args=False, starrocks_unsupported=None):
    func_cls = type(neutral_name, (GenericFunction,),
                    {'name': neutral_name, 'inherit_cache': True})

    @compiles(func_cls)
    def _compile_default(element, compiler, _name=databend_name, **kw):
        rendered = ', '.join(compiler.process(c, **kw) for c in element.clauses)
        return f"{_name}({rendered})"

    if starrocks_unsupported is not None:
        @compiles(func_cls, 'starrocks')
        def _compile_starrocks(element, compiler, _msg=starrocks_unsupported, **kw):
            raise CompileError(_msg)
        _STARROCKS_UNSUPPORTED[func_cls] = starrocks_unsupported
    else:
        @compiles(func_cls, 'starrocks')
        def _compile_starrocks(element, compiler, _name=starrocks_name,
                               _swap=swap_starrocks_args, **kw):
            clauses = list(element.clauses)
            if _swap:
                clauses = list(reversed(clauses))
            rendered = ', '.join(compiler.process(c, **kw) for c in clauses)
            return f"{_name}({rendered})"

    return func_cls


# Transparently translatable: databend spelling ↔ verified StarRocks spelling.
geom_from_wkt = _register_geom_fn('geom_from_wkt', 'st_geometryfromwkt', 'st_geometryfromtext')
geom_point = _register_geom_fn('geom_point', 'st_makegeompoint', 'st_point')
geom_as_wkt = _register_geom_fn('geom_as_wkt', 'st_aswkt', 'st_astext')
geom_contains = _register_geom_fn('geom_contains', 'st_contains', 'st_contains')
geom_x = _register_geom_fn('geom_x', 'st_x', 'st_x')
geom_y = _register_geom_fn('geom_y', 'st_y', 'st_y')
# within(a, b) = "a is within b" = b contains a; StarRocks has no st_within, so
# emit st_contains with the arguments swapped.
geom_within = _register_geom_fn('geom_within', 'st_within', 'st_contains', swap_starrocks_args=True)

# No transparent StarRocks equivalent — raise on StarRocks so wave-2b degrades
# the emission to the shapely executor (or, for createline, a python builder).
geom_area = _register_geom_fn(
    'geom_area', 'st_area',
    starrocks_unsupported='st_area has no StarRocks equivalent; degrade to the shapely area executor.')
geom_length = _register_geom_fn(
    'geom_length', 'st_length',
    starrocks_unsupported='st_length has no StarRocks equivalent; degrade to the shapely length executor.')
geom_intersects = _register_geom_fn(
    'geom_intersects', 'st_intersects',
    starrocks_unsupported='st_intersects has no StarRocks equivalent; degrade to the shapely intersects executor.')
geom_createline = _register_geom_fn(
    'geom_createline', 'st_createline',
    starrocks_unsupported='st_createline has no StarRocks equivalent; build the LINESTRING via st_linefromtext or the python executor.')
geom_centroid = _register_geom_fn(
    'geom_centroid', 'st_centroid',
    starrocks_unsupported='st_centroid has no StarRocks equivalent; degrade to the shapely centroid executor.')
# Distance is NOT a transparent rename: Databend st_distance is PLANAR over two
# geometries, while StarRocks only ships st_distance_sphere(lon0, lat0, lon1,
# lat1) — SPHERICAL and taking four scalars, not two geometries. The emission
# site (wave-2b) must supply the coordinate scalars and reconcile the degree↔
# meter unit factor; a blind @compiles here would silently change semantics.
geom_distance = _register_geom_fn(
    'geom_distance', 'st_distance',
    starrocks_unsupported='st_distance (planar, two geometries) has no transparent StarRocks equivalent; emit st_distance_sphere(lon0, lat0, lon1, lat1) — longitude first, per StarRocks ST_Distance_Sphere(x,y,...) — with unit reconciliation at the call site.')

# Bounding-rectangle edges (Alteryx SpatialInfo RectXY). Here the Databend
# spelling IS the registered name — there is nothing to translate TO, because
# StarRocks ships no envelope/bounding-box functions (st_xmin/st_xmax/st_ymin/
# st_ymax all fail with 'No matching function', verified live on StarRocks 3).
# The Alteryx mapper emits these names directly (alteryx_mapper._SPATIAL_INFO_FIELDS);
# registering them keeps the Databend rendering byte-identical while turning the
# StarRocks path into a clear compile-time CompileError instead of a cryptic
# warehouse error, matching the geom_area/geom_length fail-closed pattern.
st_xmin = _register_geom_fn(
    'st_xmin', 'st_xmin',
    starrocks_unsupported='st_xmin (bounding-rectangle edge) has no StarRocks equivalent; run this workflow on a Databend workspace.')
st_xmax = _register_geom_fn(
    'st_xmax', 'st_xmax',
    starrocks_unsupported='st_xmax (bounding-rectangle edge) has no StarRocks equivalent; run this workflow on a Databend workspace.')
st_ymin = _register_geom_fn(
    'st_ymin', 'st_ymin',
    starrocks_unsupported='st_ymin (bounding-rectangle edge) has no StarRocks equivalent; run this workflow on a Databend workspace.')
st_ymax = _register_geom_fn(
    'st_ymax', 'st_ymax',
    starrocks_unsupported='st_ymax (bounding-rectangle edge) has no StarRocks equivalent; run this workflow on a Databend workspace.')

# Alteryx converter fallback for CreatePolygon when the point args cannot be
# statically resolved to a portable POLYGON WKT string
# (alteryx_expression_converter._convert_special_function). StarRocks has st_polygon,
# but it takes a WKT string, not Databend's point-geometry args — no clean 1:1 — so
# fail closed rather than emit a mis-typed call. The converter already flags this
# fallback path low-confidence (it adds 'st_createpolygon' to `unmapped`).
st_makepolygon = _register_geom_fn(
    'st_makepolygon', 'st_makepolygon',
    starrocks_unsupported='st_makepolygon has no transparent StarRocks equivalent (st_polygon takes WKT text, not point geometries); build the POLYGON WKT explicitly or run on Databend.')


# ---------------------------------------------------------------------------
# Vector distance (sc-30364)
# ---------------------------------------------------------------------------
# ONE neutral expression that always returns a DISTANCE — lower is nearer,
# ORDER BY ASC, true (non-squared) magnitudes — so a caller can threshold and
# display the number, not just rank by it.
#
# Cosine and L2 only. Inner product is excluded by the epic: -inner_product(a, b)
# is not a distance on unnormalized vectors (self-distance is -||a||^2), so
# thresholds and displayed scores break even though ranking survives; for
# normalized embeddings cosine ranking is identical to inner product anyway.
#
# StarRocks is the only engine wired (epic 30343 D3, and `vector` compiles to
# ARRAY<FLOAT> on StarRocks alone), so the DEFAULT rendering refuses rather than
# emitting an unverified formula: a dispatch table of guessed spellings would be
# permanently green in Tier 1 and wrong on first contact with the warehouse.
#
# 🚨 l2 wraps l2_distance in sqrt(). StarRocks' l2_distance returns SQUARED
# Euclidean distance under a name that says distance — verified live on
# StarRocks 4.1.3 (sc-30350): l2_distance([1,2,3], [4,5,6]) = 27, not 5.196152 —
# while Databend's same-named function returns the true distance. Ranking is
# unaffected either way (squaring is monotonic on non-negatives), but every
# THRESHOLD and every displayed number is wrong by a square, which is precisely
# the defect the epic used to exclude inner product. sqrt() costs one scalar op
# per row after the O(dimensions) inner loop that already dominates (sc-30350
# measured l2_distance itself at ~21x cosine's compute: 1,781 ms vs 83 ms at
# 1M rows x 768 dims), so the correction is not where the money goes; the
# un-vectorized l2_distance is, and that is sc-30475's.
#
# 🚨 NULL is deliberately NOT masked. Large-magnitude vectors overflow
# StarRocks' float32 accumulator and both metrics then return NULL silently
# (sc-30350, measured) — there is no way to prevent that from outside the vendor
# function, and COALESCE-ing it to a sentinel distance would convert a visible
# NULL into an invisible wrong answer. The mis-ranking materializes in the
# ORDER BY (MySQL/StarRocks sorts NULL FIRST, so an overflowed row would top a
# nearest-neighbour result), so the guard belongs to the search step that owns
# the ORDER BY (sc-30370), which must exclude or explicitly rank NULL distances
# and is where normalization at write time is enforced.
VECTOR_DISTANCE_METRICS = ('cosine', 'l2')


class vector_distance(GenericFunction):
    """vector_distance(metric, a, b) -> distance, lower is nearer.

    `metric` is a literal from VECTOR_DISTANCE_METRICS and leads the arguments,
    matching safe_extract(field, ...) elsewhere in this module.

    🚨 The cosine range is about [-1.2e-7, 2], NOT [0, 2], and an exact match's
    distance is NEGATIVE (measured live on StarRocks 4.1.3: cosine_similarity(v, v)
    = 1.0000001 for a 768-dim v). Never filter on `distance >= 0` or
    `BETWEEN 0 AND 2` -- that drops exact matches, which are the top hit. NULL also
    propagates by design on float32 overflow; see compile_vector_distance_starrocks.
    """
    type = Double()
    name = 'vector_distance'
    inherit_cache = True


@compiles(vector_distance)
def compile_vector_distance(element, compiler, **kw):
    raise CompileError(
        f'vector_distance has no verified {compiler.dialect.name!r} rendering; the vector '
        'dtype is StarRocks-only (epic 30343 D3), and PlaidVector refuses every other dialect'
    )


@compiles(vector_distance, 'starrocks')
def compile_vector_distance_starrocks(element, compiler, **kw):
    metric, left, right = list(element.clauses)
    metric = metric.effective_value
    if metric == 'cosine':
        # 🚨 self_group() is load-bearing, not tidiness. Without it SQLAlchemy sees the
        # outer element as a Function (maximum precedence) and declines to parenthesize,
        # so `1 - cosine_similarity(a, b)` mis-associates inside ANY enclosing
        # arithmetic -- measured live on 4.1.3: `vd * 2` renders
        # `1 - cosine_similarity(a, b) * 2` = -0.94926 where 0.050736 is correct, and the
        # `1 - vd` similarity round trip comes back -0.97463, sign flipped. The l2 branch
        # is immune, being a bare function call. sc-30370 owns the ORDER BY, blended and
        # weighted distances and the similarity display, so it is the first caller to
        # compose this into arithmetic.
        #
        # 🚨 Range: NOT [0, 2], and self-distance is NOT 0. StarRocks' cosine_similarity
        # accumulates in float32 and overshoots 1 on identical vectors -- measured live on
        # 4.1.3: cosine_similarity(v, v) = 1.0000001 for a 768-dim v (and 1 - 5.96e-8 for
        # [1,2,3]), so this distance is about -1.1920929e-7 for an exact match. The real
        # range is roughly [-1.2e-7, 2]. Do NOT add a `distance >= 0` or
        # `BETWEEN 0 AND 2` sanity filter downstream: it would silently drop exact
        # matches, which are the top hit.
        return compiler.process((1 - func.cosine_similarity(left, right)).self_group(), **kw)
    if metric == 'l2':
        return compiler.process(func.sqrt(func.l2_distance(left, right)), **kw)
    raise CompileError(
        f'vector_distance metric {metric!r} is not one of {VECTOR_DISTANCE_METRICS}; '
        'inner product is excluded because it is not a distance on unnormalized vectors'
    )

# ---------------------------------------------------------------------------
# Snowflake: defaults confirmed valid (sc-23158 WS-B2)
# ---------------------------------------------------------------------------
#: Function classes whose DEFAULT @compiles rendering is already valid
#: Snowflake SQL — verified against the Snowflake function reference — so no
#: 'snowflake' variant is registered. The plaid parity harness
#: (plaid/tests/parity/test_expression_compile.py) keys its known-gap skips on
#: variant *absence*; membership here is the explicit per-function
#: confirmation its docstring anticipates.
#:
#:   safe_extract              EXTRACT(<part> FROM <expr>); year/month/day/
#:                             week/dow/epoch(_second) are documented parts
#:   safe_ltrim/rtrim/trim     LTRIM/RTRIM/TRIM(<expr> [, <characters>])
#:                             match the default's optional-chars rendering
#:   regexp_substr             REGEXP_SUBSTR(subject, pattern) — first whole
#:                             match, NULL when none (Databend contract)
#:   regexp_instr              REGEXP_INSTR(subject, pattern) — 1-based
#:                             position, 0 on no match (Databend contract)
#:   import_col                delegates to import_cast (which has a variant);
#:                             its own 3-arg regexp_replace whitespace probe is
#:                             valid Snowflake
_SNOWFLAKE_DEFAULT_OK = frozenset({
    safe_extract,
    safe_ltrim,
    safe_rtrim,
    safe_trim,
    regexp_substr,
    globals()['regexp_instr'],  # generated by the rename registry above
    import_col,
})


# ---------------------------------------------------------------------------
# StarRocks: defaults confirmed valid (sc-30376 — the compile-surface sweep)
# ---------------------------------------------------------------------------
#: Function classes whose DEFAULT @compiles rendering is already valid
#: StarRocks SQL — each confirmed live against a real StarRocks warehouse
#: (paul-dev), not read off documentation alone — so no 'starrocks' variant is
#: registered. The generated parity sweep
#: (plaid/tests/parity/test_expression_compile.py) keys its known-gap skips on
#: variant *absence*; membership here is the explicit per-function
#: confirmation, matching the _SNOWFLAKE_DEFAULT_OK convention above.
#:
#:   avg                    bare avg(...) — plain mean, a native StarRocks
#:                         aggregate with no dialect quirk to verify.
#:   variance               bare variance(...) — StarRocks' own alias for
#:                         VAR_POP (verified live: variance(x) = var_pop(x) =
#:                         1.25 for [1,2,3,4], the correct population
#:                         variance). Databend has NO bare `variance`
#:                         function at all — verified live, Databend raises
#:                         "no function matches the given name: 'variance',
#:                         do you mean 'variance_pop', 'variance_samp'?" — so
#:                         there is no cross-engine value to disagree with:
#:                         this class is already a hard error on Databend
#:                         today, pre-existing and out of this story's scope
#:                         (not "different value", a query that never runs).
#:   import_col              delegates to import_cast (which has a variant);
#:                         its own whitespace probe (2-arg regexp_replace) is
#:                         valid StarRocks
#:   safe_round              CAST(x, Numeric(38, 10)) then round(number[,
#:                         digits]) — the explicit (38, 10) precision/scale
#:                         sidesteps the bare-CAST-rounds-to-scale-0 trap
#:                         sql_metric_multiply and sql_integerize_round hit
#:                         below, so ROUND(number, digits) sees the real
#:                         fractional value. Verified live: ROUND(CAST(
#:                         '1.567' AS DECIMAL(38, 10)), 2) = 1.57.
#:   safe_upper/safe_lower  bare upper(x)/lower(x) — standard, 1-arg only in
#:                         practice (no caller passes the optional extra args)
#:   sql_set_null           plain CASE/WHEN, no dialect-specific SQL at all
#:   sql_slice_string       SUBSTRING/LEFT/RIGHT — all present and standard
#:   sql_zfill              GREATEST/LENGTH/LPAD — all present and standard
#:   sql_integerize_round    the same bare-Numeric-cast-rounds-to-scale-0
#:                         pattern sql_metric_multiply is unsupported for,
#:                         but safe here because nothing MULTIPLIES between
#:                         the two casts — scale-0 rounding on the way to an
#:                         Integer cast IS the intended "round to nearest
#:                         integer" behavior. The two engines' PRECISION
#:                         ceilings differ (bare CAST(x AS DECIMAL) verified
#:                         live: ~19 digits on StarRocks before returning
#:                         NULL; Databend's bare CAST(x AS NUMERIC) is
#:                         DECIMAL(18, 3), and its OWN CAST(..., Integer) step
#:                         maps to Int32 and hard-errors above ~10 digits —
#:                         verified live, "decimal cast to int overflow...
#:                         to_int32(...)"), but Databend's ceiling is always
#:                         the lower/first one hit: every value that succeeds
#:                         on Databend (< ~2.1e9) is well inside StarRocks'
#:                         ~19-digit headroom too. Rounding mode matches
#:                         (verified live, both engines: 1.5->2, 2.5->3,
#:                         -1.5->-2, round-half-away-from-zero). Pre-existing
#:                         Databend Int32 ceiling, not a StarRocks regression
#:                         — out of this story's scope, same as the sibling
#:                         sql_integerize_truncate gap filed separately.
_STARROCKS_DEFAULT_OK = frozenset({
    avg,
    variance,
    import_col,
    safe_round,
    safe_upper,
    safe_lower,
    sql_set_null,
    sql_slice_string,
    sql_zfill,
    sql_integerize_round,
})


# ── StarRocks string CAST ────────────────────────────────────────────────────
# StarRocks inherits MySQL's CAST rules, which render every string type as CHAR
# in a CAST (MySQL CAST forbids VARCHAR/TEXT). But StarRocks CHAR maxes at 255,
# so a String(4000)/Text cast emits CHAR(4000) — accepted only in a bare SELECT
# via coercion, and rejected by the strict type check of INSERT ... UNION into a
# STRING/varchar column ("input cols type not equal with output cols type"). The
# physical string columns are STRING (varchar-max), so render a string CAST as
# STRING to match. Non-string casts are untouched.
def _cast_targets_string(cast_type):
    # Rewrite only the string casts StarRocks would otherwise render as an
    # *invalid* CHAR: unbounded, or length > 255 (StarRocks CHAR maxes at 255).
    # Any CHAR is left alone — a fixed CHAR, GUIDHyphens' CHAR uuid impl, or a
    # short PlaidUnicode dtype like `s8`/`cidr` — since a bounded CHAR(n≤255) is
    # valid StarRocks and enforces its width; the CHAR exclusion is by isinstance,
    # independent of length. An Enum keeps its own rendering.
    t = cast_type
    while isinstance(t, sqlalchemy.sql.sqltypes.TypeDecorator):
        t = t.impl
    if (not isinstance(t, sqlalchemy.sql.sqltypes.String)
            or isinstance(t, (sqlalchemy.sql.sqltypes.CHAR, sqlalchemy.sql.sqltypes.Enum))):
        return False
    length = getattr(t, 'length', None)
    return length is None or length > 255


@compiles(sqlalchemy.sql.elements.Cast, 'starrocks')
def compile_cast_starrocks(element, compiler, **kw):
    if _cast_targets_string(element.type):
        return f'CAST({compiler.process(element.clause, **kw)} AS STRING)'
    return compiler.visit_cast(element, **kw)
