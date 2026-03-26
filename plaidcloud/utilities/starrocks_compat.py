#!/usr/bin/env python
# coding=utf-8
"""
StarRocks compatibility layer for Databend/PostgreSQL SQL functions.

Defines SQLAlchemy GenericFunction subclasses with @compiles decorators
that translate Databend/PostgreSQL function calls into StarRocks-compatible
SQL.  The default compilation (used by PostgreSQL, Databend, Greenplum, etc.)
emits the original function name unchanged so existing UDFs continue to work.

Import this module early so that the @compiles decorators are registered
before any queries are compiled against a StarRocks engine.

Dialect target
--------------
All @compiles decorators target ``'starrocks'`` (the dialect name returned by
``engine.dialect.name`` when using the ``starrocks://`` connection string).
"""

import logging
import re

from sqlalchemy import func
from sqlalchemy.exc import CompileError
from sqlalchemy.sql import functions
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.types import (
    BigInteger, Boolean, DateTime, Date, Integer, Numeric, String,
)

_log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

# PostgreSQL → MySQL/StarRocks date-format token mapping.
# Sorted by descending length at lookup time so that longer tokens
# (e.g. ``HH24``) are matched before shorter ones (``HH``).
_PG_TO_MYSQL_FORMAT = {
    'YYYY': '%Y', 'YY': '%y',
    'MM': '%m', 'DD': '%d',
    'HH24': '%H', 'HH12': '%h', 'HH': '%h',
    'MI': '%i', 'SS': '%s',
    'MS': '%f', 'US': '%f',
    'AM': '%p', 'PM': '%p', 'am': '%p', 'pm': '%p',
    'Month': '%M', 'Mon': '%b',
    'Day': '%W', 'Dy': '%a',
    'D': '%w',
    'TZ': '', 'J': '', 'Q': '',
}

_PG_SORTED = sorted(_PG_TO_MYSQL_FORMAT.items(), key=lambda kv: -len(kv[0]))


def _pg_to_mysql_fmt(pg_fmt: str) -> str:
    """Best-effort translation of a PostgreSQL date-format string to MySQL."""
    result = pg_fmt
    for pg_tok, my_tok in _PG_SORTED:
        result = result.replace(pg_tok, my_tok)
    return result


def _args(element, compiler, **kw):
    """Compile all clause-list arguments into a list of SQL strings."""
    return [compiler.process(c, **kw) for c in element.clauses]


def _bool_case(v: str, *, extras_true: str = '', extras_false: str = '') -> str:
    """Build a CASE expression that maps string values to TRUE/FALSE/NULL.

    *v* is the compiled SQL expression for the input value.
    *extras_true*/*extras_false* are additional ``WHEN … THEN`` clauses
    injected after the standard set (e.g. ``" WHEN 'on' THEN TRUE"``).
    """
    return (
        f"CASE lower(trim({v}))"
        f" WHEN 't' THEN TRUE WHEN 'true' THEN TRUE"
        f" WHEN 'y' THEN TRUE WHEN 'yes' THEN TRUE"
        f" WHEN '1' THEN TRUE"
        f"{extras_true}"
        f" WHEN 'f' THEN FALSE WHEN 'false' THEN FALSE"
        f" WHEN 'n' THEN FALSE WHEN 'no' THEN FALSE"
        f" WHEN '0' THEN FALSE"
        f"{extras_false}"
        f" ELSE NULL END"
    )


# ===================================================================
# Conversions
# ===================================================================

class to_char(functions.GenericFunction):
    type = String()
    name = 'to_char'
    inherit_cache = True


@compiles(to_char, 'starrocks')
def _sr_to_char(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"date_format({a[0]}, '{translated}')"
        return f"date_format({a[0]}, {fmt})"
    return f"CAST({a[0]} AS CHAR)"


class to_date(functions.GenericFunction):
    type = Date()
    name = 'to_date'
    inherit_cache = True


@compiles(to_date, 'starrocks')
def _sr_to_date(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"CAST(str_to_date({a[0]}, '{translated}') AS DATE)"
        return f"CAST(str_to_date({a[0]}, {fmt}) AS DATE)"
    # Single-arg: StarRocks to_date(datetime) returns date
    return f"to_date({a[0]})"


class to_number(functions.GenericFunction):
    type = Numeric()
    name = 'to_number'
    inherit_cache = True


@compiles(to_number, 'starrocks')
def _sr_to_number(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS DOUBLE)"


class to_timestamp(functions.GenericFunction):
    type = DateTime()
    name = 'to_timestamp'
    inherit_cache = True


@compiles(to_timestamp, 'starrocks')
def _sr_to_timestamp(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"str_to_date({a[0]}, '{translated}')"
        return f"str_to_date({a[0]}, {fmt})"
    # Single-arg: treat as unix timestamp
    return f"from_unixtime({a[0]})"


class unix_to_timestamp(functions.GenericFunction):
    type = DateTime()
    name = 'unix_to_timestamp'
    inherit_cache = True


@compiles(unix_to_timestamp, 'starrocks')
def _sr_unix_to_timestamp(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"from_unixtime({a[0]})"


# ===================================================================
# Date / time
# ===================================================================

class age(functions.GenericFunction):
    """Returns interval between timestamps.  StarRocks approximation
    returns the difference in *seconds* as a BIGINT."""
    type = Numeric()
    name = 'age'
    inherit_cache = True


@compiles(age, 'starrocks')
def _sr_age(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"timestampdiff(SECOND, {a[1]}, {a[0]})"
    return f"timestampdiff(SECOND, {a[0]}, now())"


class clock_timestamp(functions.GenericFunction):
    type = DateTime()
    name = 'clock_timestamp'
    inherit_cache = True


@compiles(clock_timestamp, 'starrocks')
def _sr_clock_timestamp(element, compiler, **kw):
    return "now()"


class date_part(functions.GenericFunction):
    type = Numeric()
    name = 'date_part'
    inherit_cache = True


@compiles(date_part, 'starrocks')
def _sr_date_part(element, compiler, **kw):
    clauses = list(element.clauses)
    # The field name (e.g. 'year') must be emitted as a bare SQL keyword
    # in EXTRACT(), not as a bind parameter.
    field_clause = clauses[0]
    if hasattr(field_clause, 'value'):
        field = field_clause.value          # BindParameter
    elif hasattr(field_clause, 'text'):
        field = field_clause.text           # TextClause
    else:
        field = str(compiler.process(field_clause, **kw)).strip("'")
    ts = compiler.process(clauses[1], **kw)
    return f"extract({field} FROM {ts})"


class isfinite(functions.GenericFunction):
    """StarRocks has no concept of infinite timestamps/dates;
    a non-NULL value is always finite."""
    type = Boolean()
    name = 'isfinite'
    inherit_cache = True


@compiles(isfinite, 'starrocks')
def _sr_isfinite(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"({a[0]} IS NOT NULL)"


class justify_days(functions.GenericFunction):
    name = 'justify_days'
    inherit_cache = True


@compiles(justify_days, 'starrocks')
def _sr_justify_days(element, compiler, **kw):
    # No interval justification in StarRocks; pass through
    return _args(element, compiler, **kw)[0]


class justify_hours(functions.GenericFunction):
    name = 'justify_hours'
    inherit_cache = True


@compiles(justify_hours, 'starrocks')
def _sr_justify_hours(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]


class justify_interval(functions.GenericFunction):
    name = 'justify_interval'
    inherit_cache = True


@compiles(justify_interval, 'starrocks')
def _sr_justify_interval(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]


class statement_timestamp(functions.GenericFunction):
    type = DateTime()
    name = 'statement_timestamp'
    inherit_cache = True


@compiles(statement_timestamp, 'starrocks')
def _sr_statement_timestamp(element, compiler, **kw):
    return "now()"


class timeofday(functions.GenericFunction):
    type = String()
    name = 'timeofday'
    inherit_cache = True


@compiles(timeofday, 'starrocks')
def _sr_timeofday(element, compiler, **kw):
    return "CAST(now() AS CHAR)"


class transaction_timestamp(functions.GenericFunction):
    type = DateTime()
    name = 'transaction_timestamp'
    inherit_cache = True


@compiles(transaction_timestamp, 'starrocks')
def _sr_transaction_timestamp(element, compiler, **kw):
    return "now()"


# ===================================================================
# Math
# ===================================================================

class cbrt(functions.GenericFunction):
    type = Numeric()
    name = 'cbrt'
    inherit_cache = True


@compiles(cbrt, 'starrocks')
def _sr_cbrt(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"power({a[0]}, 1.0 / 3.0)"


class log(functions.GenericFunction):
    """PostgreSQL ``log(x)`` is log-base-10; ``log(b, x)`` is log-base-b.
    StarRocks uses ``log10(x)`` / ``log(b, x)``."""
    type = Numeric()
    name = 'log'
    inherit_cache = True


@compiles(log, 'starrocks')
def _sr_log(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) == 1:
        return f"log10({a[0]})"
    return f"log({a[0]}, {a[1]})"


# SQLAlchemy already registers a ``random`` GenericFunction in some builds.
# Rather than re-registering (which emits a warning), attach the compiler
# override to whatever class ``func.random()`` actually resolves to.
_sa_random = type(func.random())


@compiles(_sa_random, 'starrocks')
def _sr_random(element, compiler, **kw):
    return "rand()"


class safe_divide(functions.GenericFunction):
    type = Numeric()
    name = 'safe_divide'
    inherit_cache = True


@compiles(safe_divide, 'starrocks')
def _sr_safe_divide(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    default = a[2] if len(a) >= 3 else 'NULL'
    return f"IF({a[1]} = 0, {default}, {a[0]} / {a[1]})"


class setseed(functions.GenericFunction):
    """StarRocks ``rand()`` does not support seeding; this is a no-op."""
    type = Numeric()
    name = 'setseed'
    inherit_cache = True


@compiles(setseed, 'starrocks')
def _sr_setseed(element, compiler, **kw):
    return "0"


class trunc(functions.GenericFunction):
    type = Numeric()
    name = 'trunc'
    inherit_cache = True


@compiles(trunc, 'starrocks')
def _sr_trunc(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    places = a[1] if len(a) >= 2 else '0'
    return f"truncate({a[0]}, {places})"


class width_bucket(functions.GenericFunction):
    type = Integer()
    name = 'width_bucket'
    inherit_cache = True


@compiles(width_bucket, 'starrocks')
def _sr_width_bucket(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    op, lo, hi, cnt = a[0], a[1], a[2], a[3]
    return (
        f"CASE"
        f" WHEN {op} < {lo} THEN 0"
        f" WHEN {op} >= {hi} THEN {cnt} + 1"
        f" ELSE CAST(floor(({op} - {lo}) * {cnt} / ({hi} - {lo})) AS INT) + 1"
        f" END"
    )


# ===================================================================
# Text – built-in equivalents
# ===================================================================

class btrim(functions.GenericFunction):
    """``btrim(s, chars)`` – trim *chars* from both sides.

    Note: PostgreSQL trims any *character* in the set; StarRocks
    ``TRIM(BOTH x FROM s)`` removes the exact *string*.  Behaviour
    matches when *chars* is a single character (the common case).
    """
    type = String()
    name = 'btrim'
    inherit_cache = True


@compiles(btrim, 'starrocks')
def _sr_btrim(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(BOTH {a[1]} FROM {a[0]})"
    return f"TRIM({a[0]})"


class chr(functions.GenericFunction):
    type = String()
    name = 'chr'
    inherit_cache = True


@compiles(chr, 'starrocks')
def _sr_chr(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"char({a[0]})"


class ltrim(functions.GenericFunction):
    """Two-arg form: ``ltrim(s, chars)`` → ``TRIM(LEADING chars FROM s)``.

    Same caveat as *btrim*: StarRocks trims a string, not a character set.
    """
    type = String()
    name = 'ltrim'
    inherit_cache = True


@compiles(ltrim, 'starrocks')
def _sr_ltrim(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(LEADING {a[1]} FROM {a[0]})"
    return f"ltrim({a[0]})"


class rtrim(functions.GenericFunction):
    type = String()
    name = 'rtrim'
    inherit_cache = True


@compiles(rtrim, 'starrocks')
def _sr_rtrim(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(TRAILING {a[1]} FROM {a[0]})"
    return f"rtrim({a[0]})"


class strpos(functions.GenericFunction):
    """``strpos(string, sub)`` → ``locate(sub, string)`` (reversed args)."""
    type = Integer()
    name = 'strpos'
    inherit_cache = True


@compiles(strpos, 'starrocks')
def _sr_strpos(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"locate({a[1]}, {a[0]})"


class to_ascii(functions.GenericFunction):
    """No direct StarRocks equivalent; returns the value unchanged."""
    type = String()
    name = 'to_ascii'
    inherit_cache = True


@compiles(to_ascii, 'starrocks')
def _sr_to_ascii(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]


class to_hex(functions.GenericFunction):
    """``to_hex(n)`` → ``lower(hex(n))``."""
    type = String()
    name = 'to_hex'
    inherit_cache = True


@compiles(to_hex, 'starrocks')
def _sr_to_hex(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"lower(hex({a[0]}))"


class translate(functions.GenericFunction):
    """PostgreSQL ``translate(s, from, to)`` does character-by-character
    replacement.  StarRocks has no equivalent.

    This wrapper emits ``translate(...)`` as-is which *will* fail at
    runtime.  Install a StarRocks UDF or rewrite the query to use
    chained ``replace()`` calls for the specific characters needed.
    """
    type = String()
    name = 'translate'
    inherit_cache = True

# No @compiles override – falls through to default ``translate(...)``


class quote_literal(functions.GenericFunction):
    """Wrap a value in SQL single-quotes, escaping internal quotes."""
    type = String()
    name = 'quote_literal'
    inherit_cache = True


@compiles(quote_literal, 'starrocks')
def _sr_quote_literal(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    # Use char(39) = single-quote to avoid quoting ambiguity
    q = "char(39)"
    return (
        f"concat({q}, replace({a[0]}, {q}, concat({q}, {q})), {q})"
    )


# ===================================================================
# Text – custom PlaidCloud functions
# ===================================================================

class numericize(functions.GenericFunction):
    """Strip non-numeric characters and cast to DOUBLE."""
    type = Numeric()
    name = 'numericize'
    inherit_cache = True


@compiles(numericize, 'starrocks')
def _sr_numericize(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return (
        f"CAST(NULLIF(regexp_replace({a[0]}, '[^0-9.eE+-]', ''), '') AS DOUBLE)"
    )


class integerize_round(functions.GenericFunction):
    """Parse string → round to nearest integer → BIGINT."""
    type = BigInteger()
    name = 'integerize_round'
    inherit_cache = True


@compiles(integerize_round, 'starrocks')
def _sr_integerize_round(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(round(CAST({a[0]} AS DOUBLE)) AS BIGINT)"


class integerize_truncate(functions.GenericFunction):
    """Parse string → truncate toward zero → BIGINT."""
    type = BigInteger()
    name = 'integerize_truncate'
    inherit_cache = True


@compiles(integerize_truncate, 'starrocks')
def _sr_integerize_truncate(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(truncate(CAST({a[0]} AS DOUBLE), 0) AS BIGINT)"


class normalize_whitespace(functions.GenericFunction):
    """Collapse runs of whitespace (including newlines, tabs) to a
    single space."""
    type = String()
    name = 'normalize_whitespace'
    inherit_cache = True


@compiles(normalize_whitespace, 'starrocks')
def _sr_normalize_whitespace(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"regexp_replace({a[0]}, '\\\\s+', ' ')"


class zfill(functions.GenericFunction):
    """Pad *string* on the left with zeroes to *length*."""
    type = String()
    name = 'zfill'
    inherit_cache = True


@compiles(zfill, 'starrocks')
def _sr_zfill(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"lpad({a[0]}, {a[1]}, '0')"


class slice_string(functions.GenericFunction):
    """Python-style 0-indexed slicing: ``slice_string(s, start, stop)``
    → ``substring(s, start+1, stop-start)``."""
    type = String()
    name = 'slice_string'
    inherit_cache = True


@compiles(slice_string, 'starrocks')
def _sr_slice_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"substring({a[0]}, {a[1]} + 1, {a[2]} - {a[1]})"


class metric_multiply(functions.GenericFunction):
    """Interpret a string with a metric suffix (K, M, G, etc.) and
    return the numeric value multiplied by the appropriate factor."""
    type = Numeric()
    name = 'metric_multiply'
    inherit_cache = True


@compiles(metric_multiply, 'starrocks')
def _sr_metric_multiply(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    v = a[0]
    num = f"CAST(regexp_replace({v}, '[^0-9.eE+-]', '') AS DOUBLE)"
    return (
        f"CASE"
        f" WHEN upper({v}) REGEXP '.*T$' THEN {num} * 1e12"
        f" WHEN upper({v}) REGEXP '.*G$' THEN {num} * 1e9"
        f" WHEN upper({v}) REGEXP '.*M$' THEN {num} * 1e6"
        f" WHEN upper({v}) REGEXP '.*K$' THEN {num} * 1e3"
        f" ELSE {num}"
        f" END"
    )


# ===================================================================
# Text type-conversion UDFs
#
# The PostgreSQL versions (plpgsql) strip spaces, attempt a cast,
# and return NULL on failure.  StarRocks CAST from invalid strings
# returns NULL (unlike PostgreSQL which throws), so a REGEXP guard
# followed by a CAST is the closest equivalent.
# ===================================================================

class text_to_integer(functions.GenericFunction):
    type = Integer()
    name = 'text_to_integer'
    inherit_cache = True


@compiles(text_to_integer, 'starrocks')
def _sr_text_to_integer(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    cleaned = f"NULLIF(replace({a[0]}, ' ', ''), '')"
    return (
        f"CASE WHEN {cleaned} REGEXP '^-?[0-9]+$'"
        f" THEN CAST({cleaned} AS INT)"
        f" ELSE NULL END"
    )


class text_to_bigint(functions.GenericFunction):
    type = BigInteger()
    name = 'text_to_bigint'
    inherit_cache = True


@compiles(text_to_bigint, 'starrocks')
def _sr_text_to_bigint(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    cleaned = f"NULLIF(replace({a[0]}, ' ', ''), '')"
    return (
        f"CASE WHEN {cleaned} REGEXP '^-?[0-9]+$'"
        f" THEN CAST({cleaned} AS BIGINT)"
        f" ELSE NULL END"
    )


class text_to_smallint(functions.GenericFunction):
    type = Integer()
    name = 'text_to_smallint'
    inherit_cache = True


@compiles(text_to_smallint, 'starrocks')
def _sr_text_to_smallint(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    cleaned = f"NULLIF(replace({a[0]}, ' ', ''), '')"
    return (
        f"CASE WHEN {cleaned} REGEXP '^-?[0-9]+$'"
        f" THEN CAST({cleaned} AS SMALLINT)"
        f" ELSE NULL END"
    )


class text_to_numeric(functions.GenericFunction):
    type = Numeric()
    name = 'text_to_numeric'
    inherit_cache = True


@compiles(text_to_numeric, 'starrocks')
def _sr_text_to_numeric(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    cleaned = f"NULLIF(replace({a[0]}, ' ', ''), '')"
    return (
        f"CASE WHEN {cleaned} REGEXP '^-?[0-9]*\\\\.?[0-9]+([eE][+-]?[0-9]+)?$'"
        f" THEN CAST({cleaned} AS DOUBLE)"
        f" ELSE NULL END"
    )


class text_to_bool(functions.GenericFunction):
    type = Boolean()
    name = 'text_to_bool'
    inherit_cache = True


@compiles(text_to_bool, 'starrocks')
def _sr_text_to_bool(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return _bool_case(a[0])


# ===================================================================
# Aggregate functions
# ===================================================================

class first(functions.GenericFunction):
    """Custom aggregate.  StarRocks ``any_value()`` is the closest
    approximation (returns an arbitrary non-NULL value from the group)."""
    name = 'first'
    inherit_cache = True


@compiles(first, 'starrocks')
def _sr_first(element, compiler, **kw):
    _log.warning(
        "first() compiled as any_value() on StarRocks — returns an "
        "arbitrary value, not necessarily the first by insertion or "
        "sort order.  Use first_value() with an explicit window ORDER BY "
        "if deterministic ordering is required."
    )
    a = _args(element, compiler, **kw)
    return f"any_value({a[0]})"


class last(functions.GenericFunction):
    """See *first* – same limitation applies."""
    name = 'last'
    inherit_cache = True


@compiles(last, 'starrocks')
def _sr_last(element, compiler, **kw):
    _log.warning(
        "last() compiled as any_value() on StarRocks — returns an "
        "arbitrary value, not necessarily the last by insertion or "
        "sort order.  Use last_value() with an explicit window ORDER BY "
        "if deterministic ordering is required."
    )
    a = _args(element, compiler, **kw)
    return f"any_value({a[0]})"


class median(functions.GenericFunction):
    type = Numeric()
    name = 'median'
    inherit_cache = True


@compiles(median, 'starrocks')
def _sr_median(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[0]}, 0.5)"


class stdev(functions.GenericFunction):
    """``stdev(x)`` → ``stddev(x)`` (StarRocks spelling)."""
    type = Numeric()
    name = 'stdev'
    inherit_cache = True


@compiles(stdev, 'starrocks')
def _sr_stdev(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"stddev({a[0]})"


# ===================================================================
# UUID
# ===================================================================

class gen_random_uuid(functions.GenericFunction):
    type = String()
    name = 'gen_random_uuid'
    inherit_cache = True


@compiles(gen_random_uuid, 'starrocks')
def _sr_gen_random_uuid(element, compiler, **kw):
    return "uuid()"


# ===================================================================
# Arrays
# ===================================================================

class string_to_array(functions.GenericFunction):
    """``string_to_array(text, delim)`` → ``split(text, delim)``."""
    name = 'string_to_array'
    inherit_cache = True


@compiles(string_to_array, 'starrocks')
def _sr_string_to_array(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"split({a[0]}, {a[1]})"


class array_to_json(functions.GenericFunction):
    type = String()
    name = 'array_to_json'
    inherit_cache = True


@compiles(array_to_json, 'starrocks')
def _sr_array_to_json(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"to_json({a[0]})"


# ===================================================================
# JSON
# ===================================================================

class json_extract_path(functions.GenericFunction):
    """``json_extract_path(json, k1, k2, …)``
    → ``json_query(json, '$.k1.k2.…')``."""
    type = String()
    name = 'json_extract_path'
    inherit_cache = True


@compiles(json_extract_path, 'starrocks')
def _sr_json_extract_path(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) > 1:
        parts = []
        for k in a[1:]:
            if k.startswith("'") and k.endswith("'"):
                parts.append(k[1:-1])
            else:
                # Dynamic key – fall back to concat-based path building.
                # This produces valid SQL but is harder to read.
                parts.append("' || " + k + " || '")
        path = '.'.join(parts)
        return f"json_query({a[0]}, '$.{path}')"
    return f"json_query({a[0]}, '$')"


class json_extract_path_text(functions.GenericFunction):
    """Like *json_extract_path* but returns a plain string."""
    type = String()
    name = 'json_extract_path_text'
    inherit_cache = True


@compiles(json_extract_path_text, 'starrocks')
def _sr_json_extract_path_text(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) > 1:
        parts = []
        for k in a[1:]:
            if k.startswith("'") and k.endswith("'"):
                parts.append(k[1:-1])
            else:
                parts.append("' || " + k + " || '")
        path = '.'.join(parts)
        return f"get_json_string({a[0]}, '$.{path}')"
    return f"get_json_string({a[0]}, '$')"


class json_object_keys(functions.GenericFunction):
    type = String()
    name = 'json_object_keys'
    inherit_cache = True


@compiles(json_object_keys, 'starrocks')
def _sr_json_object_keys(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_keys({a[0]})"


class json_each(functions.GenericFunction):
    """StarRocks does not have ``json_each``.  No viable translation
    exists – this will pass through unchanged and fail at runtime.
    Consider restructuring queries to use ``json_keys`` + ``json_query``."""
    name = 'json_each'
    inherit_cache = True

# No @compiles – passes through to fail explicitly.


class json_each_text(functions.GenericFunction):
    """See *json_each* – same limitation."""
    name = 'json_each_text'
    inherit_cache = True

# No @compiles – passes through to fail explicitly.


class json_array_elements(functions.GenericFunction):
    name = 'json_array_elements'
    inherit_cache = True


@compiles(json_array_elements, 'starrocks')
def _sr_json_array_elements(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"unnest(CAST({a[0]} AS ARRAY<JSON>))"


# ###################################################################
# Additional Databend functions not in the original expressions.csv
# ###################################################################

# ===================================================================
# Numeric – additional
# ===================================================================

class div0(functions.GenericFunction):
    """Division that returns 0 on divide-by-zero."""
    type = Numeric()
    name = 'div0'
    inherit_cache = True


@compiles(div0, 'starrocks')
def _sr_div0(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IF({a[1]} = 0, 0, {a[0]} / {a[1]})"


class divnull(functions.GenericFunction):
    """Division that returns NULL on divide-by-zero."""
    type = Numeric()
    name = 'divnull'
    inherit_cache = True


@compiles(divnull, 'starrocks')
def _sr_divnull(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IF({a[1]} = 0, NULL, {a[0]} / {a[1]})"


class factorial(functions.GenericFunction):
    """No native StarRocks factorial.  Approximation using a recursive
    CTE is impractical in a scalar context – emit a best-effort
    expression valid for n <= 20."""
    type = BigInteger()
    name = 'factorial'
    inherit_cache = True

# No @compiles – will pass through as factorial(...) and fail.
# Install a StarRocks UDF for this if needed.


# ===================================================================
# String – additional
# ===================================================================

class position(functions.GenericFunction):
    """``position(sub, str)`` → ``locate(sub, str)``."""
    type = Integer()
    name = 'position'
    inherit_cache = True


@compiles(position, 'starrocks')
def _sr_position(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 3:
        return f"locate({a[0]}, {a[1]}, {a[2]})"
    return f"locate({a[0]}, {a[1]})"


class oct(functions.GenericFunction):
    """``oct(n)`` → ``conv(n, 10, 8)``."""
    type = String()
    name = 'oct'
    inherit_cache = True


@compiles(oct, 'starrocks')
def _sr_oct(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"conv({a[0]}, 10, 8)"


class ord(functions.GenericFunction):
    """``ord(s)`` → ``ascii(s)`` (single-byte approximation)."""
    type = Integer()
    name = 'ord'
    inherit_cache = True


@compiles(ord, 'starrocks')
def _sr_ord(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"ascii({a[0]})"


class soundex(functions.GenericFunction):
    """No StarRocks equivalent.  Passes through – will fail at runtime."""
    type = String()
    name = 'soundex'
    inherit_cache = True

# No @compiles – not available in StarRocks.


class regexp_substr(functions.GenericFunction):
    """``regexp_substr(s, pattern)`` → ``regexp_extract(s, pattern, 0)``."""
    type = String()
    name = 'regexp_substr'
    inherit_cache = True


@compiles(regexp_substr, 'starrocks')
def _sr_regexp_substr(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"regexp_extract({a[0]}, {a[1]}, 0)"


class regexp_instr(functions.GenericFunction):
    """No direct StarRocks equivalent.  Best-effort: returns 1 if matched, 0 otherwise.
    For positional matching, consider ``locate`` + ``regexp_extract``."""
    type = Integer()
    name = 'regexp_instr'
    inherit_cache = True


@compiles(regexp_instr, 'starrocks')
def _sr_regexp_instr(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    # Best-effort: return position of first match (1) or 0
    return f"IF({a[0]} REGEXP {a[1]}, locate(regexp_extract({a[0]}, {a[1]}, 0), {a[0]}), 0)"


class regexp_like(functions.GenericFunction):
    """``regexp_like(s, pattern)`` → ``s REGEXP pattern``."""
    type = Boolean()
    name = 'regexp_like'
    inherit_cache = True


@compiles(regexp_like, 'starrocks')
def _sr_regexp_like(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"({a[0]} REGEXP {a[1]})"


class regexp_split_to_array(functions.GenericFunction):
    """``regexp_split_to_array(s, pattern)`` → ``split(s, pattern)``.

    Note: StarRocks ``split()`` splits on a *literal* delimiter, not a
    regex.  For single-character or literal delimiters this behaves
    identically; callers that pass actual regex patterns will get
    incorrect results and should restructure the query.
    """
    name = 'regexp_split_to_array'
    inherit_cache = True


# Regex meta-characters that indicate a non-literal pattern.
_REGEX_META = re.compile(r'[\\.*+?|^$\[\]{}()]')


@compiles(regexp_split_to_array, 'starrocks')
def _sr_regexp_split_to_array(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    # Best-effort detection: if the compiled pattern argument is a
    # string literal containing regex metacharacters, warn the caller.
    pat = a[1]
    if pat.startswith("'") and pat.endswith("'"):
        inner = pat[1:-1]
        if _REGEX_META.search(inner):
            _log.warning(
                "regexp_split_to_array() pattern %s contains regex "
                "metacharacters but StarRocks split() treats the "
                "delimiter as a literal string.  Results may differ "
                "from Databend/PostgreSQL.", pat
            )
    return f"split({a[0]}, {a[1]})"


class length_utf8(functions.GenericFunction):
    """``length_utf8(s)`` → ``char_length(s)``."""
    type = Integer()
    name = 'length_utf8'
    inherit_cache = True


@compiles(length_utf8, 'starrocks')
def _sr_length_utf8(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"char_length({a[0]})"


# ===================================================================
# Date / time – additional
# ===================================================================

class today(functions.GenericFunction):
    type = Date()
    name = 'today'
    inherit_cache = True


@compiles(today, 'starrocks')
def _sr_today(element, compiler, **kw):
    return "curdate()"


class tomorrow(functions.GenericFunction):
    type = Date()
    name = 'tomorrow'
    inherit_cache = True


@compiles(tomorrow, 'starrocks')
def _sr_tomorrow(element, compiler, **kw):
    return "date_add(curdate(), INTERVAL 1 DAY)"


class yesterday(functions.GenericFunction):
    type = Date()
    name = 'yesterday'
    inherit_cache = True


@compiles(yesterday, 'starrocks')
def _sr_yesterday(element, compiler, **kw):
    return "date_sub(curdate(), INTERVAL 1 DAY)"


class to_unix_timestamp(functions.GenericFunction):
    """``to_unix_timestamp(ts)`` → ``unix_timestamp(ts)``."""
    type = BigInteger()
    name = 'to_unix_timestamp'
    inherit_cache = True


@compiles(to_unix_timestamp, 'starrocks')
def _sr_to_unix_timestamp(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"unix_timestamp({a[0]})"


class to_yyyymm(functions.GenericFunction):
    type = Integer()
    name = 'to_yyyymm'
    inherit_cache = True


@compiles(to_yyyymm, 'starrocks')
def _sr_to_yyyymm(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(date_format({a[0]}, '%Y%m') AS INT)"


class to_yyyymmdd(functions.GenericFunction):
    type = Integer()
    name = 'to_yyyymmdd'
    inherit_cache = True


@compiles(to_yyyymmdd, 'starrocks')
def _sr_to_yyyymmdd(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(date_format({a[0]}, '%Y%m%d') AS INT)"


class to_yyyymmddhh(functions.GenericFunction):
    type = BigInteger()
    name = 'to_yyyymmddhh'
    inherit_cache = True


@compiles(to_yyyymmddhh, 'starrocks')
def _sr_to_yyyymmddhh(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(date_format({a[0]}, '%Y%m%d%H') AS BIGINT)"


class to_yyyymmddhhmmss(functions.GenericFunction):
    type = BigInteger()
    name = 'to_yyyymmddhhmmss'
    inherit_cache = True


@compiles(to_yyyymmddhhmmss, 'starrocks')
def _sr_to_yyyymmddhhmmss(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST(date_format({a[0]}, '%Y%m%d%H%i%s') AS BIGINT)"


class convert_timezone(functions.GenericFunction):
    """``convert_timezone(from_tz, to_tz, ts)`` → ``convert_tz(ts, from_tz, to_tz)``
    (argument order differs)."""
    type = DateTime()
    name = 'convert_timezone'
    inherit_cache = True


@compiles(convert_timezone, 'starrocks')
def _sr_convert_timezone(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) == 3:
        return f"convert_tz({a[2]}, {a[0]}, {a[1]})"
    # Two-arg form: convert_timezone(to_tz, ts) from session tz
    return f"convert_tz({a[1]}, 'UTC', {a[0]})"


class months_between(functions.GenericFunction):
    """``months_between(d1, d2)`` → ``months_diff(d1, d2)``."""
    type = Numeric()
    name = 'months_between'
    inherit_cache = True


@compiles(months_between, 'starrocks')
def _sr_months_between(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"months_diff({a[0]}, {a[1]})"


class timestamp_diff(functions.GenericFunction):
    """``timestamp_diff(unit, ts1, ts2)`` → ``timestampdiff(unit, ts1, ts2)``."""
    type = BigInteger()
    name = 'timestamp_diff'
    inherit_cache = True


@compiles(timestamp_diff, 'starrocks')
def _sr_timestamp_diff(element, compiler, **kw):
    clauses = list(element.clauses)
    unit_clause = clauses[0]
    if hasattr(unit_clause, 'value'):
        unit = unit_clause.value
    elif hasattr(unit_clause, 'text'):
        unit = unit_clause.text
    else:
        unit = str(compiler.process(unit_clause, **kw)).strip("'")
    ts1 = compiler.process(clauses[1], **kw)
    ts2 = compiler.process(clauses[2], **kw)
    return f"timestampdiff({unit}, {ts1}, {ts2})"


class to_start_of_year(functions.GenericFunction):
    type = Date()
    name = 'to_start_of_year'
    inherit_cache = True


@compiles(to_start_of_year, 'starrocks')
def _sr_to_start_of_year(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('year', {a[0]})"


class to_start_of_quarter(functions.GenericFunction):
    type = Date()
    name = 'to_start_of_quarter'
    inherit_cache = True


@compiles(to_start_of_quarter, 'starrocks')
def _sr_to_start_of_quarter(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('quarter', {a[0]})"


class to_start_of_month(functions.GenericFunction):
    type = Date()
    name = 'to_start_of_month'
    inherit_cache = True


@compiles(to_start_of_month, 'starrocks')
def _sr_to_start_of_month(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('month', {a[0]})"


class to_start_of_week(functions.GenericFunction):
    type = Date()
    name = 'to_start_of_week'
    inherit_cache = True


@compiles(to_start_of_week, 'starrocks')
def _sr_to_start_of_week(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('week', {a[0]})"


class to_start_of_day(functions.GenericFunction):
    type = DateTime()
    name = 'to_start_of_day'
    inherit_cache = True


@compiles(to_start_of_day, 'starrocks')
def _sr_to_start_of_day(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('day', {a[0]})"


class to_start_of_hour(functions.GenericFunction):
    type = DateTime()
    name = 'to_start_of_hour'
    inherit_cache = True


@compiles(to_start_of_hour, 'starrocks')
def _sr_to_start_of_hour(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('hour', {a[0]})"


class to_start_of_minute(functions.GenericFunction):
    type = DateTime()
    name = 'to_start_of_minute'
    inherit_cache = True


@compiles(to_start_of_minute, 'starrocks')
def _sr_to_start_of_minute(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('minute', {a[0]})"


class millennium(functions.GenericFunction):
    type = Integer()
    name = 'millennium'
    inherit_cache = True


@compiles(millennium, 'starrocks')
def _sr_millennium(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CEIL(year({a[0]}) / 1000.0)"


# ===================================================================
# Conversion – additional
# ===================================================================

class to_boolean(functions.GenericFunction):
    type = Boolean()
    name = 'to_boolean'
    inherit_cache = True


@compiles(to_boolean, 'starrocks')
def _sr_to_boolean(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    v = f"CAST({a[0]} AS VARCHAR)"
    return _bool_case(
        v,
        extras_true=" WHEN 'on' THEN TRUE",
        extras_false=" WHEN 'off' THEN FALSE",
    )


class to_string(functions.GenericFunction):
    type = String()
    name = 'to_string'
    inherit_cache = True


@compiles(to_string, 'starrocks')
def _sr_to_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS VARCHAR)"


class to_varchar(functions.GenericFunction):
    type = String()
    name = 'to_varchar'
    inherit_cache = True


@compiles(to_varchar, 'starrocks')
def _sr_to_varchar(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS VARCHAR)"


class to_int8(functions.GenericFunction):
    type = Integer()
    name = 'to_int8'
    inherit_cache = True


@compiles(to_int8, 'starrocks')
def _sr_to_int8(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS TINYINT)"


class to_int16(functions.GenericFunction):
    type = Integer()
    name = 'to_int16'
    inherit_cache = True


@compiles(to_int16, 'starrocks')
def _sr_to_int16(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS SMALLINT)"


class to_int32(functions.GenericFunction):
    type = Integer()
    name = 'to_int32'
    inherit_cache = True


@compiles(to_int32, 'starrocks')
def _sr_to_int32(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS INT)"


class to_int64(functions.GenericFunction):
    type = BigInteger()
    name = 'to_int64'
    inherit_cache = True


@compiles(to_int64, 'starrocks')
def _sr_to_int64(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS BIGINT)"


class to_uint8(functions.GenericFunction):
    """StarRocks has no unsigned types; maps to SMALLINT."""
    type = Integer()
    name = 'to_uint8'
    inherit_cache = True


@compiles(to_uint8, 'starrocks')
def _sr_to_uint8(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS SMALLINT)"


class to_uint16(functions.GenericFunction):
    type = Integer()
    name = 'to_uint16'
    inherit_cache = True


@compiles(to_uint16, 'starrocks')
def _sr_to_uint16(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS INT)"


class to_uint32(functions.GenericFunction):
    type = BigInteger()
    name = 'to_uint32'
    inherit_cache = True


@compiles(to_uint32, 'starrocks')
def _sr_to_uint32(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS BIGINT)"


class to_uint64(functions.GenericFunction):
    type = BigInteger()
    name = 'to_uint64'
    inherit_cache = True


@compiles(to_uint64, 'starrocks')
def _sr_to_uint64(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS LARGEINT)"


class to_float32(functions.GenericFunction):
    type = Numeric()
    name = 'to_float32'
    inherit_cache = True


@compiles(to_float32, 'starrocks')
def _sr_to_float32(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS FLOAT)"


class to_float64(functions.GenericFunction):
    type = Numeric()
    name = 'to_float64'
    inherit_cache = True


@compiles(to_float64, 'starrocks')
def _sr_to_float64(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS DOUBLE)"


# ===================================================================
# Conditional – additional
# ===================================================================

class iff(functions.GenericFunction):
    """``iff(cond, then, else)`` → ``IF(cond, then, else)``."""
    name = 'iff'
    inherit_cache = True


@compiles(iff, 'starrocks')
def _sr_iff(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    els = a[2] if len(a) >= 3 else 'NULL'
    return f"IF({a[0]}, {a[1]}, {els})"


class nvl(functions.GenericFunction):
    """``nvl(x, default)`` → ``IFNULL(x, default)``."""
    name = 'nvl'
    inherit_cache = True


@compiles(nvl, 'starrocks')
def _sr_nvl(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IFNULL({a[0]}, {a[1]})"


class nvl2(functions.GenericFunction):
    """``nvl2(x, if_not_null, if_null)`` → ``IF(x IS NOT NULL, …, …)``."""
    name = 'nvl2'
    inherit_cache = True


@compiles(nvl2, 'starrocks')
def _sr_nvl2(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IF({a[0]} IS NOT NULL, {a[1]}, {a[2]})"


class decode(functions.GenericFunction):
    """``decode(expr, s1, r1, s2, r2, …, default)`` → CASE expression.

    Databend decode takes variadic args: expr, then pairs of
    (search, result), and an optional trailing default.
    """
    name = 'decode'
    inherit_cache = True


@compiles(decode, 'starrocks')
def _sr_decode(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) < 3:
        return f"decode({', '.join(a)})"
    expr = a[0]
    pairs = a[1:]
    parts = [f"CASE {expr}"]
    i = 0
    while i + 1 < len(pairs):
        parts.append(f" WHEN {pairs[i]} THEN {pairs[i+1]}")
        i += 2
    if i < len(pairs):
        parts.append(f" ELSE {pairs[i]}")
    parts.append(" END")
    return "".join(parts)


class is_not_error(functions.GenericFunction):
    """Databend error handling – no StarRocks equivalent.  Returns TRUE always."""
    type = Boolean()
    name = 'is_not_error'
    inherit_cache = True


@compiles(is_not_error, 'starrocks')
def _sr_is_not_error(element, compiler, **kw):
    return "TRUE"


class error_or(functions.GenericFunction):
    """``error_or(expr, default)`` → ``IFNULL(expr, default)`` (best-effort)."""
    name = 'error_or'
    inherit_cache = True


@compiles(error_or, 'starrocks')
def _sr_error_or(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IFNULL({a[0]}, {a[1]})"


# ===================================================================
# Aggregate – additional
# ===================================================================

class arg_max(functions.GenericFunction):
    """``arg_max(arg, val)`` → ``max_by(arg, val)``."""
    name = 'arg_max'
    inherit_cache = True


@compiles(arg_max, 'starrocks')
def _sr_arg_max(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"max_by({a[0]}, {a[1]})"


class arg_min(functions.GenericFunction):
    """``arg_min(arg, val)`` → ``min_by(arg, val)``."""
    name = 'arg_min'
    inherit_cache = True


@compiles(arg_min, 'starrocks')
def _sr_arg_min(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"min_by({a[0]}, {a[1]})"


class string_agg(functions.GenericFunction):
    """``string_agg(expr, delim)`` → ``group_concat(expr SEPARATOR delim)``."""
    type = String()
    name = 'string_agg'
    inherit_cache = True


@compiles(string_agg, 'starrocks')
def _sr_string_agg(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"group_concat({a[0]} SEPARATOR {a[1]})"
    return f"group_concat({a[0]})"


class listagg(functions.GenericFunction):
    """``listagg(expr, delim)`` → ``group_concat(expr, delim)``."""
    type = String()
    name = 'listagg'
    inherit_cache = True


@compiles(listagg, 'starrocks')
def _sr_listagg(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"group_concat({a[0]} SEPARATOR {a[1]})"
    return f"group_concat({a[0]})"


class quantile_cont(functions.GenericFunction):
    """``quantile_cont(level, expr)`` → ``percentile_cont({level})``
    within StarRocks's window-function form.  Best-effort: uses
    ``percentile_approx(expr, level)`` as a scalar aggregate."""
    type = Numeric()
    name = 'quantile_cont'
    inherit_cache = True


@compiles(quantile_cont, 'starrocks')
def _sr_quantile_cont(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[1]}, {a[0]})"


class quantile_disc(functions.GenericFunction):
    type = Numeric()
    name = 'quantile_disc'
    inherit_cache = True


@compiles(quantile_disc, 'starrocks')
def _sr_quantile_disc(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[1]}, {a[0]})"


# ===================================================================
# JSON – additional
# ===================================================================

class json_path_query(functions.GenericFunction):
    """``json_path_query(json, path)`` → ``json_query(json, path)``."""
    type = String()
    name = 'json_path_query'
    inherit_cache = True


@compiles(json_path_query, 'starrocks')
def _sr_json_path_query(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_query({a[0]}, {a[1]})"


class json_path_query_first(functions.GenericFunction):
    type = String()
    name = 'json_path_query_first'
    inherit_cache = True


@compiles(json_path_query_first, 'starrocks')
def _sr_json_path_query_first(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_query({a[0]}, {a[1]})"


class json_path_exists(functions.GenericFunction):
    """``json_path_exists(json, path)`` → ``json_exists(json, path)``."""
    type = Boolean()
    name = 'json_path_exists'
    inherit_cache = True


@compiles(json_path_exists, 'starrocks')
def _sr_json_path_exists(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_exists({a[0]}, {a[1]})"


class json_to_string(functions.GenericFunction):
    """``json_to_string(x)`` → ``json_string(x)``."""
    type = String()
    name = 'json_to_string'
    inherit_cache = True


@compiles(json_to_string, 'starrocks')
def _sr_json_to_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_string({a[0]})"


class json_typeof(functions.GenericFunction):
    """No direct StarRocks equivalent.  Best-effort using CASE on json_string."""
    type = String()
    name = 'json_typeof'
    inherit_cache = True

# No @compiles – passes through; will fail. Consider installing a UDF.


class check_json(functions.GenericFunction):
    """``check_json(s)`` → parse and check validity.
    StarRocks: returns NULL if parse_json succeeds, error message otherwise."""
    type = String()
    name = 'check_json'
    inherit_cache = True


@compiles(check_json, 'starrocks')
def _sr_check_json(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"IF(parse_json({a[0]}) IS NOT NULL, NULL, 'invalid JSON')"


class json_strip_nulls(functions.GenericFunction):
    """No direct StarRocks equivalent.  Passes through."""
    type = String()
    name = 'json_strip_nulls'
    inherit_cache = True

# No @compiles – not available in StarRocks.


# ===================================================================
# Array – additional
# ===================================================================

class array_to_string(functions.GenericFunction):
    """``array_to_string(arr, delim)`` → ``array_join(arr, delim)``."""
    type = String()
    name = 'array_to_string'
    inherit_cache = True


@compiles(array_to_string, 'starrocks')
def _sr_array_to_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"array_join({a[0]}, {a[1]})"
    return f"array_join({a[0]}, ',')"


class array_indexof(functions.GenericFunction):
    """``array_indexof(arr, val)`` → ``array_position(arr, val)``."""
    type = Integer()
    name = 'array_indexof'
    inherit_cache = True


@compiles(array_indexof, 'starrocks')
def _sr_array_indexof(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_position({a[0]}, {a[1]})"


class array_compact(functions.GenericFunction):
    """Databend ``array_compact`` removes *consecutive duplicate* elements.
    StarRocks has no built-in equivalent.  Compilation raises
    ``CompileError`` because ``array_filter(…IS NOT NULL)`` (removes
    NULLs) and ``array_distinct`` (removes all duplicates regardless
    of position) are both semantically different."""
    name = 'array_compact'
    inherit_cache = True


@compiles(array_compact, 'starrocks')
def _sr_array_compact(element, compiler, **kw):
    raise CompileError(
        "array_compact() removes consecutive duplicates in Databend, "
        "but StarRocks has no equivalent operation.  Use "
        "array_distinct() to remove all duplicates (regardless of "
        "position), or restructure the query."
    )


class array_construct(functions.GenericFunction):
    """``array_construct(v1, v2, ...)`` → ``[v1, v2, ...]``."""
    name = 'array_construct'
    inherit_cache = True


@compiles(array_construct, 'starrocks')
def _sr_array_construct(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"[{', '.join(a)}]"


# ===================================================================
# Hash – additional
# ===================================================================

class xxhash64(functions.GenericFunction):
    """``xxhash64(x)`` → ``xx_hash3_64(x)``."""
    type = BigInteger()
    name = 'xxhash64'
    inherit_cache = True


@compiles(xxhash64, 'starrocks')
def _sr_xxhash64(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"xx_hash3_64({a[0]})"


class xxhash32(functions.GenericFunction):
    """StarRocks has no xxhash32.  Compilation raises ``CompileError``
    because silently substituting a different hash algorithm would
    produce output that does not match values computed on other engines."""
    type = Integer()
    name = 'xxhash32'
    inherit_cache = True


@compiles(xxhash32, 'starrocks')
def _sr_xxhash32(element, compiler, **kw):
    raise CompileError(
        "xxhash32() is not available in StarRocks and cannot be safely "
        "substituted with a different hash algorithm.  Use murmur_hash3_32() "
        "explicitly if cross-engine hash compatibility is not required."
    )


class sha1(functions.GenericFunction):
    """StarRocks does not have SHA-1.  Compilation raises ``CompileError``
    to prevent silent hash-algorithm substitution."""
    type = String()
    name = 'sha1'
    inherit_cache = True


@compiles(sha1, 'starrocks')
def _sr_sha1(element, compiler, **kw):
    raise CompileError(
        "sha1() is not available in StarRocks.  Use func.sha2(x, 256) "
        "explicitly if SHA-256 is acceptable, or func.md5(x) for a "
        "non-cryptographic fingerprint."
    )


class sha(functions.GenericFunction):
    """Alias for sha1 — same restriction applies."""
    type = String()
    name = 'sha'
    inherit_cache = True


@compiles(sha, 'starrocks')
def _sr_sha(element, compiler, **kw):
    raise CompileError(
        "sha() is not available in StarRocks.  Use func.sha2(x, 256) "
        "explicitly if SHA-256 is acceptable, or func.md5(x) for a "
        "non-cryptographic fingerprint."
    )


# ===================================================================
# Other / utility
# ===================================================================

class typeof(functions.GenericFunction):
    """No direct StarRocks equivalent.  Passes through."""
    type = String()
    name = 'typeof'
    inherit_cache = True

# No @compiles – not available in StarRocks.


class assume_not_null(functions.GenericFunction):
    """``assume_not_null(x)`` → ``x`` (pass through, optimization hint only)."""
    name = 'assume_not_null'
    inherit_cache = True


@compiles(assume_not_null, 'starrocks')
def _sr_assume_not_null(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]


class humanize_number(functions.GenericFunction):
    """Databend-specific formatting.  Best-effort: ``money_format`` adds
    comma-grouped digits but also appends ``.00`` decimal places, which
    differs from Databend's output.  A warning is logged."""
    type = String()
    name = 'humanize_number'
    inherit_cache = True


@compiles(humanize_number, 'starrocks')
def _sr_humanize_number(element, compiler, **kw):
    _log.warning(
        "humanize_number() compiled as money_format() on StarRocks — "
        "output includes fixed decimal places (e.g. '1,000.00') which "
        "differs from Databend's format.  Do not rely on exact string "
        "comparison of results."
    )
    a = _args(element, compiler, **kw)
    return f"money_format({a[0]})"


class humanize_size(functions.GenericFunction):
    """Databend-specific.  Best-effort using format_bytes (StarRocks 3.3+)."""
    type = String()
    name = 'humanize_size'
    inherit_cache = True


@compiles(humanize_size, 'starrocks')
def _sr_humanize_size(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"format_bytes({a[0]})"


# ###################################################################
# Additional Databend function wrappers — second pass
# ###################################################################

# ===================================================================
# Date / time – extraction helpers
# ===================================================================

class to_day_of_month(functions.GenericFunction):
    """``to_day_of_month(d)`` → ``dayofmonth(d)``."""
    type = Integer()
    name = 'to_day_of_month'
    inherit_cache = True


@compiles(to_day_of_month, 'starrocks')
def _sr_to_day_of_month(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"dayofmonth({a[0]})"


class to_day_of_week(functions.GenericFunction):
    """``to_day_of_week(d)`` → ``dayofweek(d)``.

    Note: Databend returns 1=Monday…7=Sunday; StarRocks ``dayofweek``
    returns 1=Sunday…7=Saturday (MySQL convention).  If Monday-based
    numbering is required, post-process the result.
    """
    type = Integer()
    name = 'to_day_of_week'
    inherit_cache = True


@compiles(to_day_of_week, 'starrocks')
def _sr_to_day_of_week(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"dayofweek({a[0]})"


class to_day_of_year(functions.GenericFunction):
    """``to_day_of_year(d)`` → ``dayofyear(d)``."""
    type = Integer()
    name = 'to_day_of_year'
    inherit_cache = True


@compiles(to_day_of_year, 'starrocks')
def _sr_to_day_of_year(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"dayofyear({a[0]})"


class to_hour(functions.GenericFunction):
    """``to_hour(ts)`` → ``hour(ts)``."""
    type = Integer()
    name = 'to_hour'
    inherit_cache = True


@compiles(to_hour, 'starrocks')
def _sr_to_hour(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"hour({a[0]})"


class to_minute(functions.GenericFunction):
    """``to_minute(ts)`` → ``minute(ts)``."""
    type = Integer()
    name = 'to_minute'
    inherit_cache = True


@compiles(to_minute, 'starrocks')
def _sr_to_minute(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"minute({a[0]})"


class to_second(functions.GenericFunction):
    """``to_second(ts)`` → ``second(ts)``."""
    type = Integer()
    name = 'to_second'
    inherit_cache = True


@compiles(to_second, 'starrocks')
def _sr_to_second(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"second({a[0]})"


class to_month(functions.GenericFunction):
    """``to_month(d)`` → ``month(d)``."""
    type = Integer()
    name = 'to_month'
    inherit_cache = True


@compiles(to_month, 'starrocks')
def _sr_to_month(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"month({a[0]})"


class to_quarter(functions.GenericFunction):
    """``to_quarter(d)`` → ``quarter(d)``."""
    type = Integer()
    name = 'to_quarter'
    inherit_cache = True


@compiles(to_quarter, 'starrocks')
def _sr_to_quarter(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"quarter({a[0]})"


class to_year(functions.GenericFunction):
    """``to_year(d)`` → ``year(d)``."""
    type = Integer()
    name = 'to_year'
    inherit_cache = True


@compiles(to_year, 'starrocks')
def _sr_to_year(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"year({a[0]})"


class to_week_of_year(functions.GenericFunction):
    """``to_week_of_year(d)`` → ``weekofyear(d)``."""
    type = Integer()
    name = 'to_week_of_year'
    inherit_cache = True


@compiles(to_week_of_year, 'starrocks')
def _sr_to_week_of_year(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"weekofyear({a[0]})"


# ===================================================================
# Date / time – sub-minute rounding
# ===================================================================

class to_start_of_second(functions.GenericFunction):
    """``to_start_of_second(ts)`` → ``date_trunc('second', ts)``."""
    type = DateTime()
    name = 'to_start_of_second'
    inherit_cache = True


@compiles(to_start_of_second, 'starrocks')
def _sr_to_start_of_second(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"date_trunc('second', {a[0]})"


class to_start_of_five_minutes(functions.GenericFunction):
    """``to_start_of_five_minutes(ts)`` – floor to nearest 5-minute boundary."""
    type = DateTime()
    name = 'to_start_of_five_minutes'
    inherit_cache = True


@compiles(to_start_of_five_minutes, 'starrocks')
def _sr_to_start_of_five_minutes(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"from_unixtime(floor(unix_timestamp({a[0]}) / 300) * 300)"


class to_start_of_ten_minutes(functions.GenericFunction):
    """``to_start_of_ten_minutes(ts)`` – floor to nearest 10-minute boundary."""
    type = DateTime()
    name = 'to_start_of_ten_minutes'
    inherit_cache = True


@compiles(to_start_of_ten_minutes, 'starrocks')
def _sr_to_start_of_ten_minutes(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"from_unixtime(floor(unix_timestamp({a[0]}) / 600) * 600)"


class to_start_of_fifteen_minutes(functions.GenericFunction):
    """``to_start_of_fifteen_minutes(ts)`` – floor to nearest 15-minute boundary."""
    type = DateTime()
    name = 'to_start_of_fifteen_minutes'
    inherit_cache = True


@compiles(to_start_of_fifteen_minutes, 'starrocks')
def _sr_to_start_of_fifteen_minutes(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"from_unixtime(floor(unix_timestamp({a[0]}) / 900) * 900)"


# ===================================================================
# Date / time – other
# ===================================================================

class add_months(functions.GenericFunction):
    """``add_months(d, n)`` → ``months_add(d, n)``."""
    type = Date()
    name = 'add_months'
    inherit_cache = True


@compiles(add_months, 'starrocks')
def _sr_add_months(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"months_add({a[0]}, {a[1]})"


class time_slot(functions.GenericFunction):
    """``time_slot(ts)`` – floor timestamp to nearest 30-minute boundary."""
    type = DateTime()
    name = 'time_slot'
    inherit_cache = True


@compiles(time_slot, 'starrocks')
def _sr_time_slot(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"from_unixtime(floor(unix_timestamp({a[0]}) / 1800) * 1800)"


class to_datetime(functions.GenericFunction):
    """``to_datetime(s)`` → ``CAST(s AS DATETIME)``."""
    type = DateTime()
    name = 'to_datetime'
    inherit_cache = True


@compiles(to_datetime, 'starrocks')
def _sr_to_datetime(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"str_to_date({a[0]}, '{translated}')"
        return f"str_to_date({a[0]}, {fmt})"
    return f"CAST({a[0]} AS DATETIME)"


class try_to_timestamp(functions.GenericFunction):
    """``try_to_timestamp(s)`` – like to_timestamp but returns NULL on
    failure.  StarRocks CAST already returns NULL on bad input."""
    type = DateTime()
    name = 'try_to_timestamp'
    inherit_cache = True


@compiles(try_to_timestamp, 'starrocks')
def _sr_try_to_timestamp(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"str_to_date({a[0]}, '{translated}')"
        return f"str_to_date({a[0]}, {fmt})"
    return f"CAST({a[0]} AS DATETIME)"


class try_to_datetime(functions.GenericFunction):
    """``try_to_datetime(s)`` – alias for try_to_timestamp."""
    type = DateTime()
    name = 'try_to_datetime'
    inherit_cache = True


@compiles(try_to_datetime, 'starrocks')
def _sr_try_to_datetime(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        fmt = a[1]
        if fmt.startswith("'") and fmt.endswith("'"):
            translated = _pg_to_mysql_fmt(fmt[1:-1])
            return f"str_to_date({a[0]}, '{translated}')"
        return f"str_to_date({a[0]}, {fmt})"
    return f"CAST({a[0]} AS DATETIME)"


class next_day(functions.GenericFunction):
    """``next_day(d, dow)`` – returns the first date after *d* that falls
    on the specified day of the week (Sunday=1 … Saturday=7 in StarRocks).

    Note: Databend uses string day names ('Monday', etc.).  The wrapper
    accepts both numeric and string forms.
    """
    type = Date()
    name = 'next_day'
    inherit_cache = True


@compiles(next_day, 'starrocks')
def _sr_next_day(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"next_day({a[0]}, {a[1]})"


class previous_day(functions.GenericFunction):
    """``previous_day(d, dow)`` – returns the last date before *d* that
    falls on the specified day of the week."""
    type = Date()
    name = 'previous_day'
    inherit_cache = True


@compiles(previous_day, 'starrocks')
def _sr_previous_day(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"previous_day({a[0]}, {a[1]})"


# ===================================================================
# String – additional aliases
# ===================================================================

class mid(functions.GenericFunction):
    """``mid(s, pos, len)`` → ``substring(s, pos, len)`` (alias)."""
    type = String()
    name = 'mid'
    inherit_cache = True


@compiles(mid, 'starrocks')
def _sr_mid(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 3:
        return f"substring({a[0]}, {a[1]}, {a[2]})"
    return f"substring({a[0]}, {a[1]})"


class trim_both(functions.GenericFunction):
    """``trim_both(s [, chars])`` → ``TRIM(BOTH chars FROM s)``."""
    type = String()
    name = 'trim_both'
    inherit_cache = True


@compiles(trim_both, 'starrocks')
def _sr_trim_both(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(BOTH {a[1]} FROM {a[0]})"
    return f"TRIM({a[0]})"


class trim_leading(functions.GenericFunction):
    """``trim_leading(s [, chars])`` → ``TRIM(LEADING chars FROM s)``."""
    type = String()
    name = 'trim_leading'
    inherit_cache = True


@compiles(trim_leading, 'starrocks')
def _sr_trim_leading(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(LEADING {a[1]} FROM {a[0]})"
    return f"ltrim({a[0]})"


class trim_trailing(functions.GenericFunction):
    """``trim_trailing(s [, chars])`` → ``TRIM(TRAILING chars FROM s)``."""
    type = String()
    name = 'trim_trailing'
    inherit_cache = True


@compiles(trim_trailing, 'starrocks')
def _sr_trim_trailing(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"TRIM(TRAILING {a[1]} FROM {a[0]})"
    return f"rtrim({a[0]})"


class split_part(functions.GenericFunction):
    """``split_part(s, delim, part)`` – exists in both Databend and
    StarRocks with the same name and semantics.  Register the class
    so that SQLAlchemy resolves ``func.split_part()`` consistently."""
    type = String()
    name = 'split_part'
    inherit_cache = True

# No @compiles – same name in StarRocks.


class regexp_replace(functions.GenericFunction):
    """``regexp_replace`` exists in both Databend and StarRocks.
    Register for consistent resolution."""
    type = String()
    name = 'regexp_replace'
    inherit_cache = True

# No @compiles – same name in StarRocks.


# ===================================================================
# Numeric – additional
# ===================================================================

class intdiv(functions.GenericFunction):
    """``intdiv(a, b)`` → ``floor(a / b)`` (integer division)."""
    type = BigInteger()
    name = 'intdiv'
    inherit_cache = True


@compiles(intdiv, 'starrocks')
def _sr_intdiv(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"floor({a[0]} / {a[1]})"


class crc32(functions.GenericFunction):
    """``crc32(s)`` – exists in both Databend and StarRocks."""
    type = BigInteger()
    name = 'crc32'
    inherit_cache = True

# No @compiles – same name in StarRocks.


# ===================================================================
# Conversion – additional
# ===================================================================

class to_text(functions.GenericFunction):
    """``to_text(x)`` → ``CAST(x AS VARCHAR)`` (alias for to_string)."""
    type = String()
    name = 'to_text'
    inherit_cache = True


@compiles(to_text, 'starrocks')
def _sr_to_text(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS VARCHAR)"


class to_decimal(functions.GenericFunction):
    """``to_decimal(x, p, s)`` → ``CAST(x AS DECIMAL(p, s))``.

    Single-arg form falls back to ``CAST(x AS DECIMAL)``.
    """
    type = Numeric()
    name = 'to_decimal'
    inherit_cache = True


@compiles(to_decimal, 'starrocks')
def _sr_to_decimal(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 3:
        return f"CAST({a[0]} AS DECIMAL({a[1]}, {a[2]}))"
    if len(a) >= 2:
        return f"CAST({a[0]} AS DECIMAL({a[1]}, 0))"
    return f"CAST({a[0]} AS DECIMAL)"


class try_cast(functions.GenericFunction):
    """``try_cast`` is a Databend function that returns NULL on failure.
    StarRocks CAST already returns NULL for most type failures, so
    this is a best-effort pass-through."""
    name = 'try_cast'
    inherit_cache = True

# No @compiles – StarRocks CAST is already lenient. Users should
# use SQLAlchemy's cast() construct directly.


# ===================================================================
# Conditional – additional
# ===================================================================

class is_error(functions.GenericFunction):
    """``is_error(expr)`` – Databend error detection.  No StarRocks
    equivalent; always returns FALSE."""
    type = Boolean()
    name = 'is_error'
    inherit_cache = True


@compiles(is_error, 'starrocks')
def _sr_is_error(element, compiler, **kw):
    return "FALSE"


class greatest_ignore_nulls(functions.GenericFunction):
    """``greatest_ignore_nulls(a, b, ...)`` – like GREATEST but skips NULLs.

    StarRocks ``GREATEST`` returns NULL if any arg is NULL.  The wrapper
    builds a CASE chain for 2 args and falls back to a nested COALESCE
    approach for more args.
    """
    name = 'greatest_ignore_nulls'
    inherit_cache = True


@compiles(greatest_ignore_nulls, 'starrocks')
def _sr_greatest_ignore_nulls(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) == 1:
        return a[0]
    if len(a) == 2:
        return (
            f"CASE"
            f" WHEN {a[0]} IS NULL THEN {a[1]}"
            f" WHEN {a[1]} IS NULL THEN {a[0]}"
            f" WHEN {a[0]} >= {a[1]} THEN {a[0]}"
            f" ELSE {a[1]}"
            f" END"
        )
    # For 3+ args: chain pairwise.
    # greatest_ignore_nulls(a, b, c) →
    #   greatest_ignore_nulls(greatest_ignore_nulls(a, b), c)
    # We inline the CASE logic iteratively.
    result = a[0]
    for nxt in a[1:]:
        result = (
            f"CASE"
            f" WHEN ({result}) IS NULL THEN {nxt}"
            f" WHEN {nxt} IS NULL THEN ({result})"
            f" WHEN ({result}) >= {nxt} THEN ({result})"
            f" ELSE {nxt}"
            f" END"
        )
    return result


class least_ignore_nulls(functions.GenericFunction):
    """``least_ignore_nulls(a, b, ...)`` – like LEAST but skips NULLs."""
    name = 'least_ignore_nulls'
    inherit_cache = True


@compiles(least_ignore_nulls, 'starrocks')
def _sr_least_ignore_nulls(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) == 1:
        return a[0]
    if len(a) == 2:
        return (
            f"CASE"
            f" WHEN {a[0]} IS NULL THEN {a[1]}"
            f" WHEN {a[1]} IS NULL THEN {a[0]}"
            f" WHEN {a[0]} <= {a[1]} THEN {a[0]}"
            f" ELSE {a[1]}"
            f" END"
        )
    result = a[0]
    for nxt in a[1:]:
        result = (
            f"CASE"
            f" WHEN ({result}) IS NULL THEN {nxt}"
            f" WHEN {nxt} IS NULL THEN ({result})"
            f" WHEN ({result}) <= {nxt} THEN ({result})"
            f" ELSE {nxt}"
            f" END"
        )
    return result


# ===================================================================
# Aggregate – additional
# ===================================================================

class median_tdigest(functions.GenericFunction):
    """``median_tdigest(x)`` → ``percentile_approx(x, 0.5)``."""
    type = Numeric()
    name = 'median_tdigest'
    inherit_cache = True


@compiles(median_tdigest, 'starrocks')
def _sr_median_tdigest(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[0]}, 0.5)"


class quantile_tdigest(functions.GenericFunction):
    """``quantile_tdigest(level, x)`` → ``percentile_approx(x, level)``."""
    type = Numeric()
    name = 'quantile_tdigest'
    inherit_cache = True


@compiles(quantile_tdigest, 'starrocks')
def _sr_quantile_tdigest(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[1]}, {a[0]})"


class quantile_tdigest_weighted(functions.GenericFunction):
    """``quantile_tdigest_weighted(level, x, w)`` – StarRocks has no
    weighted percentile.  Best-effort: uses ``percentile_approx(x, level)``
    ignoring the weight argument.  A warning is logged."""
    type = Numeric()
    name = 'quantile_tdigest_weighted'
    inherit_cache = True


@compiles(quantile_tdigest_weighted, 'starrocks')
def _sr_quantile_tdigest_weighted(element, compiler, **kw):
    _log.warning(
        "quantile_tdigest_weighted() compiled as percentile_approx() on "
        "StarRocks — weight argument is ignored.  Results may differ "
        "from Databend."
    )
    a = _args(element, compiler, **kw)
    return f"percentile_approx({a[1]}, {a[0]})"


class mode(functions.GenericFunction):
    """``mode(x)`` – returns the most frequent value.  No direct
    StarRocks equivalent.  Compilation raises ``CompileError``."""
    name = 'mode'
    inherit_cache = True


@compiles(mode, 'starrocks')
def _sr_mode(element, compiler, **kw):
    raise CompileError(
        "mode() is not available in StarRocks.  Use a subquery with "
        "GROUP BY + COUNT + ORDER BY + LIMIT 1 to find the most "
        "frequent value."
    )


class kurtosis(functions.GenericFunction):
    """``kurtosis(x)`` – not available in StarRocks."""
    type = Numeric()
    name = 'kurtosis'
    inherit_cache = True


@compiles(kurtosis, 'starrocks')
def _sr_kurtosis(element, compiler, **kw):
    raise CompileError(
        "kurtosis() is not available in StarRocks."
    )


class skewness(functions.GenericFunction):
    """``skewness(x)`` – not available in StarRocks."""
    type = Numeric()
    name = 'skewness'
    inherit_cache = True


@compiles(skewness, 'starrocks')
def _sr_skewness(element, compiler, **kw):
    raise CompileError(
        "skewness() is not available in StarRocks."
    )


class json_array_agg(functions.GenericFunction):
    """``json_array_agg(x)`` → ``to_json(array_agg(x))``."""
    type = String()
    name = 'json_array_agg'
    inherit_cache = True


@compiles(json_array_agg, 'starrocks')
def _sr_json_array_agg(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"to_json(array_agg({a[0]}))"


class json_object_agg(functions.GenericFunction):
    """``json_object_agg(key, value)`` → ``to_json(map_agg(key, value))``."""
    type = String()
    name = 'json_object_agg'
    inherit_cache = True


@compiles(json_object_agg, 'starrocks')
def _sr_json_object_agg(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"to_json(map_agg({a[0]}, {a[1]}))"


# ===================================================================
# Array – additional
# ===================================================================

class array_size(functions.GenericFunction):
    """``array_size(arr)`` → ``array_length(arr)``."""
    type = Integer()
    name = 'array_size'
    inherit_cache = True


@compiles(array_size, 'starrocks')
def _sr_array_size(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_length({a[0]})"


class array_unique(functions.GenericFunction):
    """``array_unique(arr)`` → ``array_distinct(arr)``."""
    name = 'array_unique'
    inherit_cache = True


@compiles(array_unique, 'starrocks')
def _sr_array_unique(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_distinct({a[0]})"


class array_intersection(functions.GenericFunction):
    """``array_intersection(a, b)`` → ``array_intersect(a, b)``."""
    name = 'array_intersection'
    inherit_cache = True


@compiles(array_intersection, 'starrocks')
def _sr_array_intersection(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_intersect({a[0]}, {a[1]})"


class array_overlap(functions.GenericFunction):
    """``array_overlap(a, b)`` → ``arrays_overlap(a, b)``."""
    type = Boolean()
    name = 'array_overlap'
    inherit_cache = True


@compiles(array_overlap, 'starrocks')
def _sr_array_overlap(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"arrays_overlap({a[0]}, {a[1]})"


class array_transform(functions.GenericFunction):
    """``array_transform(arr, lambda)`` → ``array_map(arr, lambda)``.

    Note: Lambda-expression translation is not always possible through
    SQLAlchemy.  This wrapper emits ``array_map(...)`` with the args
    in their original order.  If the lambda syntax does not compile
    correctly, restructure the query to use StarRocks lambda syntax
    directly.
    """
    name = 'array_transform'
    inherit_cache = True


@compiles(array_transform, 'starrocks')
def _sr_array_transform(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_map({', '.join(a)})"


class array_flatten(functions.GenericFunction):
    """``array_flatten(arr)`` – flatten nested arrays.

    StarRocks has ``array_flatten`` since 3.3+.  Falls through with
    the same name.
    """
    name = 'array_flatten'
    inherit_cache = True

# No @compiles – same name in StarRocks 3.3+.


class array_reverse(functions.GenericFunction):
    """``array_reverse(arr)`` – reverse array element order.
    StarRocks has ``reverse()`` for arrays."""
    name = 'array_reverse'
    inherit_cache = True


@compiles(array_reverse, 'starrocks')
def _sr_array_reverse(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"reverse({a[0]})"


class contains(functions.GenericFunction):
    """``contains(arr, val)`` → ``array_contains(arr, val)``
    (Databend alias for array_contains)."""
    type = Boolean()
    name = 'contains'
    inherit_cache = True


@compiles(contains, 'starrocks')
def _sr_contains(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_contains({a[0]}, {a[1]})"


class array_get(functions.GenericFunction):
    """``array_get(arr, idx)`` → ``element_at(arr, idx)``."""
    name = 'array_get'
    inherit_cache = True


@compiles(array_get, 'starrocks')
def _sr_array_get(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"element_at({a[0]}, {a[1]})"


class array_except(functions.GenericFunction):
    """``array_except(a, b)`` → ``array_difference(a, b)``."""
    name = 'array_except'
    inherit_cache = True


@compiles(array_except, 'starrocks')
def _sr_array_except(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_difference({a[0]}, {a[1]})"


class array_prepend(functions.GenericFunction):
    """``array_prepend(arr, val)`` → ``array_concat([val], arr)``.

    Databend prepends *val* to *arr*.  StarRocks has no direct
    ``array_prepend``; we emulate with ``array_concat``.
    """
    name = 'array_prepend'
    inherit_cache = True


@compiles(array_prepend, 'starrocks')
def _sr_array_prepend(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_concat([{a[1]}], {a[0]})"


class array_remove_first(functions.GenericFunction):
    """``array_remove_first(arr)`` → ``array_slice(arr, 2)``
    (skip the first element)."""
    name = 'array_remove_first'
    inherit_cache = True


@compiles(array_remove_first, 'starrocks')
def _sr_array_remove_first(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_slice({a[0]}, 2)"


class array_remove_last(functions.GenericFunction):
    """``array_remove_last(arr)`` → ``array_slice(arr, 1, array_length(arr) - 1)``."""
    name = 'array_remove_last'
    inherit_cache = True


@compiles(array_remove_last, 'starrocks')
def _sr_array_remove_last(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_slice({a[0]}, 1, array_length({a[0]}) - 1)"


class array_count(functions.GenericFunction):
    """``array_count(arr, val)`` → ``array_length(array_filter(arr, x -> x = val))``.

    Simplified version: if only one arg, count non-NULL elements.
    """
    type = Integer()
    name = 'array_count'
    inherit_cache = True


@compiles(array_count, 'starrocks')
def _sr_array_count(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    if len(a) >= 2:
        return f"array_length(array_filter({a[0]}, x -> x = {a[1]}))"
    # Count non-NULL elements
    return f"array_length(array_filter({a[0]}, x -> x IS NOT NULL))"


class array_generate_range(functions.GenericFunction):
    """``array_generate_range(start, stop [, step])`` → ``array_generate(start, stop [, step])``."""
    name = 'array_generate_range'
    inherit_cache = True


@compiles(array_generate_range, 'starrocks')
def _sr_array_generate_range(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_generate({', '.join(a)})"


# ===================================================================
# JSON – additional
# ===================================================================

class json_path_query_array(functions.GenericFunction):
    """``json_path_query_array(json, path)`` → ``json_query(json, path)``
    (best-effort; StarRocks json_query returns a JSON value)."""
    type = String()
    name = 'json_path_query_array'
    inherit_cache = True


@compiles(json_path_query_array, 'starrocks')
def _sr_json_path_query_array(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_query({a[0]}, {a[1]})"


class json_path_match(functions.GenericFunction):
    """``json_path_match(json, path)`` – Databend path-predicate match.
    No direct StarRocks equivalent.  Best-effort: test json_exists."""
    type = Boolean()
    name = 'json_path_match'
    inherit_cache = True


@compiles(json_path_match, 'starrocks')
def _sr_json_path_match(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_exists({a[0]}, {a[1]})"


class json_pretty(functions.GenericFunction):
    """``json_pretty(json)`` – no StarRocks equivalent; falls back to
    casting to string."""
    type = String()
    name = 'json_pretty'
    inherit_cache = True


@compiles(json_pretty, 'starrocks')
def _sr_json_pretty(element, compiler, **kw):
    _log.warning(
        "json_pretty() has no StarRocks equivalent — falling back to "
        "CAST AS VARCHAR which does not produce formatted output."
    )
    a = _args(element, compiler, **kw)
    return f"CAST({a[0]} AS VARCHAR)"


class get(functions.GenericFunction):
    """``get(json, key)`` → ``json_query(json, concat('$.', key))``
    (Databend semi-structured access)."""
    type = String()
    name = 'get'
    inherit_cache = True


@compiles(get, 'starrocks')
def _sr_get(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    k = a[1]
    if k.startswith("'") and k.endswith("'"):
        return f"json_query({a[0]}, '$.{k[1:-1]}')"
    return f"json_query({a[0]}, concat('$.', {k}))"


class get_path(functions.GenericFunction):
    """``get_path(json, path)`` → ``json_query(json, path)``."""
    type = String()
    name = 'get_path'
    inherit_cache = True


@compiles(get_path, 'starrocks')
def _sr_get_path(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"json_query({a[0]}, {a[1]})"


# ===================================================================
# IP address functions
# ===================================================================

class ipv4_string_to_num(functions.GenericFunction):
    """``ipv4_string_to_num(s)`` → ``inet_aton(s)``."""
    type = BigInteger()
    name = 'ipv4_string_to_num'
    inherit_cache = True


@compiles(ipv4_string_to_num, 'starrocks')
def _sr_ipv4_string_to_num(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_aton({a[0]})"


class ipv4_num_to_string(functions.GenericFunction):
    """``ipv4_num_to_string(n)`` → ``inet_ntoa(n)``."""
    type = String()
    name = 'ipv4_num_to_string'
    inherit_cache = True


@compiles(ipv4_num_to_string, 'starrocks')
def _sr_ipv4_num_to_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_ntoa({a[0]})"


class try_inet_aton(functions.GenericFunction):
    """``try_inet_aton(s)`` → ``inet_aton(s)`` (best-effort; StarRocks
    returns NULL on invalid input)."""
    type = BigInteger()
    name = 'try_inet_aton'
    inherit_cache = True


@compiles(try_inet_aton, 'starrocks')
def _sr_try_inet_aton(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_aton({a[0]})"


class try_inet_ntoa(functions.GenericFunction):
    """``try_inet_ntoa(n)`` → ``inet_ntoa(n)``."""
    type = String()
    name = 'try_inet_ntoa'
    inherit_cache = True


@compiles(try_inet_ntoa, 'starrocks')
def _sr_try_inet_ntoa(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_ntoa({a[0]})"


class try_ipv4_string_to_num(functions.GenericFunction):
    """``try_ipv4_string_to_num(s)`` → ``inet_aton(s)``."""
    type = BigInteger()
    name = 'try_ipv4_string_to_num'
    inherit_cache = True


@compiles(try_ipv4_string_to_num, 'starrocks')
def _sr_try_ipv4_string_to_num(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_aton({a[0]})"


class try_ipv4_num_to_string(functions.GenericFunction):
    """``try_ipv4_num_to_string(n)`` → ``inet_ntoa(n)``."""
    type = String()
    name = 'try_ipv4_num_to_string'
    inherit_cache = True


@compiles(try_ipv4_num_to_string, 'starrocks')
def _sr_try_ipv4_num_to_string(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"inet_ntoa({a[0]})"


# ===================================================================
# Map functions
# ===================================================================

class map_cat(functions.GenericFunction):
    """``map_cat(m1, m2)`` → ``map_concat(m1, m2)``."""
    name = 'map_cat'
    inherit_cache = True


@compiles(map_cat, 'starrocks')
def _sr_map_cat(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"map_concat({', '.join(a)})"


class map_contains_key(functions.GenericFunction):
    """``map_contains_key(m, k)`` → ``array_contains(map_keys(m), k)``."""
    type = Boolean()
    name = 'map_contains_key'
    inherit_cache = True


@compiles(map_contains_key, 'starrocks')
def _sr_map_contains_key(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"array_contains(map_keys({a[0]}), {a[1]})"


# ===================================================================
# Hash – additional (CompileError for missing algorithms)
# ===================================================================

class siphash(functions.GenericFunction):
    """``siphash(x)`` – not available in StarRocks."""
    type = BigInteger()
    name = 'siphash'
    inherit_cache = True


@compiles(siphash, 'starrocks')
def _sr_siphash(element, compiler, **kw):
    raise CompileError(
        "siphash() is not available in StarRocks.  Use "
        "func.murmur_hash3_32() or func.xx_hash3_64() as alternatives."
    )


class siphash64(functions.GenericFunction):
    """``siphash64(x)`` – not available in StarRocks."""
    type = BigInteger()
    name = 'siphash64'
    inherit_cache = True


@compiles(siphash64, 'starrocks')
def _sr_siphash64(element, compiler, **kw):
    raise CompileError(
        "siphash64() is not available in StarRocks.  Use "
        "func.xx_hash3_64() as an alternative 64-bit hash."
    )


class blake3(functions.GenericFunction):
    """``blake3(x)`` – not available in StarRocks."""
    type = String()
    name = 'blake3'
    inherit_cache = True


@compiles(blake3, 'starrocks')
def _sr_blake3(element, compiler, **kw):
    raise CompileError(
        "blake3() is not available in StarRocks.  Use func.md5() "
        "or func.sha2(x, 256) as alternatives."
    )


class city64withseed(functions.GenericFunction):
    """``city64withseed(x, seed)`` – not available in StarRocks."""
    type = BigInteger()
    name = 'city64withseed'
    inherit_cache = True


@compiles(city64withseed, 'starrocks')
def _sr_city64withseed(element, compiler, **kw):
    raise CompileError(
        "city64withseed() is not available in StarRocks.  Use "
        "func.xx_hash3_64() as an alternative 64-bit hash."
    )


# ===================================================================
# Bitmap functions – name differences
# ===================================================================

class bitmap_and_not(functions.GenericFunction):
    """``bitmap_and_not(a, b)`` → ``bitmap_andnot(a, b)``."""
    name = 'bitmap_and_not'
    inherit_cache = True


@compiles(bitmap_and_not, 'starrocks')
def _sr_bitmap_and_not(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"bitmap_andnot({a[0]}, {a[1]})"


class bitmap_cardinality(functions.GenericFunction):
    """``bitmap_cardinality(bm)`` → ``bitmap_count(bm)``."""
    type = BigInteger()
    name = 'bitmap_cardinality'
    inherit_cache = True


@compiles(bitmap_cardinality, 'starrocks')
def _sr_bitmap_cardinality(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"bitmap_count({a[0]})"


class build_bitmap(functions.GenericFunction):
    """``build_bitmap(arr)`` → ``bitmap_from_string(arr)``
    (best-effort; input format may differ)."""
    name = 'build_bitmap'
    inherit_cache = True


@compiles(build_bitmap, 'starrocks')
def _sr_build_bitmap(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"bitmap_from_string({a[0]})"


# ===================================================================
# VARIANT / semi-structured – pass-through or CompileError
# ===================================================================

class to_variant(functions.GenericFunction):
    """``to_variant(x)`` – Databend VARIANT constructor.
    StarRocks has no VARIANT type; best-effort: cast to JSON."""
    type = String()
    name = 'to_variant'
    inherit_cache = True


@compiles(to_variant, 'starrocks')
def _sr_to_variant(element, compiler, **kw):
    a = _args(element, compiler, **kw)
    return f"parse_json(CAST({a[0]} AS VARCHAR))"


class remove_nullable(functions.GenericFunction):
    """``remove_nullable(x)`` → ``x`` (pass through, type hint only)."""
    name = 'remove_nullable'
    inherit_cache = True


@compiles(remove_nullable, 'starrocks')
def _sr_remove_nullable(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]


class to_nullable(functions.GenericFunction):
    """``to_nullable(x)`` → ``x`` (pass through, type hint only)."""
    name = 'to_nullable'
    inherit_cache = True


@compiles(to_nullable, 'starrocks')
def _sr_to_nullable(element, compiler, **kw):
    return _args(element, compiler, **kw)[0]
