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
