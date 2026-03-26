#!/usr/bin/env python
# coding=utf-8
"""Smoke tests for StarRocks compatibility wrappers.

Each test compiles a ``func.xxx()`` expression against a mock StarRocks
dialect and verifies the SQL string emitted by the @compiles override.
The default (non-StarRocks) compilation is also spot-checked to ensure
existing dialects are unaffected.
"""

import unittest

from sqlalchemy import column, func
from sqlalchemy.dialects import postgresql
from sqlalchemy.exc import CompileError

# Importing the module registers all @compiles decorators.
import plaidcloud.utilities.starrocks_compat as compat  # noqa: F401

# When sqlalchemy_functions.py is available, some function names are
# handled by its own @compiles handlers (different SQL output).
_HAS_SQLA_FUNCS = compat._HAS_SQLA_FUNCS

# Try to import the StarRocks dialect; fall back to a simple mock
# that just sets dialect.name = 'starrocks'.
try:
    from starrocks.sqlalchemy.dialect import StarRocksDialect
    _sr_dialect = StarRocksDialect()
except Exception:
    # Package not installed locally – build a minimal mock dialect
    from sqlalchemy.dialects.mysql import dialect as _mysql_dialect_cls

    class _MockStarRocksDialect(_mysql_dialect_cls):
        name = 'starrocks'

    _sr_dialect = _MockStarRocksDialect()

_pg_dialect = postgresql.dialect()


def _sr_sql(expr):
    """Compile *expr* against a StarRocks dialect and return SQL text."""
    compiled = expr.compile(dialect=_sr_dialect)
    return str(compiled)


def _pg_sql(expr):
    """Compile *expr* against PostgreSQL and return SQL text."""
    compiled = expr.compile(dialect=_pg_dialect)
    return str(compiled)


class TestConversions(unittest.TestCase):
    def test_to_char(self):
        sql = _sr_sql(func.to_char(column('ts'), 'YYYY-MM-DD'))
        self.assertIn('date_format', sql)

    def test_to_number(self):
        sql = _sr_sql(func.to_number(column('v'), 'text'))
        self.assertIn('CAST', sql)
        self.assertIn('DOUBLE', sql)

    def test_unix_to_timestamp(self):
        sql = _sr_sql(func.unix_to_timestamp(column('v')))
        self.assertIn('from_unixtime', sql)

    def test_to_timestamp_two_args(self):
        sql = _sr_sql(func.to_timestamp(column('v'), 'YYYY-MM-DD'))
        self.assertIn('str_to_date', sql)

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'to_timestamp handled by sqlalchemy_functions')
    def test_to_timestamp_one_arg(self):
        sql = _sr_sql(func.to_timestamp(column('v')))
        self.assertIn('from_unixtime', sql)

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'to_date handled by sqlalchemy_functions')
    def test_to_date_two_args(self):
        sql = _sr_sql(func.to_date(column('v'), 'YYYY-MM-DD'))
        self.assertIn('str_to_date', sql)


class TestDates(unittest.TestCase):
    def test_age_two_args(self):
        sql = _sr_sql(func.age(column('ts1'), column('ts2')))
        self.assertIn('timestampdiff', sql)

    def test_clock_timestamp(self):
        sql = _sr_sql(func.clock_timestamp())
        self.assertEqual(sql, 'now()')

    def test_date_part(self):
        sql = _sr_sql(func.date_part('year', column('ts')))
        self.assertIn('extract', sql)
        self.assertIn('year', sql)

    def test_isfinite(self):
        sql = _sr_sql(func.isfinite(column('ts')))
        self.assertIn('IS NOT NULL', sql)

    def test_statement_timestamp(self):
        sql = _sr_sql(func.statement_timestamp())
        self.assertEqual(sql, 'now()')

    def test_timeofday(self):
        sql = _sr_sql(func.timeofday())
        self.assertIn('CAST', sql)
        self.assertIn('now()', sql)

    def test_transaction_timestamp(self):
        sql = _sr_sql(func.transaction_timestamp())
        self.assertEqual(sql, 'now()')


class TestMath(unittest.TestCase):
    def test_cbrt(self):
        sql = _sr_sql(func.cbrt(column('v')))
        self.assertIn('power', sql)
        self.assertIn('1.0 / 3.0', sql)

    def test_log_single_arg(self):
        sql = _sr_sql(func.log(column('v')))
        self.assertIn('log10', sql)

    def test_log_two_args(self):
        sql = _sr_sql(func.log(2, column('v')))
        self.assertIn('log(', sql)

    def test_random(self):
        sql = _sr_sql(func.random())
        self.assertEqual(sql, 'rand()')

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'safe_divide handled by sqlalchemy_functions')
    def test_safe_divide_three_args(self):
        sql = _sr_sql(func.safe_divide(column('a'), column('b'), 0))
        self.assertIn('IF(', sql)

    def test_trunc_one_arg(self):
        sql = _sr_sql(func.trunc(column('v')))
        self.assertIn('truncate(', sql)

    def test_trunc_two_args(self):
        sql = _sr_sql(func.trunc(column('v'), 2))
        self.assertIn('truncate(', sql)

    def test_width_bucket(self):
        sql = _sr_sql(func.width_bucket(column('v'), 0, 100, 10))
        self.assertIn('CASE', sql)
        self.assertIn('floor', sql)

    def test_setseed(self):
        sql = _sr_sql(func.setseed(0.5))
        self.assertEqual(sql, '0')


class TestText(unittest.TestCase):
    def test_btrim_two_args(self):
        sql = _sr_sql(func.btrim(column('v'), 'x'))
        self.assertIn('TRIM(BOTH', sql)

    def test_chr(self):
        sql = _sr_sql(func.chr(65))
        self.assertIn('char(', sql)

    def test_strpos(self):
        sql = _sr_sql(func.strpos(column('s'), 'abc'))
        self.assertIn('locate(', sql)

    def test_to_hex(self):
        sql = _sr_sql(func.to_hex(column('v')))
        self.assertIn('hex(', sql)
        self.assertIn('lower', sql)

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'numericize handled by sqlalchemy_functions')
    def test_numericize(self):
        sql = _sr_sql(func.numericize(column('v')))
        self.assertIn('regexp_replace', sql)
        self.assertIn('DOUBLE', sql)

    def test_integerize_round(self):
        sql = _sr_sql(func.integerize_round(column('v')))
        self.assertIn('round', sql)
        self.assertIn('BIGINT', sql)

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'integerize_truncate handled by sqlalchemy_functions')
    def test_integerize_truncate(self):
        sql = _sr_sql(func.integerize_truncate(column('v')))
        self.assertIn('truncate', sql)
        self.assertIn('BIGINT', sql)

    def test_normalize_whitespace(self):
        sql = _sr_sql(func.normalize_whitespace(column('v')))
        self.assertIn('regexp_replace', sql)

    def test_zfill(self):
        sql = _sr_sql(func.zfill(column('v'), 5))
        self.assertIn('lpad(', sql)
        self.assertIn("'0'", sql)

    def test_slice_string(self):
        sql = _sr_sql(func.slice_string(column('v'), 2, 5))
        self.assertIn('substring(', sql)

    def test_metric_multiply(self):
        sql = _sr_sql(func.metric_multiply(column('v')))
        self.assertIn('CASE', sql)
        self.assertIn('1e6', sql)

    def test_quote_literal(self):
        sql = _sr_sql(func.quote_literal(column('v')))
        self.assertIn('concat(', sql)
        self.assertIn('replace(', sql)


class TestTextConversions(unittest.TestCase):
    def test_text_to_integer(self):
        sql = _sr_sql(func.text_to_integer(column('v')))
        self.assertIn('REGEXP', sql)
        self.assertIn('INT', sql)

    def test_text_to_bigint(self):
        sql = _sr_sql(func.text_to_bigint(column('v')))
        self.assertIn('BIGINT', sql)

    def test_text_to_smallint(self):
        sql = _sr_sql(func.text_to_smallint(column('v')))
        self.assertIn('SMALLINT', sql)

    def test_text_to_numeric(self):
        sql = _sr_sql(func.text_to_numeric(column('v')))
        self.assertIn('DOUBLE', sql)

    def test_text_to_bool(self):
        sql = _sr_sql(func.text_to_bool(column('v')))
        self.assertIn("WHEN 'true' THEN TRUE", sql)
        self.assertIn("WHEN 'false' THEN FALSE", sql)


class TestAggregates(unittest.TestCase):
    def test_first(self):
        sql = _sr_sql(func.first(column('v')))
        self.assertIn('any_value(', sql)

    def test_last(self):
        sql = _sr_sql(func.last(column('v')))
        self.assertIn('any_value(', sql)

    def test_median(self):
        sql = _sr_sql(func.median(column('v')))
        self.assertIn('percentile_approx(', sql)
        self.assertIn('0.5', sql)

    def test_stdev(self):
        sql = _sr_sql(func.stdev(column('v')))
        self.assertIn('stddev(', sql)


class TestUUID(unittest.TestCase):
    def test_gen_random_uuid(self):
        sql = _sr_sql(func.gen_random_uuid())
        self.assertEqual(sql, 'uuid()')


class TestArrays(unittest.TestCase):
    def test_string_to_array(self):
        sql = _sr_sql(func.string_to_array(column('v'), ','))
        self.assertIn('split(', sql)

    def test_array_to_json(self):
        sql = _sr_sql(func.array_to_json(column('v')))
        self.assertIn('to_json(', sql)


class TestJSON(unittest.TestCase):
    def test_json_extract_path(self):
        sql = _sr_sql(func.json_extract_path(column('j'), 'key1', 'key2'))
        self.assertIn('json_query(', sql)

    def test_json_extract_path_text(self):
        sql = _sr_sql(func.json_extract_path_text(column('j'), 'key1'))
        self.assertIn('get_json_string(', sql)

    def test_json_object_keys(self):
        sql = _sr_sql(func.json_object_keys(column('j')))
        self.assertIn('json_keys(', sql)

    def test_json_array_elements(self):
        sql = _sr_sql(func.json_array_elements(column('j')))
        self.assertIn('unnest(', sql)


# =================================================================
# Additional Databend function wrappers
# =================================================================

class TestNumericAdditional(unittest.TestCase):
    def test_div0(self):
        sql = _sr_sql(func.div0(column('a'), column('b')))
        self.assertIn('IF(', sql)
        self.assertIn(', 0,', sql)

    def test_divnull(self):
        sql = _sr_sql(func.divnull(column('a'), column('b')))
        self.assertIn('IF(', sql)
        self.assertIn('NULL', sql)


class TestStringAdditional(unittest.TestCase):
    def test_position(self):
        sql = _sr_sql(func.position('abc', column('s')))
        self.assertIn('locate(', sql)

    def test_oct(self):
        sql = _sr_sql(func.oct(255))
        self.assertIn('conv(', sql)
        self.assertIn('10, 8', sql)

    def test_ord(self):
        sql = _sr_sql(func.ord(column('s')))
        self.assertIn('ascii(', sql)

    def test_regexp_substr(self):
        sql = _sr_sql(func.regexp_substr(column('s'), '[0-9]+'))
        self.assertIn('regexp_extract(', sql)

    def test_regexp_like(self):
        sql = _sr_sql(func.regexp_like(column('s'), '^abc'))
        self.assertIn('REGEXP', sql)

    def test_regexp_split_to_array(self):
        sql = _sr_sql(func.regexp_split_to_array(column('s'), ','))
        self.assertIn('split(', sql)

    def test_length_utf8(self):
        sql = _sr_sql(func.length_utf8(column('s')))
        self.assertIn('char_length(', sql)


class TestDateAdditional(unittest.TestCase):
    def test_today(self):
        sql = _sr_sql(func.today())
        self.assertEqual(sql, 'curdate()')

    def test_tomorrow(self):
        sql = _sr_sql(func.tomorrow())
        self.assertIn('date_add', sql)
        self.assertIn('INTERVAL 1 DAY', sql)

    def test_yesterday(self):
        sql = _sr_sql(func.yesterday())
        self.assertIn('date_sub', sql)

    def test_to_unix_timestamp(self):
        sql = _sr_sql(func.to_unix_timestamp(column('ts')))
        self.assertIn('unix_timestamp(', sql)

    def test_to_yyyymm(self):
        sql = _sr_sql(func.to_yyyymm(column('d')))
        self.assertIn("date_format(", sql)
        self.assertIn("%Y%m", sql)

    def test_to_yyyymmdd(self):
        sql = _sr_sql(func.to_yyyymmdd(column('d')))
        self.assertIn("%Y%m%d", sql)

    def test_to_yyyymmddhh(self):
        sql = _sr_sql(func.to_yyyymmddhh(column('d')))
        self.assertIn("%Y%m%d%H", sql)

    def test_to_yyyymmddhhmmss(self):
        sql = _sr_sql(func.to_yyyymmddhhmmss(column('d')))
        self.assertIn("%Y%m%d%H%i%s", sql)

    def test_convert_timezone(self):
        sql = _sr_sql(func.convert_timezone('US/Eastern', 'UTC', column('ts')))
        self.assertIn('convert_tz(', sql)

    def test_months_between(self):
        sql = _sr_sql(func.months_between(column('d1'), column('d2')))
        self.assertIn('months_diff(', sql)

    def test_to_start_of_year(self):
        sql = _sr_sql(func.to_start_of_year(column('d')))
        self.assertIn("date_trunc('year'", sql)

    def test_to_start_of_month(self):
        sql = _sr_sql(func.to_start_of_month(column('d')))
        self.assertIn("date_trunc('month'", sql)

    def test_to_start_of_day(self):
        sql = _sr_sql(func.to_start_of_day(column('ts')))
        self.assertIn("date_trunc('day'", sql)

    def test_to_start_of_hour(self):
        sql = _sr_sql(func.to_start_of_hour(column('ts')))
        self.assertIn("date_trunc('hour'", sql)

    def test_millennium(self):
        sql = _sr_sql(func.millennium(column('d')))
        self.assertIn('CEIL', sql)
        self.assertIn('1000.0', sql)


class TestConversionAdditional(unittest.TestCase):
    def test_to_boolean(self):
        sql = _sr_sql(func.to_boolean(column('v')))
        self.assertIn('CASE', sql)
        self.assertIn("WHEN 'true' THEN TRUE", sql)

    def test_to_string(self):
        sql = _sr_sql(func.to_string(column('v')))
        self.assertIn('CAST(', sql)
        self.assertIn('VARCHAR', sql)

    def test_to_int8(self):
        sql = _sr_sql(func.to_int8(column('v')))
        self.assertIn('TINYINT', sql)

    def test_to_int32(self):
        sql = _sr_sql(func.to_int32(column('v')))
        self.assertIn('AS INT)', sql)

    def test_to_int64(self):
        sql = _sr_sql(func.to_int64(column('v')))
        self.assertIn('BIGINT', sql)

    def test_to_float32(self):
        sql = _sr_sql(func.to_float32(column('v')))
        self.assertIn('FLOAT', sql)

    def test_to_float64(self):
        sql = _sr_sql(func.to_float64(column('v')))
        self.assertIn('DOUBLE', sql)

    def test_to_uint64(self):
        sql = _sr_sql(func.to_uint64(column('v')))
        self.assertIn('LARGEINT', sql)


class TestConditionalAdditional(unittest.TestCase):
    def test_iff(self):
        sql = _sr_sql(func.iff(column('c'), column('a'), column('b')))
        self.assertIn('IF(', sql)

    def test_nvl(self):
        sql = _sr_sql(func.nvl(column('v'), 0))
        self.assertIn('IFNULL(', sql)

    def test_nvl2(self):
        sql = _sr_sql(func.nvl2(column('v'), 'yes', 'no'))
        self.assertIn('IS NOT NULL', sql)

    def test_decode(self):
        sql = _sr_sql(func.decode(column('v'), 1, 'one', 2, 'two', 'other'))
        self.assertIn('CASE', sql)
        self.assertIn('WHEN', sql)
        self.assertIn('ELSE', sql)

    def test_error_or(self):
        sql = _sr_sql(func.error_or(column('v'), 0))
        self.assertIn('IFNULL(', sql)


class TestAggregateAdditional(unittest.TestCase):
    def test_arg_max(self):
        sql = _sr_sql(func.arg_max(column('a'), column('b')))
        self.assertIn('max_by(', sql)

    def test_arg_min(self):
        sql = _sr_sql(func.arg_min(column('a'), column('b')))
        self.assertIn('min_by(', sql)

    def test_string_agg(self):
        sql = _sr_sql(func.string_agg(column('v'), ','))
        self.assertIn('group_concat(', sql)
        self.assertIn('SEPARATOR', sql)

    def test_listagg(self):
        sql = _sr_sql(func.listagg(column('v'), ','))
        self.assertIn('group_concat(', sql)
        self.assertIn('SEPARATOR', sql)

    def test_quantile_cont(self):
        sql = _sr_sql(func.quantile_cont(0.5, column('v')))
        self.assertIn('percentile_approx(', sql)


class TestJSONAdditional(unittest.TestCase):
    def test_json_path_query(self):
        sql = _sr_sql(func.json_path_query(column('j'), '$.key'))
        self.assertIn('json_query(', sql)

    def test_json_path_exists(self):
        sql = _sr_sql(func.json_path_exists(column('j'), '$.key'))
        self.assertIn('json_exists(', sql)

    def test_json_to_string(self):
        sql = _sr_sql(func.json_to_string(column('j')))
        self.assertIn('json_string(', sql)

    def test_check_json(self):
        sql = _sr_sql(func.check_json(column('s')))
        self.assertIn('parse_json(', sql)


class TestArrayAdditional(unittest.TestCase):
    def test_array_to_string(self):
        sql = _sr_sql(func.array_to_string(column('a'), ','))
        self.assertIn('array_join(', sql)

    def test_array_indexof(self):
        sql = _sr_sql(func.array_indexof(column('a'), 5))
        self.assertIn('array_position(', sql)

    def test_array_compact_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.array_compact(column('a')))

    def test_array_construct(self):
        sql = _sr_sql(func.array_construct(1, 2, 3))
        self.assertIn('[', sql)
        self.assertIn(']', sql)


class TestHashAdditional(unittest.TestCase):
    def test_xxhash64(self):
        sql = _sr_sql(func.xxhash64(column('v')))
        self.assertIn('xx_hash3_64(', sql)

    def test_xxhash32_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.xxhash32(column('v')))

    def test_sha1_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.sha1(column('v')))

    def test_sha_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.sha(column('v')))


class TestUtilityAdditional(unittest.TestCase):
    def test_assume_not_null(self):
        sql = _sr_sql(func.assume_not_null(column('v')))
        # Pass-through: should not wrap in function call
        self.assertNotIn('assume_not_null', sql)

    def test_humanize_number(self):
        sql = _sr_sql(func.humanize_number(column('v')))
        self.assertIn('money_format(', sql)

    def test_humanize_size(self):
        sql = _sr_sql(func.humanize_size(column('v')))
        self.assertIn('format_bytes(', sql)


class TestPgToMysqlFmt(unittest.TestCase):
    """Unit tests for the _pg_to_mysql_fmt helper."""

    def test_basic_date(self):
        self.assertEqual(compat._pg_to_mysql_fmt('YYYY-MM-DD'), '%Y-%m-%d')

    def test_datetime(self):
        self.assertEqual(
            compat._pg_to_mysql_fmt('YYYY-MM-DD HH24:MI:SS'),
            '%Y-%m-%d %H:%i:%s',
        )

    def test_hh24_before_hh(self):
        # HH24 must be matched before HH
        self.assertEqual(compat._pg_to_mysql_fmt('HH24'), '%H')
        self.assertEqual(compat._pg_to_mysql_fmt('HH12'), '%h')
        self.assertEqual(compat._pg_to_mysql_fmt('HH'), '%h')

    def test_no_equivalent_drops(self):
        # TZ, J, Q map to empty string
        self.assertEqual(compat._pg_to_mysql_fmt('TZ'), '')
        self.assertEqual(compat._pg_to_mysql_fmt('J'), '')
        self.assertEqual(compat._pg_to_mysql_fmt('Q'), '')

    def test_passthrough(self):
        # Already MySQL-style should pass through unchanged
        self.assertEqual(compat._pg_to_mysql_fmt('%Y-%m-%d'), '%Y-%m-%d')


class TestMissingCoverage(unittest.TestCase):
    """Smoke tests for wrappers that previously lacked test coverage."""

    # Date/time
    def test_justify_days(self):
        sql = _sr_sql(func.justify_days(column('v')))
        # Pass-through: should emit bare column, not the function name
        self.assertNotIn('justify_days', sql)

    def test_justify_hours(self):
        sql = _sr_sql(func.justify_hours(column('v')))
        self.assertNotIn('justify_hours', sql)

    def test_justify_interval(self):
        sql = _sr_sql(func.justify_interval(column('v')))
        self.assertNotIn('justify_interval', sql)

    def test_timestamp_diff(self):
        sql = _sr_sql(func.timestamp_diff('day', column('ts1'), column('ts2')))
        self.assertIn('timestampdiff(', sql)
        self.assertIn('day', sql)

    def test_to_start_of_quarter(self):
        sql = _sr_sql(func.to_start_of_quarter(column('d')))
        self.assertIn("date_trunc('quarter'", sql)

    def test_to_start_of_week(self):
        sql = _sr_sql(func.to_start_of_week(column('d')))
        self.assertIn("date_trunc('week'", sql)

    def test_to_start_of_minute(self):
        sql = _sr_sql(func.to_start_of_minute(column('ts')))
        self.assertIn("date_trunc('minute'", sql)

    # Text
    def test_regexp_instr(self):
        sql = _sr_sql(func.regexp_instr(column('s'), '[0-9]+'))
        self.assertIn('locate(', sql)
        self.assertIn('regexp_extract(', sql)

    def test_ltrim_two_args(self):
        sql = _sr_sql(func.ltrim(column('s'), 'x'))
        self.assertIn('TRIM(LEADING', sql)

    def test_ltrim_one_arg(self):
        sql = _sr_sql(func.ltrim(column('s')))
        self.assertIn('ltrim(', sql)

    def test_rtrim_two_args(self):
        sql = _sr_sql(func.rtrim(column('s'), 'x'))
        self.assertIn('TRIM(TRAILING', sql)

    def test_rtrim_one_arg(self):
        sql = _sr_sql(func.rtrim(column('s')))
        self.assertIn('rtrim(', sql)

    def test_to_ascii(self):
        sql = _sr_sql(func.to_ascii(column('s')))
        # Pass-through: should not wrap in function call
        self.assertNotIn('to_ascii', sql)

    # Conversion
    def test_to_int16(self):
        sql = _sr_sql(func.to_int16(column('v')))
        self.assertIn('SMALLINT', sql)

    def test_to_uint8(self):
        sql = _sr_sql(func.to_uint8(column('v')))
        self.assertIn('SMALLINT', sql)

    def test_to_uint16(self):
        sql = _sr_sql(func.to_uint16(column('v')))
        self.assertIn('INT', sql)

    def test_to_uint32(self):
        sql = _sr_sql(func.to_uint32(column('v')))
        self.assertIn('BIGINT', sql)

    def test_to_varchar(self):
        sql = _sr_sql(func.to_varchar(column('v')))
        self.assertIn('CAST(', sql)
        self.assertIn('VARCHAR', sql)

    # Conditional
    def test_is_not_error(self):
        sql = _sr_sql(func.is_not_error(column('v')))
        self.assertEqual(sql, 'TRUE')

    # JSON
    def test_json_path_query_first(self):
        sql = _sr_sql(func.json_path_query_first(column('j'), '$.key'))
        self.assertIn('json_query(', sql)

    # Aggregate
    def test_quantile_disc(self):
        sql = _sr_sql(func.quantile_disc(0.75, column('v')))
        self.assertIn('percentile_approx(', sql)

    # to_boolean extras
    def test_to_boolean_has_on_off(self):
        sql = _sr_sql(func.to_boolean(column('v')))
        self.assertIn("'on'", sql)
        self.assertIn("'off'", sql)

    # text_to_bool uses shared helper
    def test_text_to_bool_no_on_off(self):
        sql = _sr_sql(func.text_to_bool(column('v')))
        self.assertNotIn("'on'", sql)
        self.assertNotIn("'off'", sql)


class TestDefaultDialectUnchanged(unittest.TestCase):
    """Verify that non-StarRocks dialects still emit the original
    function name (i.e. the GenericFunction registration does not
    break PostgreSQL compilation)."""

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'safe_divide PG handler from sqlalchemy_functions')
    def test_safe_divide_pg(self):
        sql = _pg_sql(func.safe_divide(column('a'), column('b'), 0))
        self.assertIn('safe_divide(', sql)

    @unittest.skipIf(_HAS_SQLA_FUNCS, 'numericize PG handler from sqlalchemy_functions')
    def test_numericize_pg(self):
        sql = _pg_sql(func.numericize(column('v')))
        self.assertIn('numericize(', sql)

    def test_gen_random_uuid_pg(self):
        sql = _pg_sql(func.gen_random_uuid())
        self.assertIn('gen_random_uuid(', sql)

    def test_div0_pg(self):
        sql = _pg_sql(func.div0(column('a'), column('b')))
        self.assertIn('div0(', sql)

    def test_today_pg(self):
        sql = _pg_sql(func.today())
        self.assertIn('today(', sql)

    def test_arg_max_pg(self):
        sql = _pg_sql(func.arg_max(column('a'), column('b')))
        self.assertIn('arg_max(', sql)


# =================================================================
# Second-pass wrappers
# =================================================================

class TestDateExtraction(unittest.TestCase):
    def test_to_day_of_month(self):
        sql = _sr_sql(func.to_day_of_month(column('d')))
        self.assertIn('dayofmonth(', sql)

    def test_to_day_of_week(self):
        sql = _sr_sql(func.to_day_of_week(column('d')))
        self.assertIn('dayofweek(', sql)
        # Verify Monday-based compensation formula
        self.assertIn('+ 5', sql)
        self.assertIn('+ 1', sql)

    def test_to_day_of_year(self):
        sql = _sr_sql(func.to_day_of_year(column('d')))
        self.assertIn('dayofyear(', sql)

    def test_to_hour(self):
        sql = _sr_sql(func.to_hour(column('ts')))
        self.assertIn('hour(', sql)

    def test_to_minute(self):
        sql = _sr_sql(func.to_minute(column('ts')))
        self.assertIn('minute(', sql)

    def test_to_second(self):
        sql = _sr_sql(func.to_second(column('ts')))
        self.assertIn('second(', sql)

    def test_to_month(self):
        sql = _sr_sql(func.to_month(column('d')))
        self.assertIn('month(', sql)

    def test_to_quarter(self):
        sql = _sr_sql(func.to_quarter(column('d')))
        self.assertIn('quarter(', sql)

    def test_to_year(self):
        sql = _sr_sql(func.to_year(column('d')))
        self.assertIn('year(', sql)

    def test_to_week_of_year(self):
        sql = _sr_sql(func.to_week_of_year(column('d')))
        self.assertIn('weekofyear(', sql)


class TestDateRounding(unittest.TestCase):
    def test_to_start_of_second(self):
        sql = _sr_sql(func.to_start_of_second(column('ts')))
        self.assertIn("date_trunc('second'", sql)

    def test_to_start_of_five_minutes(self):
        sql = _sr_sql(func.to_start_of_five_minutes(column('ts')))
        self.assertIn('from_unixtime', sql)
        self.assertIn('300', sql)

    def test_to_start_of_ten_minutes(self):
        sql = _sr_sql(func.to_start_of_ten_minutes(column('ts')))
        self.assertIn('from_unixtime', sql)
        self.assertIn('600', sql)

    def test_to_start_of_fifteen_minutes(self):
        sql = _sr_sql(func.to_start_of_fifteen_minutes(column('ts')))
        self.assertIn('from_unixtime', sql)
        self.assertIn('900', sql)


class TestDateOther(unittest.TestCase):
    def test_add_months(self):
        sql = _sr_sql(func.add_months(column('d'), 3))
        self.assertIn('months_add(', sql)

    def test_time_slot(self):
        sql = _sr_sql(func.time_slot(column('ts')))
        self.assertIn('from_unixtime', sql)
        self.assertIn('1800', sql)

    def test_to_datetime_one_arg(self):
        sql = _sr_sql(func.to_datetime(column('s')))
        self.assertIn('CAST(', sql)
        self.assertIn('DATETIME', sql)

    def test_to_datetime_two_args(self):
        sql = _sr_sql(func.to_datetime(column('s'), 'YYYY-MM-DD'))
        self.assertIn('str_to_date', sql)

    def test_try_to_timestamp(self):
        sql = _sr_sql(func.try_to_timestamp(column('s')))
        self.assertIn('CAST(', sql)
        self.assertIn('DATETIME', sql)

    def test_try_to_datetime(self):
        sql = _sr_sql(func.try_to_datetime(column('s')))
        self.assertIn('CAST(', sql)
        self.assertIn('DATETIME', sql)

    def test_next_day(self):
        sql = _sr_sql(func.next_day(column('d'), 'Monday'))
        self.assertIn('next_day(', sql)

    def test_previous_day(self):
        sql = _sr_sql(func.previous_day(column('d'), 'Monday'))
        self.assertIn('previous_day(', sql)


class TestStringAliases(unittest.TestCase):
    def test_mid_three_args(self):
        sql = _sr_sql(func.mid(column('s'), 1, 5))
        self.assertIn('substring(', sql)

    def test_trim_both(self):
        sql = _sr_sql(func.trim_both(column('s'), 'x'))
        self.assertIn('TRIM(BOTH', sql)

    def test_trim_leading(self):
        sql = _sr_sql(func.trim_leading(column('s'), 'x'))
        self.assertIn('TRIM(LEADING', sql)

    def test_trim_trailing(self):
        sql = _sr_sql(func.trim_trailing(column('s'), 'x'))
        self.assertIn('TRIM(TRAILING', sql)

    def test_trim_both_one_arg(self):
        sql = _sr_sql(func.trim_both(column('s')))
        self.assertIn('TRIM(', sql)

    def test_trim_leading_one_arg(self):
        sql = _sr_sql(func.trim_leading(column('s')))
        self.assertIn('ltrim(', sql)

    def test_trim_trailing_one_arg(self):
        sql = _sr_sql(func.trim_trailing(column('s')))
        self.assertIn('rtrim(', sql)


class TestNumericAdditional2(unittest.TestCase):
    def test_intdiv(self):
        sql = _sr_sql(func.intdiv(column('a'), column('b')))
        self.assertIn('floor(', sql)


class TestConversionAdditional2(unittest.TestCase):
    def test_to_text(self):
        sql = _sr_sql(func.to_text(column('v')))
        self.assertIn('CAST(', sql)
        self.assertIn('VARCHAR', sql)

    def test_to_decimal_three_args(self):
        sql = _sr_sql(func.to_decimal(column('v'), 10, 2))
        self.assertIn('CAST(', sql)
        self.assertIn('DECIMAL(', sql)

    def test_to_decimal_one_arg(self):
        sql = _sr_sql(func.to_decimal(column('v')))
        self.assertIn('CAST(', sql)
        self.assertIn('DECIMAL', sql)


class TestConditionalAdditional2(unittest.TestCase):
    def test_is_error(self):
        sql = _sr_sql(func.is_error(column('v')))
        self.assertEqual(sql, 'FALSE')

    def test_greatest_ignore_nulls_two(self):
        sql = _sr_sql(func.greatest_ignore_nulls(column('a'), column('b')))
        self.assertIn('CASE', sql)
        self.assertIn('IS NULL', sql)

    def test_greatest_ignore_nulls_three(self):
        sql = _sr_sql(func.greatest_ignore_nulls(column('a'), column('b'), column('c')))
        self.assertIn('CASE', sql)

    def test_least_ignore_nulls_two(self):
        sql = _sr_sql(func.least_ignore_nulls(column('a'), column('b')))
        self.assertIn('CASE', sql)
        self.assertIn('IS NULL', sql)

    def test_least_ignore_nulls_three(self):
        sql = _sr_sql(func.least_ignore_nulls(column('a'), column('b'), column('c')))
        self.assertIn('CASE', sql)


class TestAggregateAdditional2(unittest.TestCase):
    def test_median_tdigest(self):
        sql = _sr_sql(func.median_tdigest(column('v')))
        self.assertIn('percentile_approx(', sql)
        self.assertIn('0.5', sql)

    def test_quantile_tdigest(self):
        sql = _sr_sql(func.quantile_tdigest(0.9, column('v')))
        self.assertIn('percentile_approx(', sql)

    def test_quantile_tdigest_weighted(self):
        sql = _sr_sql(func.quantile_tdigest_weighted(0.5, column('v'), column('w')))
        self.assertIn('percentile_approx(', sql)

    def test_mode_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.mode(column('v')))

    def test_kurtosis_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.kurtosis(column('v')))

    def test_skewness_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.skewness(column('v')))

    def test_json_array_agg(self):
        sql = _sr_sql(func.json_array_agg(column('v')))
        self.assertIn('to_json(', sql)
        self.assertIn('array_agg(', sql)

    def test_json_object_agg(self):
        sql = _sr_sql(func.json_object_agg(column('k'), column('v')))
        self.assertIn('to_json(', sql)
        self.assertIn('map_agg(', sql)


class TestArrayAdditional2(unittest.TestCase):
    def test_array_size(self):
        sql = _sr_sql(func.array_size(column('a')))
        self.assertIn('array_length(', sql)

    def test_array_unique(self):
        sql = _sr_sql(func.array_unique(column('a')))
        self.assertIn('array_distinct(', sql)

    def test_array_intersection(self):
        sql = _sr_sql(func.array_intersection(column('a'), column('b')))
        self.assertIn('array_intersect(', sql)

    def test_array_overlap(self):
        sql = _sr_sql(func.array_overlap(column('a'), column('b')))
        self.assertIn('arrays_overlap(', sql)

    def test_array_transform(self):
        sql = _sr_sql(func.array_transform(column('a'), column('f')))
        self.assertIn('array_map(', sql)

    def test_array_reverse(self):
        sql = _sr_sql(func.array_reverse(column('a')))
        self.assertIn('reverse(', sql)

    def test_contains(self):
        sql = _sr_sql(func.contains(column('a'), 5))
        self.assertIn('array_contains(', sql)

    def test_array_get(self):
        sql = _sr_sql(func.array_get(column('a'), 1))
        self.assertIn('element_at(', sql)

    def test_array_except(self):
        sql = _sr_sql(func.array_except(column('a'), column('b')))
        self.assertIn('array_filter(', sql)
        self.assertIn('NOT array_contains(', sql)

    def test_array_prepend(self):
        sql = _sr_sql(func.array_prepend(column('a'), 1))
        self.assertIn('array_concat(', sql)

    def test_array_remove_first(self):
        sql = _sr_sql(func.array_remove_first(column('a')))
        self.assertIn('array_slice(', sql)

    def test_array_remove_last(self):
        sql = _sr_sql(func.array_remove_last(column('a')))
        self.assertIn('array_slice(', sql)
        self.assertIn('array_length(', sql)

    def test_array_count_two_args(self):
        sql = _sr_sql(func.array_count(column('a'), 5))
        self.assertIn('array_filter(', sql)
        self.assertIn('array_length(', sql)

    def test_array_count_one_arg(self):
        sql = _sr_sql(func.array_count(column('a')))
        self.assertIn('IS NOT NULL', sql)

    def test_array_generate_range(self):
        sql = _sr_sql(func.array_generate_range(1, 10, 2))
        self.assertIn('array_generate(', sql)


class TestJSONAdditional2(unittest.TestCase):
    def test_json_path_query_array(self):
        sql = _sr_sql(func.json_path_query_array(column('j'), '$.arr'))
        self.assertIn('json_query(', sql)

    def test_json_path_match(self):
        sql = _sr_sql(func.json_path_match(column('j'), '$.key'))
        self.assertIn('json_exists(', sql)

    def test_json_pretty(self):
        sql = _sr_sql(func.json_pretty(column('j')))
        self.assertIn('CAST(', sql)
        self.assertIn('VARCHAR', sql)

    def test_get(self):
        sql = _sr_sql(func.get(column('j'), 'key'))
        self.assertIn('json_query(', sql)

    def test_get_path(self):
        sql = _sr_sql(func.get_path(column('j'), '$.a.b'))
        self.assertIn('json_query(', sql)


class TestIPFunctions(unittest.TestCase):
    def test_ipv4_string_to_num(self):
        sql = _sr_sql(func.ipv4_string_to_num(column('ip')))
        self.assertIn('inet_aton(', sql)

    def test_ipv4_num_to_string(self):
        sql = _sr_sql(func.ipv4_num_to_string(column('n')))
        self.assertIn('inet_ntoa(', sql)

    def test_try_inet_aton(self):
        sql = _sr_sql(func.try_inet_aton(column('ip')))
        self.assertIn('inet_aton(', sql)

    def test_try_inet_ntoa(self):
        sql = _sr_sql(func.try_inet_ntoa(column('n')))
        self.assertIn('inet_ntoa(', sql)

    def test_try_ipv4_string_to_num(self):
        sql = _sr_sql(func.try_ipv4_string_to_num(column('ip')))
        self.assertIn('inet_aton(', sql)

    def test_try_ipv4_num_to_string(self):
        sql = _sr_sql(func.try_ipv4_num_to_string(column('n')))
        self.assertIn('inet_ntoa(', sql)


class TestMapFunctions(unittest.TestCase):
    def test_map_cat(self):
        sql = _sr_sql(func.map_cat(column('m1'), column('m2')))
        self.assertIn('map_concat(', sql)

    def test_map_contains_key(self):
        sql = _sr_sql(func.map_contains_key(column('m'), 'k'))
        self.assertIn('array_contains(', sql)
        self.assertIn('map_keys(', sql)


class TestHashAdditional2(unittest.TestCase):
    def test_siphash_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.siphash(column('v')))

    def test_siphash64_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.siphash64(column('v')))

    def test_blake3_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.blake3(column('v')))

    def test_city64withseed_raises(self):
        with self.assertRaises(CompileError):
            _sr_sql(func.city64withseed(column('v'), 42))


class TestBitmapFunctions(unittest.TestCase):
    def test_bitmap_and_not(self):
        sql = _sr_sql(func.bitmap_and_not(column('a'), column('b')))
        self.assertIn('bitmap_andnot(', sql)

    def test_bitmap_cardinality(self):
        sql = _sr_sql(func.bitmap_cardinality(column('bm')))
        self.assertIn('bitmap_count(', sql)

    def test_build_bitmap(self):
        sql = _sr_sql(func.build_bitmap(column('v')))
        self.assertIn('bitmap_from_string(', sql)


class TestVariantFunctions(unittest.TestCase):
    def test_to_variant(self):
        sql = _sr_sql(func.to_variant(column('v')))
        self.assertIn('parse_json(', sql)
        self.assertIn('VARCHAR', sql)

    def test_remove_nullable(self):
        sql = _sr_sql(func.remove_nullable(column('v')))
        self.assertNotIn('remove_nullable', sql)

    def test_to_nullable(self):
        sql = _sr_sql(func.to_nullable(column('v')))
        self.assertNotIn('to_nullable', sql)


class TestDefaultDialectUnchanged2(unittest.TestCase):
    """Verify second-pass wrappers don't break PostgreSQL compilation."""

    def test_to_day_of_month_pg(self):
        sql = _pg_sql(func.to_day_of_month(column('d')))
        self.assertIn('to_day_of_month(', sql)

    def test_intdiv_pg(self):
        sql = _pg_sql(func.intdiv(column('a'), column('b')))
        self.assertIn('intdiv(', sql)

    def test_array_size_pg(self):
        sql = _pg_sql(func.array_size(column('a')))
        self.assertIn('array_size(', sql)

    def test_map_cat_pg(self):
        sql = _pg_sql(func.map_cat(column('m1'), column('m2')))
        self.assertIn('map_cat(', sql)

    def test_ipv4_string_to_num_pg(self):
        sql = _pg_sql(func.ipv4_string_to_num(column('ip')))
        self.assertIn('ipv4_string_to_num(', sql)


if __name__ == '__main__':
    unittest.main()
