# coding=utf-8
"""Spark SQL spellings of the portable functions, as the Alteryx converter emits them.

Each was a statement Spark 4.0 (ANSI on) rejected, or answered differently from
DuckDB, when the converter's isolation suite was replayed on it.
"""

import pytest
import sqlalchemy
from databricks.sqlalchemy import DatabricksDialect
from sqlalchemy import column, func

from plaidcloud.utilities import sqlalchemy_functions as sf
from plaidcloud.utilities.sql_expression import eval_expression, get_table_rep

TABLE = get_table_rep('t', [
    {'source': 'A', 'dtype': 'Float64'},
    {'source': 'S', 'dtype': 'String'},
    {'source': 'D', 'dtype': 'DateTime'},
    {'source': 'D2', 'dtype': 'DateTime'},
], 'sc')


def databricks(expression):
    return str(eval_expression(expression, None, [TABLE]).compile(
        dialect=DatabricksDialect(), compile_kwargs={'literal_binds': True}))


@pytest.mark.parametrize('expression, expected', [
    ("func.today()", 'current_date()'),
    ("func.modulo(get_column(table, 'A'), 3)", 'mod(sc.t.`A`, nullif(3, 0))'),
    ("func.ord(get_column(table, 'S'))", 'ascii(sc.t.`S`)'),
    ("func.to_year(get_column(table, 'D'))", 'year(sc.t.`D`)'),
    ("func.to_day_of_month(get_column(table, 'D'))", 'day(sc.t.`D`)'),
    ("func.add_days(get_column(table, 'D'), 3)", 'timestampadd(DAY, 3, sc.t.`D`)'),
    ("func.add_months(get_column(table, 'D'), 2)", 'timestampadd(MONTH, 2, sc.t.`D`)'),
    ("func.date_diff('day', get_column(table, 'D2'), get_column(table, 'D'))",
     'timestampdiff(DAY, sc.t.`D2`, sc.t.`D`)'),
    ("func.to_string(get_column(table, 'A'))", 'CAST(sc.t.`A` AS STRING)'),
    ("func.try_to_float64(get_column(table, 'S'))", 'try_cast(sc.t.`S` AS DOUBLE)'),
    ("func.starts_with(get_column(table, 'S'), 'a')", "startswith(sc.t.`S`, 'a')"),
    ("func.ends_with(get_column(table, 'S'), 'a')", "endswith(sc.t.`S`, 'a')"),
    ("func.regexp_substr(get_column(table, 'S'), 'x+')", "regexp_extract(sc.t.`S`, 'x+', 0)"),
])
def test_function_is_spelled_the_way_spark_reads_it(expression, expected):
    assert databricks(expression) == expected


@pytest.mark.parametrize('mysql_format, java_pattern', [
    ('%Y-%m-%d %H:%i:%S', 'yyyy-MM-dd HH:mm:ss'),
    ('%M %e, %Y at %h:%i %p', "MMMM d, yyyy 'at' hh:mm a"),
    ('%d/%m/%y', 'dd/MM/yy'),
])
def test_mysql_date_format_becomes_a_java_pattern(mysql_format, java_pattern):
    sql = databricks(f"func.date_format(get_column(table, 'D'), {mysql_format!r})")
    assert sql == "date_format(sc.t.`D`, '" + java_pattern.replace("'", "\\'") + "')"


@pytest.mark.parametrize('postgres_format, java_pattern', [
    ('YYYY/MM/DD', 'yyyy/MM/dd'),
    ('Mon DD, YYYY', 'MMM dd, yyyy'),
    ('HH24:MI:SS', 'HH:mm:ss'),
])
def test_postgres_date_format_becomes_a_java_pattern_in_to_char(postgres_format, java_pattern):
    sql = databricks(f"func.to_char(get_column(table, 'D'), {postgres_format!r})")
    assert sql == f"date_format(sc.t.`D`, '{java_pattern}')"


def test_parse_reads_a_one_digit_field_and_is_null_on_garbage():
    sql = databricks("func.to_timestamp(get_column(table, 'S'), 'MM/DD/YYYY')")
    assert sql == "try_to_timestamp(nullif(trim(CAST(sc.t.`S` AS STRING)), ''), 'M/d/yyyy')"


def test_to_date_is_the_date_of_the_parse():
    sql = databricks("func.to_date(get_column(table, 'S'))")
    assert sql == "CAST(try_to_timestamp(nullif(trim(CAST(sc.t.`S` AS STRING)), '')) AS DATE)"


@pytest.mark.parametrize('sql_type', [sqlalchemy.Text, sqlalchemy.TEXT])
def test_text_is_string_because_spark_has_no_text_type(sql_type):
    assert sql_type().compile(dialect=DatabricksDialect()) == 'STRING'


def test_cast_is_try_cast_so_a_malformed_cell_is_null_not_a_failed_step():
    sql = str(sqlalchemy.cast(column('x'), sqlalchemy.Integer).compile(dialect=DatabricksDialect()))
    assert sql == 'try_cast(x AS INT)'


def test_a_date_diff_unit_spark_has_no_name_for_refuses():
    with pytest.raises(sqlalchemy.exc.CompileError):
        databricks("func.date_diff('fortnight', get_column(table, 'D2'), get_column(table, 'D'))")


def test_regexp_substr_with_an_occurrence_refuses_rather_than_dropping_it():
    with pytest.raises(sqlalchemy.exc.CompileError):
        databricks("func.regexp_substr(get_column(table, 'S'), 'x', 1, 2)")


def test_other_warehouses_are_untouched():
    from sqlalchemy.dialects import postgresql
    assert str(func.starts_with(column('a'), 'x').compile(dialect=postgresql.dialect())) == 'starts_with(a, %(starts_with_1)s)'
    assert sf.starts_with.name == 'starts_with'


@pytest.mark.parametrize('postgres_format, java_pattern', [
    ('YYYYMMDD', 'yyyyMMdd'),
    ('YYYYMMDDHH24MISS', 'yyyyMMddHHmmss'),
    ('DD-MM-YYYY HH24:MI', 'd-M-yyyy H:m'),
])
def test_a_field_next_to_another_is_fixed_width_so_compact_dates_parse(postgres_format, java_pattern):
    sql = databricks(f"func.to_timestamp(get_column(table, 'S'), {postgres_format!r})")
    assert sql == f"try_to_timestamp(nullif(trim(CAST(sc.t.`S` AS STRING)), ''), '{java_pattern}')"


def test_week_specifiers_java_has_no_letter_for_are_computed():
    sql = databricks("func.date_format(get_column(table, 'D'), '%U/%w')")
    assert sql == (
        "concat(lpad(CAST(floor((dayofyear(sc.t.`D`) + 6 - (dayofweek(sc.t.`D`) - 1)) / 7) AS STRING), 2, '0'), "
        "date_format(sc.t.`D`, '/'), CAST(dayofweek(sc.t.`D`) - 1 AS STRING))"
    )


@pytest.mark.parametrize('specifier', ['%u', '%Q'])
def test_a_specifier_with_no_spelling_refuses_naming_it(specifier):
    with pytest.raises(sqlalchemy.exc.CompileError, match=specifier):
        databricks(f"func.date_format(get_column(table, 'D'), 'Week {specifier}')")


def test_float_is_the_64_bit_double():
    sql = str(sqlalchemy.cast(column('x'), sqlalchemy.Float).compile(dialect=DatabricksDialect()))
    assert sql == 'try_cast(x AS DOUBLE)'
