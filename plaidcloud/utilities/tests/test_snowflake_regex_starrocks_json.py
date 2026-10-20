# coding=utf-8
"""Snowflake spellings of the regexp_* functions and StarRocks spellings of the
Databend JSON / time zone functions the Savant converter emits.

Snowflake reads POSIX ERE (no inline flags, non-capturing groups or lazy
quantifiers), so a pattern carrying one is rewritten; every other call, and
every other dialect, renders exactly as before.
"""

import pytest
import sqlalchemy
from sqlalchemy import column, func
from sqlalchemy.dialects import registry
from sqlalchemy.exc import CompileError

from plaidcloud.utilities import sqlalchemy_functions as sf  # noqa: F401 -- registers the compiles

DIALECTS = {name: registry.load(name)() for name in ('databend', 'starrocks', 'snowflake', 'databricks')}
S = column('s', sqlalchemy.Text)


def sql(expression, dialect):
    return str(expression.compile(dialect=DIALECTS[dialect], compile_kwargs={'literal_binds': True}))


def default(name, *args):
    """The rendering every dialect without a rewrite gives: the plain function."""
    return sql(sqlalchemy.sql.functions.Function(name, *args), 'databend')


# --- Snowflake regexp ----------------------------------------------------------------------------------------

@pytest.mark.parametrize('expression', [
    func.regexp_instr(S, r'\d+'),
    func.regexp_substr(S, r'(\d+)-x'),
    func.regexp_replace(S, r'([0-9]+)', r'0\1'),
    # Arities the rewrite does not cover pass through even with a construct in the pattern.
    func.regexp_replace(S, r'(?i)a', 'b', 'g'),
    func.regexp_instr(S, r'(?i)a', 2),
    func.regexp_substr(S, r'(?i)a', 1, 2),
    # A lazy quantifier alone in an extent form: unchanged, Snowflake answers for it as it did.
    func.regexp_substr(S, 'a.*?b'),
    func.regexp_replace(S, 'a+?', ''),
    # A per-row pattern with no literal (?i) in it.
    func.regexp_instr(S, S),
])
def test_snowflake_call_without_a_construct_keeps_its_rendering(expression):
    assert sql(expression, 'snowflake') == default(expression.name, *expression.clauses)


@pytest.mark.parametrize('expression, expected', [
    (func.regexp_instr(S, r'(?i)\d+$'), r"REGEXP_INSTR(s, '\\d+$', 1, 1, 0, 'i')"),
    (func.regexp_instr(S, r'(?si)a'), "REGEXP_INSTR(s, 'a', 1, 1, 0, 'is')"),
    (func.regexp_substr(S, '(?i)ab'), "REGEXP_SUBSTR(s, 'ab', 1, 1, 'i')"),
    (func.regexp_replace(S, '(?m)^a', 'b'), "REGEXP_REPLACE(s, '^a', 'b', 1, 0, 'm')"),
])
def test_leading_inline_flags_become_the_parameters_argument(expression, expected):
    assert sql(expression, 'snowflake') == expected


def test_dotall_group_rewrites_only_its_own_dots():
    # The dot outside the group keeps its meaning; a dot in a class is a dot.
    assert sql(func.regexp_instr(S, r'.(?s:a.[.]b)'), 'snowflake') == \
        r"REGEXP_INSTR(s, '.(a[\\s\\S][.]b)', 1, 1, 0, 'c')"


@pytest.mark.parametrize('pattern, group, expected_pattern, expected_group', [
    (r'(?:x)(\d+)', 1, r'(x)(\\d+)', 2),
    (r'(a)(?:b)(c)', 2, '(a)(b)(c)', 3),
    (r'(a)(?:b)(c)', 1, '(a)(b)(c)', 1),
    (r'(?:(a)(?:b))(c)', 1, '((a)(b))(c)', 2),
    (r'(?:(a)(?:b))(c)', 2, '((a)(b))(c)', 4),
    (r'(?s:(a).)(b)', 2, r'((a)[\\s\\S])(b)', 3),
    (r'(?<year>\d{4})(?:-)(?P<month>\d\d)', 2, r'(\\d{4})(-)(\\d\\d)', 3),
])
def test_non_capturing_groups_become_groups_and_the_extract_group_follows(pattern, group, expected_pattern,
                                                                          expected_group):
    assert sql(func.regexp_extract(S, pattern, group), 'snowflake') == (
        f"IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, '{expected_pattern}', 1, 1, 'ce', {expected_group}), ''))")


def test_regexp_extract_whole_match_has_no_group_argument():
    assert sql(func.regexp_extract(S, 'a', 0), 'snowflake') == \
        "IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, 'a', 1, 1, 'c'), ''))"


def test_regexp_extract_no_match_is_empty_text_and_a_null_subject_null():
    # Databend's regexp_extract answers '' for no match; Snowflake's REGEXP_SUBSTR answers NULL.
    assert sql(func.regexp_extract(S, '(?i)(a)', 1), 'snowflake') == \
        "IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, '(a)', 1, 1, 'ie', 1), ''))"


def test_regexp_extract_per_row_pattern_reads_its_case_flag_at_run_time():
    pattern = func.concat('(?i)', S)
    assert sql(func.regexp_extract(S, pattern, 1), 'snowflake') == (
        "IFF(s IS NULL OR concat('(?i)', s) IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, "
        "CASE WHEN startswith(concat('(?i)', s), '(?i)') THEN substr(concat('(?i)', s), 5) ELSE concat('(?i)', s) END, "
        "1, 1, CASE WHEN startswith(concat('(?i)', s), '(?i)') THEN 'ie' ELSE 'ce' END, 1), ''))")


def test_per_row_pattern_with_a_literal_case_flag_is_read_at_run_time():
    assert sql(func.regexp_instr(S, func.concat('(?i)', S)), 'snowflake') == (
        "REGEXP_INSTR(s, CASE WHEN startswith(concat('(?i)', s), '(?i)') THEN substr(concat('(?i)', s), 5) "
        "ELSE concat('(?i)', s) END, 1, 1, 0, CASE WHEN startswith(concat('(?i)', s), '(?i)') THEN 'i' ELSE 'c' END)")


@pytest.mark.parametrize('pattern, replacement, expected_pattern, expected_replacement', [
    (r'(?:a)(b)', r'<\1>', '(a)(b)', r'<\\2>'),
    # An escaped backslash before a digit is a backslash and a digit, not a reference.
    (r'(?:a)(b)', r'\\1\1', '(a)(b)', r'\\\\1\\2'),
    # A literal \n naming a group the pattern does not have is left alone.
    (r'(?:a)(b)', r'\5', '(a)(b)', r'\\5'),
])
def test_replacement_references_follow_the_renumbered_groups(pattern, replacement, expected_pattern,
                                                             expected_replacement):
    assert sql(func.regexp_replace(S, pattern, replacement), 'snowflake') == \
        f"REGEXP_REPLACE(s, '{expected_pattern}', '{expected_replacement}', 1, 0, 'c')"


def test_replacement_reference_past_nine_is_refused():
    pattern = '(?:a)' * 9 + '(b)'
    with pytest.raises(CompileError, match='above 9'):
        sql(func.regexp_replace(S, pattern, r'\1'), 'snowflake')


def test_per_row_replacement_cannot_follow_renumbered_groups():
    with pytest.raises(CompileError, match='replacement must be a literal'):
        sql(func.regexp_replace(S, '(?:a)(b)', S), 'snowflake')


@pytest.mark.parametrize('pattern, expected', [
    (r'\x{41}\x42', 'AB'),
    (r'a\x{2E}', r'a\\.'),           # a metacharacter stays literal
    (r'[\x{5D}\x{2E}]', '[\\\\].]'),  # in a class only [ ] \ ^ - need escaping
    (r'[\x{5E}a]', '[\\\\^a]'),
    (r'[a\x{2D}z]', '[a\\\\-z]'),
    (r'\x{5C}\x5C', '\\\\\\\\\\\\\\\\'),  # outside a class: an escaped backslash twice
    (r'[\x{5C}\x5C]', '[\\\\\\\\\\\\\\\\]'),  # inside a class: still escaped
    (r'[\x5B:alpha:]]', '[\\\\[:alpha:]]'),
    (r'\x{7C}', r'\\|'),            # | and { are metacharacters
    (r'\x{7B}', r'\\{'),
])
def test_hex_escapes_become_the_character(pattern, expected):
    assert sql(func.regexp_instr(S, pattern), 'snowflake') == f"REGEXP_INSTR(s, '{expected}', 1, 1, 0, 'c')"


@pytest.mark.parametrize('pattern, expected', [
    ('a.*?b', 'a.*b'), ('a+?', 'a+'), ('a??', 'a?'), ('a{2,5}?', 'a{2,5}'),
])
def test_match_position_drops_lazy_quantifiers(pattern, expected):
    # Where the leftmost match starts does not depend on how far it extends.
    assert sql(func.regexp_instr(S, pattern), 'snowflake') == f"REGEXP_INSTR(s, '{expected}', 1, 1, 0, 'c')"


@pytest.mark.parametrize('expression', [
    func.regexp_extract(S, r'(?i)^((?s:.*?))(?:\d+$)', 1),
    func.regexp_extract(S, 'a(.+?)b', 1),
])
def test_regexp_extract_with_a_lazy_quantifier_is_refused(expression):
    # Snowflake has no regexp_extract, so there is no earlier rendering to keep.
    with pytest.raises(CompileError, match='non-greedy'):
        sql(expression, 'snowflake')


@pytest.mark.parametrize('expression', [
    func.regexp_substr(S, '(?i)a.*?b'),
    # The Alteryx Regex parse form: a lazy prefix that picks the match.
    func.regexp_replace(S, r'(?i)^[\s\S]*?(?:-(\d{4}))[\s\S]*$', r'\1'),
])
def test_extent_form_with_a_lazy_quantifier_keeps_its_rendering(expression):
    # Never refused for other callers: the call renders exactly as before.
    assert sql(expression, 'snowflake') == default(expression.name, *expression.clauses)


def test_quantifier_braces_that_are_text_are_not_lazy():
    assert sql(func.regexp_extract(S, 'a}?', 0), 'snowflake') == \
        "IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, 'a}?', 1, 1, 'c'), ''))"


def test_snowflake_literal_doubles_each_backslash_once():
    # snowflake-sqlalchemy's literal processor escapes backslashes; the rewrite must not escape them again.
    assert sql(func.regexp_substr(S, r'(?i)\d\\'), 'snowflake') == r"REGEXP_SUBSTR(s, '\\d\\\\', 1, 1, 'i')"
    compiled = func.regexp_substr(S, r'(?i)\d').compile(dialect=DIALECTS['snowflake'])
    assert list(compiled.params.values()) == [r'\d', 'i']


@pytest.mark.parametrize('dialect', ['databend', 'starrocks', 'databricks'])
@pytest.mark.parametrize('name, args', [
    ('regexp_extract', (S, r'(?i)(?:x)(\d+)', 1)),
    ('regexp_replace', (S, r'(?i)(?:a)(b).*?', r'\1')),
])
def test_other_dialects_keep_the_default_rendering(dialect, name, args):
    assert sql(getattr(func, name)(*args), dialect) == sql(sqlalchemy.sql.functions.Function(name, *args), dialect)


@pytest.mark.parametrize('dialect', ['databend', 'databricks'])
@pytest.mark.parametrize('name', ['regexp_instr', 'regexp_substr'])
def test_other_dialects_keep_instr_and_substr(dialect, name):
    args = (S, r'(?i)(?:a).*?')
    assert sql(getattr(func, name)(*args), dialect) == sql(sqlalchemy.sql.functions.Function(name, *args), dialect)


# --- StarRocks ------------------------------------------------------------------------------------------------

def test_starrocks_regexp_instr_is_the_match_position():
    # '12 abc 12' with '\d+$' is 8: the old locate(<matched text>) answered 1, where '12' first occurs.
    assert sql(func.regexp_instr(S, r'\d+$'), 'starrocks') == (
        r"CASE WHEN (regexp(s, '\\d+$') = 1) THEN char_length(regexp_extract(s, "
        r"concat('^((?s:.*?))(?:', '\\d+$', ')'), 1)) + 1 WHEN (regexp(s, '\\d+$') = 0) THEN 0 ELSE NULL END")


@pytest.mark.parametrize('path, expected', [
    ('name', '$."name"'),
    ('customer.name', '$."customer"."name"'),
    ('items[0].sku', '$."items"[0]."sku"'),
    ('[0]', '$[0]'),
    ('a:b', '$."a"."b"'),
    ('vectors["k 1"].values', '$."vectors"."k 1"."values"'),
])
def test_json_extract_path_text_becomes_get_json_string(path, expected):
    assert sql(func.json_extract_path_text(S, path), 'starrocks') == f"get_json_string(s, '{expected}')"


def test_json_extract_path_text_per_row_path():
    # Databend's `:` separator is StarRocks' `.`.
    assert sql(func.json_extract_path_text(S, func.concat('[', 1, ']')), 'starrocks') == \
        "get_json_string(s, concat('$.', replace(concat('[', 1, ']'), ':', '.')))"


@pytest.mark.parametrize('args', [(S, 'a..b'), (S, 'a', 'b'), ()])
def test_json_extract_path_text_without_a_starrocks_spelling_is_refused(args):
    with pytest.raises(CompileError, match='json_extract_path_text'):
        sql(func.json_extract_path_text(*args), 'starrocks')


def test_json_object_insert_text_value_stays_a_json_string():
    assert sql(func.json_object_insert(func.parse_json(S), 'k', S, True), 'starrocks') == (
        """CASE WHEN (s IS NULL) THEN parse_json(s) """
        """ELSE json_set(parse_json(s), '$."k"', json_query(to_json(named_struct('v', s)), '$.v')) END""")


def test_json_object_insert_per_row_key_is_refused():
    with pytest.raises(CompileError, match='literal key'):
        sql(func.json_object_insert(func.parse_json(S), S, 99, True), 'starrocks')


def test_json_object_insert_untyped_json_looking_text_stays_a_string():
    # upper('{"a":1}') is untyped (NullType); raw into json_set StarRocks parsed it into {"k": {"A": 1}}.
    compiled = sql(func.json_object_insert(func.parse_json(S), 'k', func.upper(S), True), 'starrocks')
    assert "json_query(to_json(named_struct('v', upper(" in compiled and compiled.endswith("'$.v')) END")


def test_json_object_insert_typed_number_and_boolean_stay_raw():
    number = sqlalchemy.type_coerce(func.round(column('x', sqlalchemy.Float), 1), sqlalchemy.Numeric)
    flag = sqlalchemy.type_coerce(column('x', sqlalchemy.Float) > 1, sqlalchemy.Boolean)
    assert sql(func.json_object_insert(func.parse_json(S), 'k', flag, True), 'starrocks') == \
        """CASE WHEN ((x > 1) IS NULL) THEN parse_json(s) ELSE json_set(parse_json(s), '$."k"', x > 1) END"""
    compiled = sql(func.json_object_insert(func.parse_json(S), 'k', number, True), 'starrocks')
    assert "ELSE json_set(parse_json(s), '$.\"k\"', round(" in compiled and 'named_struct' not in compiled


def test_json_object_insert_without_update_is_refused():
    with pytest.raises(CompileError, match='update form'):
        sql(func.json_object_insert(func.parse_json(S), 'k', 1, False), 'starrocks')


def test_convert_timezone_becomes_convert_tz_from_utc():
    moment = column('t', sqlalchemy.DateTime)
    assert sql(func.convert_timezone('America/New_York', moment), 'starrocks') == \
        "convert_tz(t, 'UTC', 'America/New_York')"
    with pytest.raises(CompileError, match='two-argument'):
        sql(func.convert_timezone('UTC', 'America/New_York', moment), 'starrocks')


@pytest.mark.parametrize('dialect', ['databend', 'snowflake', 'databricks'])
@pytest.mark.parametrize('name, args', [
    ('json_extract_path_text', (S, 'a.b[0]')),
    ('json_object_insert', (S, 'k', S, True)),
    ('convert_timezone', ('America/New_York', S)),
])
def test_json_and_time_zone_functions_keep_other_dialects(dialect, name, args):
    assert sql(getattr(func, name)(*args), dialect) == sql(sqlalchemy.sql.functions.Function(name, *args), dialect)


@pytest.mark.parametrize('expression', [
    func.regexp_extract(S, '(?=a)(?:b)(c)', 1),       # a lookahead
    func.regexp_instr(S, '(?i)a(?m)^b'),              # a flag group past the start
    func.regexp_substr(S, '(?i:a.b)'),                # a scoped flag over more than text
    func.regexp_replace(S, '(?-i:a)(?:b)', 'x'),      # a flag turned off
])
def test_construct_without_an_exact_snowflake_spelling_keeps_the_old_rendering(expression):
    # Rather than emit a pattern that means something else.
    assert sql(expression, 'snowflake') == default(expression.name, *expression.clauses)


def test_per_row_group_number_cannot_follow_renumbered_groups():
    with pytest.raises(CompileError, match='group number must be a literal'):
        sql(func.regexp_extract(S, '(?:a)(b)', column('n', sqlalchemy.Integer)), 'snowflake')


def test_regexp_extract_arity_is_checked_on_snowflake():
    with pytest.raises(CompileError, match='regexp_extract takes'):
        sql(func.regexp_extract(S), 'snowflake')
    assert sql(func.regexp_extract(S, 'a'), 'snowflake') == \
        "IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, 'a', 1, 1, 'c'), ''))"


@pytest.mark.parametrize('pattern, group, expected_pattern, expected_group', [
    # A POSIX class ends at its own `:]`; the `.` and `(` after it are still inside the brackets.
    ('(?s:[[:alpha:].(])(b)', 1, '([[:alpha:].(])(b)', 2),
    # An escaped brace is text, so the `?` after it is a quantifier, not laziness.
    (r'a\{2,3}?(?:x)(y)', 1, r'a\\{2,3}?(x)(y)', 2),
    (r'\x{110000}(?:a)(b)', 1, r'\\x{110000}(a)(b)', 2),
    # A leading `]` (after an optional `^`) is a member, so the `(` after it is still inside the brackets.
    ('[^]a(](?:x)(b)', 1, '[^]a(](x)(b)', 2),
    # A nested non-capturing group inherits the dotall of the group around it.
    (r'(?s:a(?:b.))(c)', 1, r'(a(b[\\s\\S]))(c)', 3),
])
def test_scanner_edges(pattern, group, expected_pattern, expected_group):
    assert sql(func.regexp_extract(S, pattern, group), 'snowflake') == (
        f"IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, '{expected_pattern}', 1, 1, 'ce', {expected_group}), ''))")


def test_the_function_hook_leaves_other_snowflake_functions_alone():
    for expression in (func.upper(S), func.count(S), func.my_udf(S, 'x'), func.mypkg.regexp_replace(S, '(?i)a', 'b')):
        assert sql(expression, 'snowflake') == sql(expression, 'databend')


def test_value_reading_starrocks_compiles_are_not_statement_cached():
    # The StarRocks path/key is rendered from the bound value; a cache hit would reuse another call's path.
    assert func.json_extract_path_text(S, 'a')._generate_cache_key() is None
    assert func.json_object_insert(S, 'k', 1, True)._generate_cache_key() is None


@pytest.mark.parametrize('pattern, expected, flags', [
    ('(?i)(?m)^a', '^a', 'im'),                      # Savant's (?i) before the user's own flags
    ('(?s)(?i)(?m)a', 'a', 'ims'),
])
def test_every_leading_flag_group_becomes_a_parameter(pattern, expected, flags):
    assert sql(func.regexp_instr(S, pattern), 'snowflake') == f"REGEXP_INSTR(s, '{expected}', 1, 1, 0, '{flags}')"


@pytest.mark.parametrize('pattern, group, expected_pattern, expected_group, flags', [
    # Prophecy's case-insensitive keyword alternation.
    (r'\s(?i:AND|or)\s(x)', 1, r'\\s([Aa][Nn][Dd]|[Oo][Rr])\\s(x)', 2, 'ce'),
    # Already case-insensitive throughout: the group only stops being non-capturing.
    (r'(?i)(?i:ab)(x)', 1, '(ab)(x)', 2, 'ie'),
])
def test_scoped_case_flag_over_text_spells_both_cases(pattern, group, expected_pattern, expected_group, flags):
    assert sql(func.regexp_extract(S, pattern, group), 'snowflake') == (
        f"IFF(s IS NULL, NULL, COALESCE(REGEXP_SUBSTR(s, '{expected_pattern}', 1, 1, '{flags}', {expected_group}), ''))")


def test_control_characters_are_literal_but_nul_stays_an_escape():
    assert sql(func.regexp_instr(S, r'(?i)\x00\x{1F}\x85'), 'snowflake') == \
        "REGEXP_INSTR(s, '\\\\x00\x1f\x85', 1, 1, 0, 'i')"


def test_scoped_case_flag_over_a_multi_character_case_keeps_the_old_rendering():
    # 'ß'.upper() is 'SS' and no other letter folds with it; [SSß] would match a lone S.
    rendered = sql(func.regexp_extract(S, r'(?i:ße)(x)', 1), 'snowflake')
    assert '[S' not in rendered and '(?i:ße)' in rendered


def test_snowflake_regexp_compiles_are_not_statement_cached():
    # The Snowflake rendering is built from the pattern's value; a cache hit would reuse another call's pattern.
    for expression in (func.regexp_instr(S, 'a'), func.regexp_substr(S, 'a'), func.regexp_extract(S, 'a', 1)):
        assert expression._generate_cache_key() is None
    # regexp_replace is a plain (cacheable) Function: safe only while the dialect never caches statements.
    assert DIALECTS['snowflake'].supports_statement_cache is False


@pytest.mark.parametrize('letter', ['ς', 'σ', 'Σ', 'k', 'K', '\u212a', 's', '\u017f', 'å', '\u212b', '\u0131', '\u0130'])
def test_scoped_case_flag_over_a_letter_with_extra_case_folds_keeps_the_old_rendering(letter):
    rendered = sql(func.regexp_extract(S, f'(?i:a{letter})(x)', 1), 'snowflake')
    assert '[Aa]' not in rendered and f'(?i:a{letter})' in rendered


@pytest.mark.parametrize('value, expected', [
    # An untyped (NullType) result, such as substr's, is wrapped: its text may read as JSON.
    (func.substr(S, 1, 2), "json_query(to_json(named_struct('v', substr("),
    (column('t', sqlalchemy.Text), "json_query(to_json(named_struct('v', t)), '$.v')"),
    (column('ts', sqlalchemy.DateTime), "json_query(to_json(named_struct('v', CAST(ts AS STRING))), '$.v')"),
    (column('d', sqlalchemy.Date), "json_query(to_json(named_struct('v', CAST(d AS STRING))), '$.v')"),
    (column('n', sqlalchemy.Numeric(38, 10)), None),
    (column('i', sqlalchemy.Integer), None),
    (sqlalchemy.literal(True), None),
    (column('j', sqlalchemy.JSON), None),
])
def test_json_object_insert_wraps_all_but_typed_numbers_booleans_and_json(value, expected):
    compiled = sql(func.json_object_insert(func.parse_json(S), 'k', value, True), 'starrocks')
    if expected is None:
        # to_json would store a DECIMAL as a string and a boolean as 1
        assert 'named_struct' not in compiled
    else:
        assert expected in compiled


@pytest.mark.parametrize('key', ['k"x', 'y[0]', 'a[', 'a]', 'a\\b', '$', ''])
def test_json_object_insert_key_starrocks_misreads_is_refused(key):
    with pytest.raises(CompileError, match='key'):
        sql(func.json_object_insert(func.parse_json(S), key, 1, True), 'starrocks')


@pytest.mark.parametrize('key', ['a b', 'x.y', 'a*', '*', 'é', 'a$', 'a-b', 'a:b'])
def test_json_object_insert_key_starrocks_reads_literally_is_kept(key):
    assert f'$."{key}"' in sql(func.json_object_insert(func.parse_json(S), key, 1, True), 'starrocks')


@pytest.mark.parametrize('path', ['a[', 'a]', '["y[0]"]', '["a\\\\b"]', '["$"]', '[""]'])
def test_json_extract_path_text_key_starrocks_misreads_is_refused(path):
    with pytest.raises(CompileError, match='json_extract_path_text'):
        sql(func.json_extract_path_text(S, path), 'starrocks')


def test_json_object_insert_decorated_number_stays_raw():
    from plaidcloud.rpc.database import PlaidNumeric
    compiled = sql(func.json_object_insert(func.parse_json(S), 'k', column('n', PlaidNumeric), True), 'starrocks')
    assert 'named_struct' not in compiled


def test_dotall_group_leaves_a_dot_after_it_alone():
    assert sql(func.regexp_instr(S, r'(?s:a.)b.c'), 'snowflake') == \
        r"REGEXP_INSTR(s, '(a[\\s\\S])b.c', 1, 1, 0, 'c')"


@pytest.mark.parametrize('value', [
    column('t', sqlalchemy.Text), column('n', sqlalchemy.Numeric(38, 10)), column('i', sqlalchemy.Integer),
    sqlalchemy.null(), sqlalchemy.cast(sqlalchemy.null(), sqlalchemy.Integer),
])
def test_json_object_insert_null_value_returns_the_document_unchanged(value):
    compiled = sql(func.json_object_insert(func.parse_json(S), 'k', value, True), 'starrocks')
    assert compiled.startswith('CASE WHEN (') and 'IS NULL) THEN parse_json(s) ELSE json_set(' in compiled
