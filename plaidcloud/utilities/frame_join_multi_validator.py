#!/usr/bin/env python
# coding=utf-8

"""Config validator for the `frame_join_multi` transform step.

Two callers:
- Plaid `gst.save_object` hook (Layer 1) — runs at every persistence path.
- workflow-runner executor (Layer 2) — runs at execute time, catches paths that bypass Layer 1
  (flashback restore via remote RPC, Kube-job-dispatched imports, configs from before this
  validator was deployed).

Both layers import this same function; it is the single source of truth.

The validator never echoes user input back. Errors carry a stable reason token; the client maps
the token to a translated message. Reason tokens are listed in `_REASONS` below.
"""

import json
import re

__all__ = [
    'validate_frame_join_multi_config',
    'JoinMultiValidationError',
    'OPERATORS',
    'JOIN_TYPES',
    'RESERVED_ALIASES',
]


OPERATORS = frozenset({
    '=', '<>', '<', '<=', '>', '>=',
    'BETWEEN',
    'IS NULL', 'IS NOT NULL',
    'IN', 'NOT IN',
    'LIKE', 'NOT LIKE',
})

JOIN_TYPES = frozenset({'inner', 'left', 'full', 'cross'})

RESERVED_ALIASES = frozenset({
    'result', 'select', 'from', 'where', 'join', 'having', 'group', 'order',
    'union', 'table', 'inner', 'left', 'right', 'full', 'cross', 'on',
})

# A source/target table reference is the canonical `analyzetable_<uuid>` id that table_find/
# table_upsert return and that frame_extract and frame_join_inner/outer also accept (get_frame()
# resolves it). This is an identifier-shape safety gate only — it keeps dots, quotes, and
# whitespace out of the SQL the executor emits; the real existence check is get_frame() at execute
# time. Match the same id shape the rest of the platform uses (cf. core query.py _TABLE_ID_RE)
# rather than the old `tab`-prefix that never matched a real `analyzetable_` id.
_TABLE_ID_RE = re.compile(r'[A-Za-z0-9_][A-Za-z0-9_-]{0,127}')
_ALIAS_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,62}')
# Positional table names the executor's expression namespace reserves (`table1`, `table2`, ...);
# the bare `table` is in RESERVED_ALIASES. An alias matching these would be silently shadowed.
_POSITIONAL_TABLE_RE = re.compile(r'table\d+')
_COLUMN_REF_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]*\.[A-Za-z_][A-Za-z0-9_]*')
_COLUMN_ID_RE = re.compile(r'[A-Za-z_][A-Za-z0-9_]{0,62}')

# Allowed sqlalchemy dtype tokens (per plaidcloud.rpc.type_conversion.sqlalchemy_from_dtype).
# Closed set keeps the cross-layer contract tight — adding new types requires updating both
# this set and the dtype mapper.
_DTYPE_ENUM = frozenset({
    'text', 'integer', 'bigint', 'smallint', 'tinyint', 'numeric', 'decimal',
    'float', 'double', 'boolean', 'currency', 'date', 'timestamp', 'time', 'interval',
    'json', 'uuid', 'serial', 'bigserial', 'largebinary',
})

# Allowed aggregation tokens. Anything not in this set would still be accepted by
# get_agg_fn() via getattr(sqlalchemy.func, ...) and emit a bogus SQL function call.
_AGG_ENUM = frozenset({
    'group', 'group_null', 'dont_group',
    'sum', 'sum_null',
    'count', 'count_null', 'count_distinct', 'count_distinct_null',
    'min', 'min_null', 'max', 'max_null',
    'avg', 'avg_null',
})

MAX_CONFIG_BYTES = 256 * 1024
MAX_SOURCES = 32
MAX_TOTAL_CONDITIONS = 256
MAX_TARGET_COLUMNS = 1024
MAX_CONDITIONS_PER_EDGE = 16
MAX_IN_VALUES = 1000
MAX_IN_VALUE_BYTES = 1024
MAX_IN_VALUES_TOTAL_BYTES = 64 * 1024
MAX_PATTERN_LEN = 256


class JoinMultiValidationError(Exception):
    """Raised when a frame_join_multi config violates a validator rule.

    Carries a stable reason token (`reason`) and a JSONPath-style field locator (`field`).
    The client maps reason → translated message; the server never echoes user input.
    """

    def __init__(self, reason: str, field: str = ''):
        self.code = 'JOIN_VALIDATION'
        self.reason = reason
        self.field = field
        super().__init__(f'{self.code}: {reason} at {field}' if field else f'{self.code}: {reason}')


def _err(reason: str, field: str = ''):
    raise JoinMultiValidationError(reason, field)


def validate_frame_join_multi_config(config: dict, dialect: str | None = None) -> None:
    """Validate a frame_join_multi config dict.

    Raises JoinMultiValidationError on the first violation. Caller catches and translates
    to whatever exception type their layer needs (HTTPException, UserError, etc.).

    Args:
        config: The step config dict (the `config` field of a step record, not the whole step).
        dialect: The tenant's datastore_dialect ('starrocks', 'databend', etc.) for engine-specific
            rules like FULL OUTER rejection on Databend. None = skip dialect-specific rules
            (save-time hook doesn't have dialect in scope; executor passes it).
    """
    if not isinstance(config, dict):
        _err('config_not_dict')

    # Shape caps (run first, short-circuit before per-field checks)
    try:
        serialized_len = len(json.dumps(config))
    except (TypeError, ValueError):
        _err('config_not_json_serializable')
    if serialized_len > MAX_CONFIG_BYTES:
        _err('config_too_large')

    sources = config.get('sources')
    if not isinstance(sources, list):
        _err('sources_not_list', 'sources')
    if not (1 <= len(sources) <= MAX_SOURCES):
        _err('sources_count_out_of_range', 'sources')

    edges = config.get('edges')
    if not isinstance(edges, list):
        _err('edges_not_list', 'edges')
    if len(edges) != len(sources) - 1:
        _err('edges_count_mismatch', 'edges')

    total_conditions = sum(
        len(e.get('conditions', [])) for e in edges if isinstance(e, dict)
    )
    if total_conditions > MAX_TOTAL_CONDITIONS:
        _err('too_many_total_conditions', 'edges')

    target_columns = config.get('target_columns', [])
    if not isinstance(target_columns, list):
        _err('target_columns_not_list', 'target_columns')
    if len(target_columns) > MAX_TARGET_COLUMNS:
        _err('too_many_target_columns', 'target_columns')

    # Sources: alias + source field checks
    aliases_lower_to_alias: dict[str, str] = {}
    alias_to_columnset: dict[str, set[str]] = {}
    for i, s in enumerate(sources):
        sfield = f'sources[{i}]'
        if not isinstance(s, dict):
            _err('source_not_dict', sfield)
        alias = s.get('alias')
        if not isinstance(alias, str) or not _ALIAS_RE.fullmatch(alias):
            _err('alias_invalid', f'{sfield}.alias')
        # `table` and `table1`/`table2`/... are the positional names the executor exposes in the
        # expression namespace (get_safe_dict). An alias colliding with one would be silently
        # shadowed by the positional table — an expression `table1.col` would read the FIRST
        # source, not this alias — so reject those names (the bare `table` is in RESERVED_ALIASES).
        if alias.lower() in RESERVED_ALIASES or _POSITIONAL_TABLE_RE.fullmatch(alias.lower()):
            _err('alias_reserved', f'{sfield}.alias')
        if alias.lower() in aliases_lower_to_alias:
            _err('alias_duplicate', f'{sfield}.alias')
        aliases_lower_to_alias[alias.lower()] = alias

        source = s.get('source')
        if not isinstance(source, str) or not _TABLE_ID_RE.fullmatch(source):
            _err('source_not_table_id', f'{sfield}.source')

        source_columns = s.get('source_columns')
        if not isinstance(source_columns, list):
            _err('source_columns_not_list', f'{sfield}.source_columns')
        if len(source_columns) == 0:
            # A source with zero columns produces invalid SQL (SELECT FROM <table>) — reject.
            _err('source_columns_empty', f'{sfield}.source_columns')
        col_names: set[str] = set()
        for j, c in enumerate(source_columns):
            if not isinstance(c, dict) or 'id' not in c:
                _err('source_column_invalid', f'{sfield}.source_columns[{j}]')
            # Round-8 added isinstance check (TypeError leak); round-9 tightens to regex
            # match so empty strings, reserved words, identifier-unfriendly characters
            # ('col with space', '1col', 'order' etc.) don't slip through to SQL emit time.
            if not isinstance(c['id'], str) or not _COLUMN_ID_RE.fullmatch(c['id']):
                _err('source_column_invalid', f'{sfield}.source_columns[{j}].id')
            if c['id'] in col_names:
                # Duplicate ids silently dedupe in a set but cause sqlalchemy Table-build
                # failure at SQL emit time. Reject at save (round-7).
                _err('source_column_duplicate_id', f'{sfield}.source_columns[{j}].id')
            # dtype: required + closed enum. Round-11 caught that sqlalchemy_from_dtype(None)
            # raises RegexMapKeyError, and an unvalidated string can mask the actual column
            # type (e.g., defaulting INTEGER to text produces lexicographic comparison bugs).
            dtype = c.get('dtype')
            if not isinstance(dtype, str) or dtype.lower().split('(')[0] not in _DTYPE_ENUM:
                _err('source_column_dtype_invalid', f'{sfield}.source_columns[{j}].dtype')
            col_names.add(c['id'])
        alias_to_columnset[alias] = col_names

    aliases = [s['alias'] for s in sources]
    aliases_set = set(aliases)
    root_alias = aliases[0]

    # Edges: shape + tree topology
    to_aliases_seen: set[str] = set()
    for i, e in enumerate(edges):
        efield = f'edges[{i}]'
        if not isinstance(e, dict):
            _err('edge_not_dict', efield)
        from_alias = e.get('from_alias')
        to_alias = e.get('to_alias')
        # isinstance guard before `in <set>` lookups — list/dict aliases would raise
        # TypeError(unhashable) and escalate to HTTP 500 in the catch-all handler. Round-7.
        if not isinstance(from_alias, str):
            _err('from_alias_invalid', f'{efield}.from_alias')
        if not isinstance(to_alias, str):
            _err('to_alias_invalid', f'{efield}.to_alias')
        if from_alias not in aliases_set:
            _err('from_alias_unknown', f'{efield}.from_alias')
        if to_alias not in aliases_set:
            _err('to_alias_unknown', f'{efield}.to_alias')
        if to_alias == root_alias:
            _err('root_used_as_to_alias', f'{efield}.to_alias')
        if to_alias in to_aliases_seen:
            _err('to_alias_duplicate', f'{efield}.to_alias')
        to_aliases_seen.add(to_alias)

        jt = e.get('join_type')
        if not isinstance(jt, str):
            _err('join_type_invalid', f'{efield}.join_type')
        if jt not in JOIN_TYPES:
            _err('join_type_invalid', f'{efield}.join_type')
        if jt == 'full' and dialect == 'databend':
            _err('full_outer_unsupported_on_dialect', f'{efield}.join_type')

    # Tree connectivity: BFS from root must visit every alias
    children_by_parent: dict[str, list[str]] = {a: [] for a in aliases}
    for e in edges:
        children_by_parent[e['from_alias']].append(e['to_alias'])
    reachable = {root_alias}
    queue = [root_alias]
    while queue:
        n = queue.pop(0)
        for c in children_by_parent.get(n, ()):
            if c in reachable:
                continue
            reachable.add(c)
            queue.append(c)
    if reachable != aliases_set:
        _err('tree_not_connected_from_root', 'edges')

    # Conditions per edge
    for i, e in enumerate(edges):
        efield = f'edges[{i}]'
        conditions = e.get('conditions', [])
        if not isinstance(conditions, list):
            _err('conditions_not_list', f'{efield}.conditions')
        jt = e['join_type']
        if jt == 'cross':
            if conditions:
                _err('cross_join_has_conditions', f'{efield}.conditions')
            continue
        if not (1 <= len(conditions) <= MAX_CONDITIONS_PER_EDGE):
            _err('conditions_count_out_of_range', f'{efield}.conditions')

        from_alias = e['from_alias']
        to_alias = e['to_alias']
        aliases_seen_in_edge: set[str] = set()

        for j, c in enumerate(conditions):
            cfield = f'{efield}.conditions[{j}]'
            if not isinstance(c, dict):
                _err('condition_not_dict', cfield)
            op = c.get('operator')
            # isinstance guard prevents TypeError on `in OPERATORS` if op is list/dict (round-7).
            if not isinstance(op, str) or op not in OPERATORS:
                _err('operator_invalid', f'{cfield}.operator')

            left_expr = c.get('left_expr')
            if not isinstance(left_expr, str) or not _COLUMN_REF_RE.fullmatch(left_expr):
                _err('left_expr_invalid', f'{cfield}.left_expr')
            left_alias, left_col = left_expr.split('.', 1)
            if left_alias not in aliases_set:
                _err('left_expr_alias_unknown', f'{cfield}.left_expr')
            if left_col not in alias_to_columnset[left_alias]:
                _err('left_expr_column_unknown', f'{cfield}.left_expr')
            aliases_seen_in_edge.add(left_alias)

            # Operator-specific right-side validation
            if op in ('IS NULL', 'IS NOT NULL'):
                pass  # right_expr unused
            elif op in ('IN', 'NOT IN'):
                _validate_in_values(c.get('in_values'), f'{cfield}.in_values')
            elif op in ('LIKE', 'NOT LIKE'):
                _validate_pattern(c.get('pattern'), f'{cfield}.pattern')
            elif op == 'BETWEEN':
                _validate_between_bound(c.get('between_low'), aliases_set,
                                        alias_to_columnset, f'{cfield}.between_low',
                                        aliases_seen_in_edge)
                _validate_between_bound(c.get('right_expr'), aliases_set,
                                        alias_to_columnset, f'{cfield}.right_expr',
                                        aliases_seen_in_edge)
            else:
                # Binary column-to-column operators
                right_expr = c.get('right_expr')
                if not isinstance(right_expr, str) or not _COLUMN_REF_RE.fullmatch(right_expr):
                    _err('right_expr_invalid', f'{cfield}.right_expr')
                right_alias, right_col = right_expr.split('.', 1)
                if right_alias not in aliases_set:
                    _err('right_expr_alias_unknown', f'{cfield}.right_expr')
                if right_col not in alias_to_columnset[right_alias]:
                    _err('right_expr_column_unknown', f'{cfield}.right_expr')
                if right_alias == left_alias:
                    _err('same_alias_self_compare', cfield)
                aliases_seen_in_edge.add(right_alias)

        # At least one condition must reference both from_alias and to_alias (binds the edge)
        if from_alias not in aliases_seen_in_edge or to_alias not in aliases_seen_in_edge:
            _err('edge_conditions_do_not_bind_aliases', f'{efield}.conditions')

    # Target columns. `source_alias` is required for ALL frame_join_multi configs (no legacy
    # `source_table: 'table1'|'table2'` fallback). Round-5 caught that allowing the legacy
    # form for 2-source configs caused silent column-property-propagation loss in the
    # executor (which filters target_columns by source_alias). Uniform requirement closes that.
    # Round-11 adds target, dtype, agg, and mode-key checks — every field the executor reads.
    seen_targets: set[str] = set()
    for i, tc in enumerate(target_columns):
        tfield = f'target_columns[{i}]'
        if not isinstance(tc, dict):
            _err('target_column_not_dict', tfield)

        # `target`: the OUTPUT column name. Required by get_insert_query and _propagate_column_properties.
        target_name = tc.get('target')
        if not isinstance(target_name, str) or not _COLUMN_ID_RE.fullmatch(target_name):
            _err('target_name_invalid', f'{tfield}.target')
        if target_name in seen_targets:
            _err('target_name_duplicate', f'{tfield}.target')
        seen_targets.add(target_name)

        # `dtype`: required + closed enum. Same rationale as source_columns[*].dtype above.
        dtype = tc.get('dtype')
        if not isinstance(dtype, str) or dtype.lower().split('(')[0] not in _DTYPE_ENUM:
            _err('target_column_dtype_invalid', f'{tfield}.dtype')

        # `agg`: optional, but if set must be a closed-enum string. Anything outside the enum
        # would cause get_agg_fn to dispatch via getattr(sqlalchemy.func, ...) — arbitrary
        # SQL function call to a function the engine probably doesn't have.
        agg = tc.get('agg')
        if agg is not None and (not isinstance(agg, str) or agg not in _AGG_ENUM):
            _err('target_column_agg_invalid', f'{tfield}.agg')

        # Mode key: exactly one of (`source` for a column-ref, `expression`, `constant`) must be
        # set — unless dtype is a 'serial'/'bigserial' magic-column type. `expression` columns
        # route through the executor's eval_expression (the same surface as source_where/having)
        # and may reference the join aliases, which the executor exposes in the expression
        # namespace. A multi-table CASE like `collectedbyname` can only be expressed this way.
        # The executor selects "expression mode" with a truthy `if expression:` check
        # (get_from_clause), so the validator mirrors that truthiness. An empty string is falsy
        # in both (treated as no expression); a whitespace-only string is truthy but would crash
        # compile() at execute time, so reject it here (same as `having`/`source_where`).
        expression = tc.get('expression')
        if expression is not None and not isinstance(expression, str):
            _err('target_column_expression_not_string', f'{tfield}.expression')
        has_expression = bool(expression)
        if has_expression:
            if not expression.strip():
                _err('target_column_expression_empty', f'{tfield}.expression')
            if len(expression) > 4096:
                _err('target_column_expression_too_long', f'{tfield}.expression')
        mode_keys = [k for k in ('source', 'constant') if tc.get(k) not in (None, '')]
        if has_expression:
            mode_keys.append('expression')
        is_magic = dtype.lower() in {'serial', 'bigserial'} if isinstance(dtype, str) else False
        if not is_magic and len(mode_keys) != 1:
            _err('target_column_mode_required', tfield)

        # `source_alias` binds a source-column target to one source (the executor filters
        # target_columns by source_alias when propagating column properties). Required for
        # `source`-mode columns; `expression`/`constant`/magic columns aren't tied to a single
        # source so they don't carry one. Round-5 required it uniformly to close a legacy
        # `source_table` propagation hole — that hole only ever affected source-mode columns,
        # so scoping the requirement to source-mode keeps it closed.
        src_field = tc.get('source')
        if isinstance(src_field, str) and src_field:
            source_alias = tc.get('source_alias')
            # isinstance check first so we don't leak TypeError from the `in aliases_set` lookup
            # when given a list/dict (round-6 R6-8).
            if not isinstance(source_alias, str) or not source_alias:
                _err('source_alias_required', f'{tfield}.source_alias')
            if source_alias not in aliases_set:
                _err('source_alias_unknown', f'{tfield}.source_alias')
            # Cross-check: the target column's `source` must reference a column that exists in
            # the chosen alias's source_columns. Round-6 caught the column-existence half; round-7
            # closes the prefix-mismatch half (silent wrong-result if `source = 'a.col'` but
            # `source_alias = 'b'` and `col` exists in both aliases — executor emits `b.col`).
            if '.' in src_field:
                prefix, col_part = src_field.split('.', 1)
                if prefix != source_alias:
                    _err('target_column_source_alias_prefix_mismatch', f'{tfield}.source')
            else:
                col_part = src_field
            if col_part not in alias_to_columnset[source_alias]:
                _err('target_column_source_not_in_alias', f'{tfield}.source')

    # Per-source source_where validation:
    #  1. Reject whitespace-only strings (would compile() to empty expression and crash).
    #  2. Reject cross-source alias-qualified references.
    # Within-source alias-qualified refs (`<own_alias>.col`) are also rejected because the
    # executor's eval_expression only exposes the source as `table1`, not by alias — round-12
    # caught that the validator green-lit a form the executor cannot run. Users should write
    # `table1.col` (or just `col`) per the binary-join convention.
    for i, s in enumerate(sources):
        sw = s.get('source_where')
        if sw is None:
            continue
        if not isinstance(sw, str):
            _err('source_where_not_string', f'sources[{i}].source_where')
        if not sw.strip():
            # Whitespace-only string would crash apply_output_filter/eval_expression with
            # SyntaxError at execute time. Treat as absent.
            continue
        alias = s['alias']
        for other in aliases:
            if other == alias:
                continue
            pat = re.compile(rf'(?<![A-Za-z0-9_]){re.escape(other)}\.[A-Za-z_]')
            if pat.search(sw):
                _err('cross_source_where_ref', f'sources[{i}].source_where')
        # Reject within-own-source alias-qualified refs that the executor can't resolve.
        own_pat = re.compile(rf'(?<![A-Za-z0-9_]){re.escape(alias)}\.[A-Za-z_]')
        if own_pat.search(sw):
            _err('source_where_alias_qualified_ref', f'sources[{i}].source_where')

    # Having: type-check only. Scope enforcement (`result.<target>` only, no bare names, no
    # source aliases) happens at execute time in apply_output_filter's eval_expression scope —
    # an unknown bare name raises NameError → SQLExpressionError → UserError, surfaced to the
    # user as a friendly message. A save-time scope check would need a real expression parser
    # to avoid false-positives on string literals; deferred to Phase 2+ if it proves needed.
    having = config.get('having')
    if having is not None and not isinstance(having, str):
        _err('having_not_string', 'having')
    if isinstance(having, str):
        if len(having) > 4096:
            _err('having_too_long', 'having')
        # Whitespace-only would crash apply_output_filter at execute time (compile of '' raises
        # SyntaxError outside eval_expression's try/except). Round-12 caught it persisting
        # across multiple rounds — close it at the validator.
        if having and not having.strip():
            _err('having_empty_or_whitespace', 'having')
        # Reject having referencing target columns that don't exist (round-13).
        # apply_output_filter exposes `result.<target_name>`; an unknown target would raise
        # AttributeError at execute time. Catching at save time gives a friendly reason.
        if having and having.strip():
            for m in re.finditer(r'(?<![A-Za-z0-9_])result\.([A-Za-z_][A-Za-z0-9_]*)', having):
                if m.group(1) not in seen_targets:
                    _err('having_references_unknown_target', 'having')
                    break

    # output_row_limit: user-controlled soft ceiling for the cartesian guard. Cap it server-side
    # so a malicious config can't set it to 499B and bypass the guard up to the hard limit.
    output_row_limit = config.get('output_row_limit')
    if output_row_limit is not None:
        if not isinstance(output_row_limit, int) or isinstance(output_row_limit, bool):
            _err('output_row_limit_not_int', 'output_row_limit')
        if output_row_limit < 1 or output_row_limit > 10_000_000_000:
            _err('output_row_limit_out_of_range', 'output_row_limit')

    # target_frame: the destination table id. Required by the executor's get_frame() call.
    # Validator round-11 closed the gap (was unchecked).
    target_frame = config.get('target_frame')
    if not isinstance(target_frame, str) or not _TABLE_ID_RE.fullmatch(target_frame):
        _err('target_frame_invalid', 'target_frame')

    # datastore_dialect: derived server-side from tenant context, NOT user config. Save-time
    # callers (dialect=None) reject any user-supplied value to prevent bypassing engine-specific
    # rules. Execute-time callers (dialect != None) accept whatever's there since the framework
    # has already merged in the tenant-trusted dialect. Validator round-11 closed this bypass.
    if dialect is None and 'datastore_dialect' in config:
        _err('datastore_dialect_not_user_settable', 'datastore_dialect')


def _validate_in_values(in_values, field):
    if not isinstance(in_values, list):
        _err('in_values_not_list', field)
    if not (1 <= len(in_values) <= MAX_IN_VALUES):
        _err('in_values_count_out_of_range', field)
    total = 0
    for k, v in enumerate(in_values):
        if not isinstance(v, (str, int, float, bool)) and v is not None:
            _err('in_value_not_primitive', f'{field}[{k}]')
        try:
            elem_len = len(json.dumps(v))
        except (TypeError, ValueError):
            _err('in_value_not_serializable', f'{field}[{k}]')
        if elem_len > MAX_IN_VALUE_BYTES:
            _err('in_value_too_large', f'{field}[{k}]')
        total += elem_len
    if total > MAX_IN_VALUES_TOTAL_BYTES:
        _err('in_values_total_too_large', field)


def _validate_pattern(pattern, field):
    if not isinstance(pattern, str):
        _err('pattern_not_string', field)
    if len(pattern) > MAX_PATTERN_LEN:
        _err('pattern_too_long', field)


def _validate_between_bound(bound, aliases_set, alias_to_columnset, field, aliases_seen_in_edge):
    """A BETWEEN bound is either a column ref (alias.col) or a primitive literal."""
    if isinstance(bound, str):
        if _COLUMN_REF_RE.fullmatch(bound):
            alias, col = bound.split('.', 1)
            if alias not in aliases_set:
                _err('between_bound_alias_unknown', field)
            if col not in alias_to_columnset[alias]:
                _err('between_bound_column_unknown', field)
            aliases_seen_in_edge.add(alias)
            return
        # else treat as a string literal — allowed
        if len(bound) > MAX_IN_VALUE_BYTES:
            _err('between_bound_string_too_long', field)
        return
    if isinstance(bound, (int, float, bool)) or bound is None:
        return
    _err('between_bound_invalid', field)


