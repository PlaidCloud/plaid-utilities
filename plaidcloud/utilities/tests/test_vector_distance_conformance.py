# coding=utf-8
"""sc-30364 Tier 2: vector_distance conformance against a live StarRocks warehouse.

Tier 1 (test_sqlalchemy_functions.py) pins the SQL string this module's canonical
expression compiles to. It cannot tell whether that SQL computes the right number --
StarRocks' `l2_distance` returning SQUARED L2 under a distance name is exactly the class
of defect a compile test is blind to. So an engine's contract row is a hypothesis until
this module passes against a real warehouse of that engine.

StarRocks' row is no longer a hypothesis: 13 of the 14 cells were executed live on
paul-dev (`current_version()` = '4.1.3-8a8e186', read-only), the two near_duplicate cases
among them, and every one agrees with an independently computed distance inside the
tolerance below. The fourteenth, ('identical_768', 'l2'), is REFERENCE-DERIVED, not
measured -- see MEASURED_CANONICAL_STARROCKS_4_1_3, which labels it. The values are
recorded there and re-checked on every CI run.

Running it live::

    PLAID_VECTOR_CONFORMANCE_DSN='starrocks://user:pw@host:9030/db' \
        pytest plaidcloud/utilities/tests/test_vector_distance_conformance.py

Without that variable the live class skips and only the harness tests run -- those are
unconditional, and they prove the harness would actually FAIL on the two defects
sc-30350 measured (squared L2, and a NULL where a number was expected) rather than
being vacuously green.

No stored data is needed: every case is a literal ARRAY<FLOAT> pair, so any StarRocks
connection suffices. paul-dev's `sc30350-vector-gate` project additionally holds the
sc-30350 fixtures (`vec_roundtrip`, `vec_long`, `vec_perf_768`, `vec_perf_1536`) if a
data-bound or latency re-run is ever wanted.

Owner and cadence are recorded in the story (sc-30364). Be clear about what the version
guard below is and is not: NOTHING here schedules itself. This repo's CI does not set
PLAID_VECTOR_CONFORMANCE_DSN and has no `schedule:`/`cron:` workflow. The guard's code
path does run in CI, against the hardcoded version strings the harness tests pass it; what
it can never do unprompted is read a REAL engine's version, because only the live class
does that and only a person starts the live class. The guard annotates a run; it cannot
trigger one. The cadence is therefore manual and depends
on its named owner -- if that is ever to change it needs a scheduled workflow carrying the
DSN as a secret, which is the change that would make the guard load-bearing.
"""
import math
import os
import struct
import unittest

import sqlalchemy

from plaidcloud.utilities import sqlalchemy_functions as sf

#: The DSN of a StarRocks warehouse to run the live tier against. Absent -> skip.
VECTOR_CONFORMANCE_DSN_ENV = 'PLAID_VECTOR_CONFORMANCE_DSN'

#: The engine version these expected values were verified against (sc-30364, live).
#: A different release is reported as a failure, not tolerated: StarRocks changed
#: `l2_distance`'s semantics from Databend's under the same name once already, so the
#: values here are only claimed for this release. This ANNOTATES a run -- it does not
#: schedule one (see the module docstring). StarRocks' current_version()
#: returns release-buildhash ('4.1.3-8a8e186', measured live on paul-dev), so only the
#: release prefix is compared -- a rebuild of the same release is not a semantics change.
VERIFIED_STARROCKS_VERSION = '4.1.3'


def _release(engine_version):
    return str(engine_version).split('-', 1)[0]

#: Tolerance. StarRocks accumulates distance math in float32 and returns FLOAT (~7
#: significant decimal digits); sc-30350 measured agreement with a float64 reference
#: over float32 inputs at ~6e-8 relative for cosine_similarity and ~7.5e-8 for
#: l2_distance. A pure RELATIVE tolerance is not enough for cosine, for TWO reasons,
#: and the absolute term carries both:
#:   1. the float32 error lands on the SIMILARITY (magnitude ~1) while `1 - similarity`
#:      can be ~0.025, turning a 6e-8 absolute error into a 2.4e-6 relative one;
#:   2. an exact match has a NEGATIVE cosine distance -- StarRocks' cosine_similarity
#:      overshoots 1 on identical vectors (measured live on 4.1.3: 1.0000001 for a
#:      768-dim vector, so the distance is -1.1920929e-7). Around an expected 0 the
#:      relative term contributes nothing at all, so the absolute term is the ONLY
#:      thing that admits the real self-distance. Tightening it below ~2e-7 would
#:      make the identical_768 case fail against a correct engine.
#: Combined as abs(actual - expected) <= ABSOLUTE + RELATIVE * abs(expected). Loose
#: enough for float32, ~200x tighter than the squared-vs-true L2 gap it has to catch.
#: Loosening either constant is a contract change: TestConformanceHarness pins both
#: values and pins that a 1e-5 absolute error is still caught.
ABSOLUTE_TOLERANCE = 1e-6
RELATIVE_TOLERANCE = 1e-6

_FLOAT32_MAX = 3.4028234663852886e38

#: (name, a, b). The first five are sc-30350's own measured rows, so a failure here is
#: directly comparable to the numbers in that gate report.
_LONG = [i / 1000.0 for i in range(1, 769)]
CASES = (
    # l2_distance -> 27 live; the true distance is 5.196152. The squared-L2 exposer.
    ('basic', [1.0, 2.0, 3.0], [4.0, 5.0, 6.0]),
    ('precision', [1.0, -2.5, 3.14159265358979, 2.718281828459045], [0.5, -1.25, 2.0, 0.0625]),
    # Both metrics measured NULL here: float32 accumulator overflow, silently.
    ('overflow', [1e-38, 1e38, 1.1754943508222875e-38, 3.4028234663852886e38], [0.5, -1.25, 2.0, 0.0625]),
    ('large_finite', [16777216.0, 16777217.0, 0.30000000000000004, 1.0000001], [0.5, -1.25, 2.0, 0.0625]),
    # Self-distance. l2 is exactly 0 live; cosine is NOT -- cosine_similarity(v, v)
    # measured 1.0000001 on 4.1.3, so the cosine distance is -1.1920929e-7. Negative,
    # and only the absolute tolerance term admits it.
    ('identical_768', _LONG, list(_LONG)),
    # The nearest-neighbour operating regime, which nothing else here covers. Real NN
    # search lives at cosine distance ~1e-5..1e-2, where the 1e-6 absolute floor is
    # percent-level slack rather than the 0.004% it is at 0.025 -- and the slack depends
    # on the distance MAGNITUDE, not the dimension, so short vectors exercise it exactly.
    # (Dimensional realism is identical_768's job.) Deliberately short so the live tier
    # stays cheap to run: a conformance case nobody can afford to execute is not a case.
    ('near_duplicate', [0.5, 0.25, 0.125, 0.0625], [0.501, 0.249, 0.126, 0.0615]),
    ('near_duplicate_wider', [0.5, 0.25, 0.125, 0.0625], [0.506, 0.244, 0.131, 0.0565]),
)


#: THE RECORDED TIER 2 RUN, and its provenance cell by cell. 13 of these 14 values are
#: what the canonical expression ITSELF returned, executed live on paul-dev (StarRocks
#: `current_version()` = '4.1.3-8a8e186', read-only, sc-30364). All 14 pass the diff
#: against the independently computed reference -- the assertion is
#: test_the_recorded_live_run_still_passes below, so the run is enforced in CI rather
#: than living in a report someone has to find.
#:
#: The ONE exception is marked inline: ('identical_768', 'l2') is reference-derived, not
#: measured. Do not let it drift into being cited as engine output -- a baseline diffed
#: against a number no engine produced is the same defect this suite exists to catch, one
#: layer down. Closing it needs a single read-only query,
#:     SELECT CAST(sqrt(l2_distance(v, v)) AS DOUBLE)
#:         FROM (SELECT array_map(x -> CAST(x / 1000.0 AS FLOAT),
#:                                array_generate(1, 768)) AS v) t;
#: which is expected to return exactly 0.0; replace the entry and this note when it runs.
#:
#: The 768-dim vector was supplied to the engine as
#: array_map(x -> CAST(x / 1000.0 AS FLOAT), array_generate(1, 768)), which sc-30350
#: verified element-for-element equal to float32(i / 1000.0) at every one of the 768
#: positions; the other six cases were passed as the literal arrays this module emits.
#:
#: For contrast, the raw inner values on the same engine: l2_distance([1,2,3], [4,5,6])
#: = 27 (SQUARED), which sqrt() turns into the 5.196152422706632 recorded here, and
#: cosine_similarity(v, v) = 1.0000001192092896 for the 768-dim vector, which is why
#: identical_768's cosine distance is NEGATIVE.
MEASURED_CANONICAL_STARROCKS_4_1_3 = {
    ('basic', 'cosine'): 0.025368213653564453,
    ('basic', 'l2'): 5.196152422706632,
    ('precision', 'cosine'): 0.15590637922286987,
    ('precision', 'l2'): 3.1888729953111348,
    ('overflow', 'cosine'): None,
    ('overflow', 'l2'): None,
    ('large_finite', 'cosine'): 1.2198967933654785,
    ('large_finite', 'l2'): 23726567.82027641,
    ('identical_768', 'cosine'): -1.1920928955078125e-07,
    # REFERENCE-DERIVED, not measured: sc-30350 measured l2_distance(v, v) = 0 exactly
    # for this vector, and sqrt(0) = 0. The canonical expression was not itself run for
    # this one cell. See the note above.
    ('identical_768', 'l2'): 0.0,
    ('near_duplicate', 'cosine'): 5.543231964111328e-06,
    ('near_duplicate', 'l2'): 0.0019999947678738636,
    ('near_duplicate_wider', 'cosine'): 0.00019866228103637695,
    ('near_duplicate_wider', 'l2'): 0.01199998869195358,
}


def _f32(value):
    return struct.unpack('f', struct.pack('f', value))[0]


def reference_distance(metric, a, b):
    """The expected distance, computed independently of the warehouse.

    Inputs are rounded to float32 (what the column stores) and accumulated in float64
    (what a correct answer is), then the engine's float32 accumulator is modelled: a
    partial sum past the float32 ceiling is where StarRocks returns NULL rather than inf
    or an error, so `None` is a real expected value here, not "unknown".
    """
    a = [_f32(x) for x in a]
    b = [_f32(y) for y in b]
    if metric == 'cosine':
        dot = math.fsum(x * y for x, y in zip(a, b))
        norm_a = math.fsum(x * x for x in a)
        norm_b = math.fsum(y * y for y in b)
        if max(abs(dot), norm_a, norm_b) > _FLOAT32_MAX:
            return None
        return 1.0 - dot / math.sqrt(norm_a * norm_b)
    if metric == 'l2':
        squared = math.fsum((x - y) ** 2 for x, y in zip(a, b))
        if squared > _FLOAT32_MAX:
            return None
        return math.sqrt(squared)
    raise AssertionError(f'no reference implementation for metric {metric!r}')


def distance_sql(metric, a, b):
    """The canonical expression itself, compiled for StarRocks -- not a hand-written
    query, so the live tier verifies what callers actually emit."""
    def literal(values):
        return sqlalchemy.literal_column(
            'CAST([{}] AS ARRAY<FLOAT>)'.format(', '.join(repr(float(v)) for v in values)))
    expr = sf.vector_distance(metric, literal(a), literal(b))
    dialect = sqlalchemy.dialects.registry.load('starrocks')()
    return str(expr.compile(dialect=dialect, compile_kwargs={'literal_binds': True}))


def conformance_failures(execute, engine_version, cases=CASES):
    """Run every case through `execute` and return a list of human-readable failures.

    `execute` takes one SQL expression string and returns its scalar value (or None for
    NULL). Passing it in keeps the diffing logic testable without a warehouse.
    """
    failures = []
    if _release(engine_version) != VERIFIED_STARROCKS_VERSION:
        failures.append(
            f'engine version {engine_version!r} is not the verified {VERIFIED_STARROCKS_VERSION!r}: '
            're-verify the metric semantics live and update VERIFIED_STARROCKS_VERSION')
    for name, a, b in cases:
        for metric in sf.VECTOR_DISTANCE_METRICS:
            expected = reference_distance(metric, a, b)
            actual = execute(distance_sql(metric, a, b))
            if expected is None:
                if actual is not None:
                    failures.append(
                        f'{name}/{metric}: expected NULL (float32 accumulator overflow), got {actual!r}')
            elif actual is None:
                failures.append(f'{name}/{metric}: expected {expected!r}, got NULL')
            else:
                tolerance = ABSOLUTE_TOLERANCE + RELATIVE_TOLERANCE * abs(expected)
                if abs(float(actual) - expected) > tolerance:
                    failures.append(
                        f'{name}/{metric}: expected {expected!r}, got {actual!r} '
                        f'(difference {abs(float(actual) - expected)!r} exceeds {tolerance!r})')
    return failures


def _by_name(name):
    return next((a, b) for case_name, a, b in CASES if case_name == name)


def _perfect_engine(sql):
    return _PERFECT[sql]


_PERFECT = {
    distance_sql(metric, a, b): reference_distance(metric, a, b)
    for name, a, b in CASES
    for metric in sf.VECTOR_DISTANCE_METRICS
}


class TestConformanceHarness(unittest.TestCase):
    """The harness must fail on the defects sc-30350 actually measured. An accepting
    harness is worse than none: it would certify the contract on the first live run."""

    def test_a_correct_engine_produces_no_failures(self):
        self.assertEqual([], conformance_failures(_perfect_engine, VERIFIED_STARROCKS_VERSION))

    def test_squared_l2_is_caught(self):
        # StarRocks 4.1.3's own behaviour without the sqrt() wrapper.
        def squared_l2_engine(sql):
            value = _PERFECT[sql]
            return value ** 2 if 'l2_distance' in sql and value is not None else value

        failures = conformance_failures(squared_l2_engine, VERIFIED_STARROCKS_VERSION)
        self.assertTrue(failures)
        self.assertTrue(all('/l2:' in f for f in failures), failures)
        self.assertIn('basic/l2', ' '.join(failures))

    def test_a_null_where_a_number_is_expected_is_caught(self):
        failures = conformance_failures(lambda sql: None, VERIFIED_STARROCKS_VERSION)
        self.assertIn('got NULL', ' '.join(failures))

    def test_a_number_where_null_is_expected_is_caught(self):
        # Masking the overflow (COALESCE to a sentinel distance) must not pass.
        failures = conformance_failures(
            lambda sql: 0.0 if _PERFECT[sql] is None else _PERFECT[sql], VERIFIED_STARROCKS_VERSION)
        self.assertEqual(2, len(failures), failures)
        self.assertTrue(all(f.startswith('overflow/') for f in failures), failures)

    def test_a_version_bump_is_reported_as_a_rerun_trigger(self):
        failures = conformance_failures(_perfect_engine, '4.2.0')
        self.assertIn('re-verify', failures[0])

    def test_the_build_hash_starrocks_actually_returns_is_not_drift(self):
        # current_version() on paul-dev returns '4.1.3-8a8e186'; comparing the whole
        # string would fail the suite on every live run.
        self.assertEqual([], conformance_failures(_perfect_engine, '4.1.3-8a8e186'))

    def test_the_tolerance_separates_true_l2_from_the_squared_value(self):
        # 5.196152 vs 27 on sc-30350's own case, and the tolerance is nowhere near it.
        expected = reference_distance('l2', *_by_name('basic'))
        self.assertAlmostEqual(5.196152422706632, expected, places=9)
        self.assertGreater(abs(27.0 - expected),
                           ABSOLUTE_TOLERANCE + RELATIVE_TOLERANCE * abs(expected))

    def test_the_overflow_case_expects_null_from_both_metrics(self):
        a, b = _by_name('overflow')
        for metric in sf.VECTOR_DISTANCE_METRICS:
            with self.subTest(metric=metric):
                self.assertIsNone(reference_distance(metric, a, b))

    def test_self_distance_is_zero_for_l2_and_slightly_negative_for_cosine(self):
        # Not symmetric, and the asymmetry is the engine's: l2_distance(v, v) is exactly
        # 0, but cosine_similarity(v, v) overshoots 1 in float32 (measured 1.0000001 on
        # 4.1.3 for this vector), so the cosine distance is -1.1920929e-7. A downstream
        # `distance >= 0` filter would drop exact matches -- the top hit.
        a, b = _by_name('identical_768')
        self.assertEqual(0.0, reference_distance('l2', a, b))
        self.assertEqual(-1.1920928955078125e-7,
                         MEASURED_CANONICAL_STARROCKS_4_1_3[('identical_768', 'cosine')])
        self.assertEqual([], conformance_failures(
            _perfect_engine, VERIFIED_STARROCKS_VERSION, cases=[('identical_768', a, b)]))

    def test_the_nearest_neighbour_band_is_covered(self):
        for name in ('near_duplicate', 'near_duplicate_wider'):
            with self.subTest(case=name):
                cosine = reference_distance('cosine', *_by_name(name))
                self.assertLess(1e-6, cosine)
                self.assertGreater(1e-2, cosine)

    def test_both_tolerance_constants_are_pinned(self):
        # Loosening either is a contract change, not a maintenance detail: a 1000x
        # looser absolute term still separates 27 from 5.196, so the only thing standing
        # between this suite and a rubber stamp is these two numbers.
        self.assertEqual(1e-6, ABSOLUTE_TOLERANCE)
        self.assertEqual(1e-6, RELATIVE_TOLERANCE)

    def test_a_ten_times_looser_absolute_error_is_still_caught(self):
        # 1e-5 absolute on the cosine cells, where the relative term contributes
        # essentially nothing. Fails if ABSOLUTE_TOLERANCE is ever raised past 1e-5.
        def drifting_engine(sql):
            value = _PERFECT[sql]
            return value if value is None or 'cosine_similarity' not in sql else value + 1e-5

        failures = conformance_failures(drifting_engine, VERIFIED_STARROCKS_VERSION)
        self.assertEqual(len([c for c in CASES if reference_distance('cosine', c[1], c[2]) is not None]),
                         len(failures), failures)
        self.assertTrue(all('/cosine:' in f for f in failures), failures)

    def test_the_executed_sql_is_the_canonical_expression(self):
        self.assertIn('sqrt(l2_distance(', distance_sql('l2', [1.0], [2.0]))
        self.assertIn('1 - cosine_similarity(', distance_sql('cosine', [1.0], [2.0]))

    def test_the_recorded_live_run_still_passes(self):
        """The Tier 2 run itself, replayed deterministically: feed the harness exactly
        what StarRocks 4.1.3 returned and it must report no failures. This is what keeps
        the executed run a live artefact instead of a claim in a PR body -- if anyone
        tightens the tolerance or changes a case's vectors without re-running live, this
        fails."""
        recorded = {(name, metric): MEASURED_CANONICAL_STARROCKS_4_1_3[(name, metric)]
                    for name, _, _ in CASES for metric in sf.VECTOR_DISTANCE_METRICS}
        self.assertEqual(len(recorded), len(MEASURED_CANONICAL_STARROCKS_4_1_3),
                         'a case was added or renamed without a recorded live value')
        index = {distance_sql(metric, a, b): (name, metric)
                 for name, a, b in CASES for metric in sf.VECTOR_DISTANCE_METRICS}
        failures = conformance_failures(lambda sql: recorded[index[sql]], '4.1.3-8a8e186')
        self.assertEqual([], failures, '\n'.join(failures))

    def test_the_recorded_cosine_self_distance_is_negative(self):
        # Not a rounding curiosity: it is the reason a `distance >= 0` guard downstream
        # would drop exact matches, and the reason ABSOLUTE_TOLERANCE cannot be tightened
        # below ~2e-7.
        self.assertLess(MEASURED_CANONICAL_STARROCKS_4_1_3[('identical_768', 'cosine')], 0.0)
        self.assertGreater(ABSOLUTE_TOLERANCE,
                           abs(MEASURED_CANONICAL_STARROCKS_4_1_3[('identical_768', 'cosine')]))

    def test_an_unmodelled_metric_is_not_silently_skipped(self):
        with self.assertRaises(AssertionError):
            reference_distance('inner_product', [1.0], [2.0])


@unittest.skipUnless(
    os.environ.get(VECTOR_CONFORMANCE_DSN_ENV),
    f'live StarRocks conformance requires {VECTOR_CONFORMANCE_DSN_ENV}')
class TestStarrocksVectorDistanceLive(unittest.TestCase):  # pragma: no cover - requires a warehouse
    def test_every_case_matches_an_independently_computed_distance(self):
        engine = sqlalchemy.create_engine(os.environ[VECTOR_CONFORMANCE_DSN_ENV])
        with engine.connect() as conn:
            version = conn.execute(sqlalchemy.text('SELECT current_version()')).scalar()

            def execute(sql):
                return conn.execute(sqlalchemy.text(f'SELECT {sql}')).scalar()

            failures = conformance_failures(execute, str(version))
        self.assertEqual([], failures, '\n'.join(failures))
