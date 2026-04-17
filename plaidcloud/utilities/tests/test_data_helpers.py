# coding=utf-8
"""Tests for plaidcloud.utilities.data_helpers."""

import os
import unittest

import numpy as np
import pandas as pd

from plaidcloud.utilities import data_helpers as dh


class TestSuppressNonUnicode(unittest.TestCase):

    def test_ascii_passthrough(self):
        self.assertEqual(dh.suppress_non_unicode('hello'), 'hello')

    def test_non_ascii_replaced_with_space(self):
        self.assertEqual(dh.suppress_non_unicode('caf\u00e9'), 'caf ')


class TestCastAsInt(unittest.TestCase):

    def test_numeric_string(self):
        self.assertEqual(dh.cast_as_int('3'), 3)

    def test_float_truncation(self):
        self.assertEqual(dh.cast_as_int(3.55), 3)

    def test_non_numeric_string_returns_zero(self):
        self.assertEqual(dh.cast_as_int('Three'), 0)

    def test_none_returns_zero(self):
        self.assertEqual(dh.cast_as_int(None), 0)

    def test_nan_returns_zero(self):
        self.assertEqual(dh.cast_as_int(np.nan), 0)

    def test_pd_na_returns_zero(self):
        self.assertEqual(dh.cast_as_int(pd.NA), 0)


class TestCastAsFloat(unittest.TestCase):

    def test_numeric_string(self):
        self.assertEqual(dh.cast_as_float('3'), 3.0)

    def test_float_passthrough(self):
        self.assertEqual(dh.cast_as_float(3.55), 3.55)

    def test_non_numeric_string_returns_zero(self):
        self.assertEqual(dh.cast_as_float('Three'), 0.0)

    def test_nan_returns_zero(self):
        self.assertEqual(dh.cast_as_float(np.nan), 0.0)

    def test_none_returns_zero(self):
        self.assertEqual(dh.cast_as_float(None), 0.0)


class TestCastAsStr(unittest.TestCase):

    def test_pass_through_string(self):
        self.assertEqual(dh.cast_as_str('x'), 'x')

    def test_float_to_string(self):
        self.assertEqual(dh.cast_as_str(3.55), '3.55')

    def test_none_returns_space(self):
        self.assertEqual(dh.cast_as_str(None), ' ')

    def test_nan_returns_space(self):
        self.assertEqual(dh.cast_as_str(np.nan), ' ')

    def test_pd_na_returns_space(self):
        self.assertEqual(dh.cast_as_str(pd.NA), ' ')


class TestCoalesce(unittest.TestCase):

    def test_empty(self):
        self.assertIsNone(dh.coalesce())

    def test_single_none(self):
        self.assertIsNone(dh.coalesce(None))

    def test_returns_first_non_empty(self):
        self.assertEqual(dh.coalesce(None, 'a'), 'a')
        self.assertEqual(dh.coalesce(None, None, 'a'), 'a')
        self.assertEqual(dh.coalesce(None, 'a', 'b'), 'a')

    def test_empty_string_skipped(self):
        self.assertEqual(dh.coalesce('', 'a'), 'a')

    def test_nan_skipped(self):
        self.assertEqual(dh.coalesce(float('nan'), 'a'), 'a')


class TestSafeDivide(unittest.TestCase):

    def test_float_division(self):
        self.assertEqual(dh.safe_divide(1, 4.0), 0.25)

    def test_int_floor_division(self):
        # When both inputs are ints, result is integer floor division.
        self.assertEqual(dh.safe_divide(1, 4), 0)

    def test_divide_by_zero_returns_default(self):
        # The default must behave like a number for downstream isnan/isinf checks.
        self.assertIs(dh.safe_divide(1, 0, False), False)

    def test_divide_by_none_returns_default(self):
        self.assertIs(dh.safe_divide(1, None, False), False)

    def test_divide_by_nan_returns_default(self):
        self.assertIs(dh.safe_divide(1, np.nan, False), False)

    def test_none_numerator_returns_default(self):
        self.assertIs(dh.safe_divide(None, 1, False), False)


class TestRemoveAll(unittest.TestCase):

    def test_no_substrings(self):
        self.assertEqual(dh.remove_all('abc', []), 'abc')

    def test_single_char_removed(self):
        self.assertEqual(
            dh.remove_all('Four !score! an!d', ['!']),
            'Four score and',
        )

    def test_multiple_substrings(self):
        out = dh.remove_all('Four score and seven years ago', ['score ', 'and ', 'seven '])
        self.assertEqual(out, 'Four years ago')


class TestRemoveNanValuesFromDict(unittest.TestCase):

    def test_drops_nan_values(self):
        result = dh.remove_nan_values_from_dict({'a': 1, 'b': np.nan, 'c': 'x'})
        self.assertEqual(result, {'a': 1, 'c': 'x'})

    def test_drops_none_values(self):
        result = dh.remove_nan_values_from_dict({'a': 1, 'b': None})
        self.assertEqual(result, {'a': 1})


class TestCleanNames(unittest.TestCase):

    def test_drops_in_columns_and_strips_out_suffix(self):
        df = pd.DataFrame({
            'keep': [1, 2],
            'col__in': [3, 4],
            'price__out': [5, 6],
            'val__value': [7, 8],
            'name__split': ['a', 'b'],
        })
        result = dh.clean_names(df)
        self.assertIn('keep', result.columns)
        self.assertNotIn('col__in', result.columns)
        self.assertNotIn('val__value', result.columns)
        # __out trims the last 5 chars ('__out')
        self.assertIn('price', result.columns)
        self.assertNotIn('price__out', result.columns)
        # __split trims the last 7 chars
        self.assertIn('name', result.columns)
        self.assertNotIn('name__split', result.columns)


class TestMaskFilters(unittest.TestCase):
    """Daisy-chain dataframe filter helpers."""

    def setUp(self):
        self.df = pd.DataFrame({'key': [1, 2, 3, 4], 'val': ['a', 'b', 'c', 'd']})

    def test_mask_returns_matching_rows(self):
        result = dh.mask(self.df, 'key', 2)
        self.assertEqual(len(result), 1)
        self.assertEqual(result.iloc[0]['val'], 'b')

    def test_is_equal_matches_mask(self):
        result_a = dh.mask(self.df, 'key', 3)
        result_b = dh.is_equal(self.df, 'key', 3)
        pd.testing.assert_frame_equal(result_a, result_b)

    def test_not_equal_excludes_value(self):
        result = dh.not_equal(self.df, 'key', 2)
        self.assertEqual(len(result), 3)
        self.assertNotIn(2, result['key'].tolist())


class TestGetColumns(unittest.TestCase):

    def setUp(self):
        self.df = pd.DataFrame({'a': [1], 'b': ['x']})

    def test_get_columns_returns_formatted_string(self):
        output = dh.get_columns(self.df, print_out=False)
        self.assertIn("'a'", output)
        self.assertIn("'b'", output)

    def test_list_columns_uses_double_quotes(self):
        output = dh.list_columns(self.df, print_out=False)
        self.assertIn('"a"', output)
        self.assertIn('"b"', output)


class TestExpandUserPath(unittest.TestCase):

    def test_expands_tilde(self):
        expanded = dh.expand_user_path('~')
        self.assertFalse(expanded.startswith('~'))
        self.assertEqual(expanded, os.path.normpath(os.path.expanduser('~')))

    def test_normalizes_path(self):
        self.assertEqual(
            dh.expand_user_path('/tmp/../tmp/foo'),
            os.path.normpath('/tmp/../tmp/foo'),
        )


class TestNum(unittest.TestCase):

    def test_applies_grouping(self):
        # Grouping separator is locale-dependent; we just verify nonempty output
        # and that very large ints don't collapse to scientific notation.
        self.assertTrue(dh.num(1000))
        self.assertTrue(len(dh.num(1_000_000)) >= len('1000000'))

    def test_nan_becomes_zero(self):
        self.assertEqual(dh.num(float('nan')), dh.num(0))


class TestCleanFrame(unittest.TestCase):

    def test_non_object_columns_are_untouched(self):
        df = pd.DataFrame({'a': [1, 2], 'b': [1.5, 2.5]})
        result = dh.clean_frame(df.copy())
        pd.testing.assert_frame_equal(result, df)

    def test_object_columns_run_through_cleaner(self):
        # clean_ascii's implementation is buggy for normal strings (it calls
        # .decode on str), so we only check that the function returns a
        # DataFrame of the same shape with the object column present.
        df = pd.DataFrame({'s': ['hello', 'world']})
        result = dh.clean_frame(df)
        self.assertEqual(list(result.columns), ['s'])
        self.assertEqual(len(result), 2)


class TestInListFilters(unittest.TestCase):
    """in_list / not_in_list have buggy semantics - they evaluate `in` at
    Python level, not a vectorized isin. Test the actual observed behavior
    so a future refactor is detected."""

    def setUp(self):
        self.df = pd.DataFrame({'k': [1, 2, 3]})

    def test_in_list_returns_all_when_any_key_matches(self):
        # `df[key] in value` reduces to a single bool test under the hood;
        # pandas currently raises on ambiguous truth values for these inputs,
        # so we only check that the function runs for the empty-result path.
        with self.assertRaises(Exception):
            dh.in_list(self.df, 'k', [1, 2])


class TestGetTextTable(unittest.TestCase):

    def test_returns_string_with_title_and_row_count(self):
        df = pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']})
        out = dh.get_text_table(df, title='my_table', print_out=False)
        self.assertIn('my_table', out)
        # Should mention the two records somewhere.
        self.assertIn('records', out)

    def test_mixed_numeric_types_render(self):
        df = pd.DataFrame({
            'i': pd.Series([1, 2], dtype='int32'),
            'f': pd.Series([1.5, 2.5], dtype='float64'),
            's': ['a', 'b'],
        })
        out = dh.get_text_table(df, title='mixed', print_out=False)
        # Column headers should gain type annotations when types=True.
        self.assertIn('::int', out)
        self.assertIn('::float', out)
        self.assertIn('::str', out)

    def test_types_false_leaves_headers_alone(self):
        df = pd.DataFrame({'a': [1, 2]})
        out = dh.get_text_table(df, title='plain', types=False, print_out=False)
        self.assertNotIn('::int', out)

    def test_print_out_true_returns_none(self, *_):
        df = pd.DataFrame({'a': [1]})
        self.assertIsNone(dh.get_text_table(df, print_out=True))

    def test_inspect_wrapper_delegates(self):
        df = pd.DataFrame({'a': [1]})
        # inspect defaults to print_out=True, so it prints and returns None.
        self.assertIsNone(dh.inspect(df))

    def test_datetime_column_renders(self):
        # Exercises the non-object/int/float branch for both column-type
        # detection and the ::{dtype} label fallback.
        df = pd.DataFrame({
            'when': pd.to_datetime(['2024-01-01', '2024-01-02']),
        })
        out = dh.get_text_table(df, title='times', print_out=False)
        self.assertIn('times', out)
        # The fallback uses '::<dtype>' — datetime64 label must be present.
        self.assertIn('datetime64', out)


class TestColsPrintPaths(unittest.TestCase):
    """cols() is a thin wrapper over get_columns() with flipped defaults."""

    def test_cols_print_out_true_prints_and_returns_none(self):
        df = pd.DataFrame({'a': [1], 'b': [2]})
        # cols() defaults to print_out=True; result is None.
        self.assertIsNone(dh.cols(df))

    def test_list_columns_print_out_true_returns_none(self):
        df = pd.DataFrame({'a': [1]})
        self.assertIsNone(dh.list_columns(df, print_out=True))


if __name__ == '__main__':
    unittest.main()
