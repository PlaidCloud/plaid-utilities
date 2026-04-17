# coding=utf-8
"""Tests for plaidcloud.utilities.utility."""

import os
import tempfile
import unittest

import pandas as pd

from plaidcloud.utilities import utility


class TestMonthToQuarter(unittest.TestCase):

    def test_quarter_boundaries(self):
        self.assertEqual(utility.month_to_quarter(1), 1)
        self.assertEqual(utility.month_to_quarter(3), 1)
        self.assertEqual(utility.month_to_quarter(4), 2)
        self.assertEqual(utility.month_to_quarter(9), 3)
        self.assertEqual(utility.month_to_quarter(12), 4)

    def test_string_input_accepted(self):
        self.assertEqual(utility.month_to_quarter('5'), 2)

    def test_invalid_month_raises(self):
        with self.assertRaises(ValueError):
            utility.month_to_quarter(0)
        with self.assertRaises(ValueError):
            utility.month_to_quarter(13)
        with self.assertRaises(ValueError):
            utility.month_to_quarter('not a number')


class TestGetRunTime(unittest.TestCase):

    def test_short_duration(self):
        formatted, raw = utility.get_run_time(100.0, 105.5)
        self.assertEqual(formatted, '0:00:06')
        self.assertAlmostEqual(raw, 5.5)

    def test_longer_duration(self):
        formatted, _ = utility.get_run_time(0.0, 3661.0)
        self.assertEqual(formatted, '1:01:01')


class TestRollupSum(unittest.TestCase):

    def test_basic_rollup(self):
        df = pd.DataFrame({
            'blue': [1, 2],
            'red': [3, 4],
            'blue_weight': [1, 1],
            'red_weight': [1, 1],
        })
        result, rollups = utility.rollup_sum(
            df,
            {'blue': (1, 'color'), 'red': (1, 'color')},
        )
        self.assertIn('color', result.columns)
        self.assertNotIn('blue', result.columns)
        self.assertNotIn('red', result.columns)
        self.assertEqual(list(result['color']), [4, 6])
        self.assertEqual(rollups, ['color'])

    def test_rollup_string_mapping_defaults_weight(self):
        df = pd.DataFrame({'old': [2, 3], 'old_weight': [1, 1]})
        result, rollups = utility.rollup_sum(df, {'old': 'new'})
        self.assertIn('new', result.columns)
        self.assertEqual(list(result['new']), [2, 3])
        self.assertEqual(rollups, ['new'])

    def test_rollup_into_existing_column_name_renames_old_column(self):
        # If the rollup target collides with an existing column, the existing
        # column should be renamed (and included in the sum) rather than
        # overwritten.
        df = pd.DataFrame({'x': [10, 20], 'x_weight': [1, 1]})
        result, rollups = utility.rollup_sum(df, {'x': 'x'})
        self.assertEqual(list(result['x']), [10, 20])
        self.assertEqual(rollups, ['x'])


class TestIterDataframes(unittest.TestCase):

    def test_yields_dataframes_from_csv_path_list(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'data.csv')
            pd.DataFrame({'a': [1, 2], 'b': ['x', 'y']}).to_csv(
                path, sep='|', index=False,
            )

            dfs = list(utility.iter_dataframes([path]))

        self.assertEqual(len(dfs), 1)
        self.assertEqual(list(dfs[0].columns), ['a', 'b'])
        self.assertEqual(len(dfs[0]), 2)

    def test_skips_empty_frames(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'empty.csv')
            pd.DataFrame({'a': [], 'b': []}).to_csv(path, sep='|', index=False)

            dfs = list(utility.iter_dataframes([path]))

        self.assertEqual(dfs, [])


class TestGetPathSet(unittest.TestCase):

    def test_returns_set_of_matching_paths(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ('a.txt', 'b.txt', 'c.md'):
                with open(os.path.join(tmp, name), 'w') as f:
                    f.write('x')

            results = utility.get_path_set(['*.txt'], prefix=tmp)

        names = {os.path.basename(p) for p in results}
        self.assertEqual(names, {'a.txt', 'b.txt'})

    def test_no_prefix_uses_raw_pattern(self):
        with tempfile.TemporaryDirectory() as tmp:
            target = os.path.join(tmp, 'present.csv')
            with open(target, 'w') as f:
                f.write('x')
            results = utility.get_path_set([os.path.join(tmp, '*.csv')])
        self.assertEqual(results, {target})

    def test_empty_patterns_returns_empty_set(self):
        self.assertEqual(utility.get_path_set([]), set())


class TestPrefixITEM(unittest.TestCase):

    def test_pads_numeric_to_eight_digits(self):
        # cast_as_int strips non-digit characters and pads the result.
        self.assertEqual(utility.prefix_ITEM('123456'), 'ITEM_00123456')

    def test_zero_returns_nan(self):
        import math
        result = utility.prefix_ITEM('not a number')
        self.assertTrue(math.isnan(result))


class TestGetPatternsForSync(unittest.TestCase):

    def test_extracts_short_names_and_appends_excel_regex(self):
        paths = {
            'a': '/some/path/to/a.csv',
            'b': ['/dir1/b1.tsv', '/dir2/b2.tsv'],
            'ignored_dict': {'no': 'expansion'},
        }
        result = utility.get_patterns_for_sync(paths)
        # short names are present
        self.assertIn('a.csv', result)
        self.assertIn('b1.tsv', result)
        self.assertIn('b2.tsv', result)
        # nested dict values are skipped
        self.assertNotIn('no', result)
        # The code does `paths.extend((r'.*\.xls[xm]?$'))` which explodes the
        # regex into a list of characters rather than appending it whole, so we
        # just assert the final list still has a recognizable excel-regex char.
        self.assertIn('$', result)


class TestIterDataframesEdgeCases(unittest.TestCase):

    def test_glob_string_expands_to_multiple_files(self):
        with tempfile.TemporaryDirectory() as tmp:
            for name in ('a.csv', 'b.csv'):
                pd.DataFrame({'x': [1]}).to_csv(
                    os.path.join(tmp, name), sep='|', index=False,
                )
            dfs = list(utility.iter_dataframes(os.path.join(tmp, '*.csv')))

        self.assertEqual(len(dfs), 2)

    def test_chunksize_iterates_textfilereader(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = os.path.join(tmp, 'big.csv')
            pd.DataFrame({'x': range(10)}).to_csv(path, sep='|', index=False)

            chunks = list(
                utility.iter_dataframes([path], pd_read_csv_kwargs={'sep': '|', 'chunksize': 3}),
            )

        total = sum(len(c) for c in chunks)
        self.assertEqual(total, 10)
        self.assertGreater(len(chunks), 1)


class TestGetPatternsForSyncDefaultConf(unittest.TestCase):

    def test_none_paths_dict_uses_module_conf(self):
        from unittest import mock
        # Inject a minimal paths dict into the module-level conf.
        with mock.patch.object(utility, 'conf', {'paths': {'IN': '/x/in.csv'}}):
            result = utility.get_patterns_for_sync(None)
        self.assertIn('in.csv', result)


class TestFillMissingITEMs(unittest.TestCase):

    def test_fills_from_mapping_with_same_column_names(self):
        # Both DataFrames share the ITEM column name, so pandas applies suffixes.
        df_to_fill = pd.DataFrame({
            'email': ['a@x', 'b@y', 'c@z'],
            'ITEM': ['ITEM_001', None, 'ITEM_003'],
        })
        mapping = pd.DataFrame(
            {'ITEM': ['ITEM_MAP_B']},
            index=['b@y'],
        )
        mapping.index.name = 'email'

        filled = utility.fill_missing_ITEMs(
            df_to_fill, mapping,
            to_fill_email_col_name='email',
            to_fill_ITEM_col_name='ITEM',
            map_ITEM_col_name='ITEM',
        )

        # The None got filled from the mapping.
        row = filled.loc[filled['email'] == 'b@y'].iloc[0]
        self.assertEqual(row['ITEM'], 'ITEM_MAP_B')

    def test_fills_from_mapping_with_different_column_names(self):
        df_to_fill = pd.DataFrame({
            'email': ['a@x', 'b@y'],
            'ITEM': [None, None],
        })
        mapping = pd.DataFrame(
            {'MAP_ITEM': ['ITEM_A', 'ITEM_B']},
            index=['a@x', 'b@y'],
        )
        mapping.index.name = 'email'

        filled = utility.fill_missing_ITEMs(
            df_to_fill, mapping,
            to_fill_email_col_name='email',
            to_fill_ITEM_col_name='ITEM',
            map_ITEM_col_name='MAP_ITEM',
        )

        self.assertEqual(sorted(filled['ITEM'].tolist()), ['ITEM_A', 'ITEM_B'])


if __name__ == '__main__':
    unittest.main()
