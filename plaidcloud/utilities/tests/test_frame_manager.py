#!/usr/bin/env python
# coding=utf-8

import unittest
import pytest
import numpy as np
import pandas as pd
from plaidcloud.utilities import frame_manager
from plaidcloud.utilities.frame_manager import coalesce

__author__ = "Andrew Hodgson"
__copyright__ = "© Copyright 2009-2014, Tartan Solutions, Inc"
__credits__ = ["Paul Morel", "Adams Tower"]
__license__ = "Apache 2.0"
__maintainer__ = "Andrew Hodgson"
__email__ = "andrew.hodgson@tartansolutions.com"

nan = np.nan


# Test to see that 2 data frames are equal
# http://stackoverflow.com/questions/14224172/equality-in-pandas-dataframes-column-order-matters
def assertFrameEqual(df1, df2, **kwargs):
    """ Assert that two dataframes are equal, ignoring ordering of columns

    Args:
        df1 (`pandas.DataFrame`): The DataFrame to compare against `df2`
        df2 (`pandas.DataFrame`): The DataFrame to compare against `df1`
        **kwargs (dict): A dict to pass to `pandas.util.testing.assert_frame_equal`
    """
    from pandas.testing import assert_frame_equal
    return assert_frame_equal(df1, df2, check_names=True, check_like=True, **kwargs)


class TestFrameManager(unittest.TestCase):

    """These tests validate the data model methods"""

    def setUp(self):
        "Constructs a test environment if necessary"
        self.df = frame_manager.pd.DataFrame([('Andrew', 31, 500), ('Optimus', 30, 1000), ('Iron Man', 51, 1250), ('Batman', 75, 50), ('Andrew', 31, 2500)], columns=['Name', 'Age', 'Points'])

        # duplicate
        self.df2 = frame_manager.pd.DataFrame([('Andrew', 31, 500), ('Optimus', 30, 1000), ('Iron Man', 51, 1250), ('Batman', 75, 50), ('Andrew', 31, 2500)], columns=['Name', 'Age', 'Points'])

        self.df9 = frame_manager.pd.DataFrame([('Andrew', 31, 5), ('Optimus', 30, 10), ('Iron Man', 51, 12), ('Batman', 75, 11)], columns=['Name', 'age', 'Level'])

        # Deadpool is villain aged 23... not listed
        self.df3 = frame_manager.pd.DataFrame([(30, 'Autobot'), (51, 'Superhero'), (75, 'Superhero'), (23, 'Villain')], columns=['Age', 'Title'])

        self.df_blank = frame_manager.pd.DataFrame()

        self.df_mon_val = frame_manager.pd.DataFrame([('Jan', 5), ('Feb', 10), ('Mar', 15), ('Jan', 20), ('Feb', 25), ('Mar', 30)], columns = ['mon', 'val'])

        self.df6 = frame_manager.pd.DataFrame([(30, 'Autobot', 2354, 0), (30, 'Decepticon', 18, 0), (51, 'Superhero', 234, 0), (75, 'Superhero', 897, 0), (23, 'Villain', 46546, 0)], columns=['Age', 'Title', 'DropMe', 'Points'])

#    def test_get_frame_model_path(self):
#        pass

#    def test_get_frame_zone_path(self):
#        pass

#    def test_load_frame(self):
#        pass

#    def test_load_frame_meta(self):
#        pass

#    def test_clear_frame(self):
#        pass

#    def test_clear_zone_frame(self):
#        pass

#    def test_load_zone_frame(self):
#        pass

#    def test_load_zone_frame_meta(self):
#        pass

#    def test_save_frame(self):
#        pass

#    def test_get_tmp_frame_path(self):
#        pass

#    def test_compress_frame(self):
#        pass

#    def test_uncompress_frame(self):
#        pass

#    def test_append_frame(self):
#        #x = frame_manager.append_frame(
#        pass

    def test_describe(self):
        """Tests to verify descriptive statistics about data frame"""
        x = frame_manager.describe(self.df)
        self.assertEqual(x['Age']['max'], max(self.df['Age']))
        self.assertEqual(x['Points']['min'], min(self.df['Points']))
        self.assertEqual(x['Age']['mean'], np.mean(self.df['Age']))
        self.assertEqual(x['Points']['mean'], np.mean(self.df['Points']))

    def test_count_unique(self):
        """Tests to verify count of distinct records in data frame"""
        x = frame_manager.count_unique('Name', 'Points', self.df)
        y = self.df.groupby('Name').count()['Age']['Andrew']
        z = self.df.groupby('Name').count()['Age']['Iron Man']
        self.assertEqual(x['Andrew'], y)
        self.assertEqual(x['Iron Man'], z)

    def test_sum(self):
        """Tests to verify sum of records in data frame"""
        x = frame_manager.sum('Name', self.df)
        y = self.df.groupby('Name').sum()
        self.assertEqual(x['Points']['Andrew'], y['Points']['Andrew'])
        self.assertEqual(x['Age']['Batman'], y['Age']['Batman'])

    def test_std(self):
        """Tests to verify standard deviation of records in data frame"""
        x = frame_manager.std('mon', self.df_mon_val)
        y = self.df_mon_val.groupby('mon').std()
        assertFrameEqual(x, y)

    def test_mean(self):
        """Tests to verify mean of records in data frame"""
        x = frame_manager.mean('Name', self.df)
        y = self.df.groupby(['Name']).mean()
        self.assertEqual(x['Points'][1], y['Points'][1])

    def test_count(self):
        """Tests to verify count of records in data frame"""
        x = frame_manager.count('Name', self.df)
        y = self.df.groupby('Name').count()
        self.assertEqual(x['Points'][1], y['Points'][1])

    def test_inner_join(self):
        """Tests to verify inner join capability"""
        x = frame_manager.inner_join(self.df, self.df3, ['Age'])
        y = frame_manager.pd.merge(self.df, self.df3, 'inner', ['Age'])
        assertFrameEqual(x, y)

    def test_outer_join(self):
        """Tests to verify outer join capability"""
        x = frame_manager.outer_join(self.df, self.df3, ['Age'])
        y = frame_manager.pd.merge(self.df, self.df3, 'outer', ['Age'])
        assertFrameEqual(x, y)

    def test_left_join(self):
        """Tests to verify left join capability"""
        x = frame_manager.left_join(self.df, self.df3, ['Age'])
        y = frame_manager.pd.merge(self.df, self.df3, 'left', ['Age'])
        assertFrameEqual(x, y)

    def test_right_join(self):
        """Tests to verify right join capability"""
        x = frame_manager.right_join(self.df, self.df3, ['Age'])
        y = frame_manager.pd.merge(self.df, self.df3, 'right', ['Age'])
        assertFrameEqual(x, y)

#    def test_memoize(self):
#        pass

#    def test_geo_distance(self):
#        pass

#    def test_geo_location(self):
#        pass

#    def test_trailing_negative(self):
#        pass

    def test_now(self):
        """Tests to verify current time"""
        x = frame_manager.now()
        y = frame_manager.utc.timestamp()
        self.assertEqual(x, y)

#    def test_concat(self):
#        df2 = self.df
#        x = frame_manager.concat([self.df, df2], [self.df])
#        print x

#    def test_covariance(self):
#        pass

#    def test_correlation(self):
#        pass

#    def test_apply_agg(self):
#        pass

#    def test_distinct(self):
#        pass

#    def test_find_duplicates(self):
#        pass

#    def test_sort(self):
#        pass

#    def test_replace_column(self):
#        pass

    def test_replace(self):
        """Tests to verify replacement using dictionary key/value combinations"""
        replace_dict = {'Optimus': 'Optimus Prime', 50: 5000}
        x = frame_manager.replace(self.df, replace_dict)
        y = self.df.replace(replace_dict)
        assertFrameEqual(x, y)

#    def test_reindex(self):
#        pass

    def test_rename_columns(self):
        """Tests to verify renamed columns using dictionary key/value combinations"""
        rename_dict = {'Name': 'Title', 'Points': 'Salary'}
        x = frame_manager.rename_columns(self.df, rename_dict)
        y = self.df.rename(columns=rename_dict)
        assertFrameEqual(x, y)

#    def test_column_info(self):
#        pass

    @pytest.mark.skip('Dtypes seem to be wrong, should be passing sql types?')
    def test_set_column_types(self):
        """Tests to verify data type conversion for columns"""
        type_dict = {'Name': 's32', 'Points': 'float16', 'Age': 'int8'}
        self.assertNotEqual('int8', self.df['Age'].dtypes)
        self.assertNotEqual('float16', self.df['Points'].dtypes)

        x = frame_manager.set_column_types(self.df, type_dict)
        self.assertEqual('float32', x['Points'].dtypes)
        self.assertEqual('int64', x['Age'].dtypes)
        self.assertEqual('object', x['Name'].dtypes)

    def test_drop_column(self):
        """Tests to verify columns dropped appropriately"""
        x = frame_manager.drop_column(self.df, ['Age'])
        y = self.df2
        del y['Age']
        assertFrameEqual(x, y)

    def test_has_data(self):
        """Tests to verify a data frame does/doesn't have data"""
        x = frame_manager.has_data(self.df_blank)
        y = frame_manager.has_data(self.df)
        self.assertFalse(x)
        self.assertTrue(y)

#    def test_in_column(self):
#        pass

#    def test_frame_source_reduce(self):
#        """Tests to verify that data is filtered as expected (aka SQL Where)"""
#        x = frame_manager.frame_source_reduce(self.df)
#        assertFrameEqual(x, self.df2)

#    def test_apply_variables(self):
#        pass

#    def test_frame_map_update(self):
#        pass

#    def test_get_entity_frame(self):
#        pass

#    def test_save_entity_frame(self):
#        pass

    def test_lookup(self):
        """Tests to verify lookup capability"""
        # x = frame_manager.lookup(self.df, self.df6, ['Age'], None, ['Age', 'Title'])
        orig_lookup = self.df6.copy()
        w = frame_manager.lookup(self.df, self.df9, left_on=['Name', 'Age'], right_on=['Name', 'age'])
        print(w)
        x = frame_manager.lookup(self.df, self.df6, ['Age'])
        y = frame_manager.distinct(self.df6, ['Age'])
        z = frame_manager.left_join(self.df, y, ['Age'])
        print(x)
        print(z)
        assertFrameEqual(x, z)
        # ensure lookup frame integrity
        assertFrameEqual(orig_lookup, self.df6)

    def tearDown(self):
        "Clean up any test structure or records generated during the testing"
        del self.df
        del self.df2
        del self.df_blank
        del self.df_mon_val
        del self.df6


class TestCoalesce(unittest.TestCase):
    def setUp(self):
        self.reference_data = {
            'A': [nan, 'aa', nan, nan, nan],
            'B': ['b', 'bb', None, nan, 'bbbbb'],
            'C': ['c', 'cc', 'ccc', 'cccc', 'ccccc'],
            'D': ['d', '', nan, nan, nan],
            'E': ['e', 'ee', nan, None, 7],
            'one': [1, nan, nan, nan, nan],  # float64
            'two': [2, 2, 2.2, nan, 0],  # float64
            'three': [nan, nan, nan, 3, 3]
        }

    def test_string_columns(self):
        """Test the basic case with strings."""

        df = pd.DataFrame(data=self.reference_data)

        # Two columns
        result = coalesce(df['A'], df['C'])
        self.assertTrue(
            (result == pd.Series(['c', 'aa', 'ccc', 'cccc', 'ccccc']))
            .all()
        )

        # Three columns
        result = coalesce(df['A'], df['D'], df['C'])
        self.assertTrue(
            (result == pd.Series(['d', 'aa', 'ccc', 'cccc', 'ccccc']))
            .all()
        )

        # None is equivalent to NaN
        result = coalesce(df['B'], df['C'])
        self.assertTrue(
            (result == pd.Series(['b', 'bb', 'ccc', 'cccc', 'bbbbb']))
            .all()
        )

    def test_one_column(self):
        """Test that using one column is a no-op, returning no changes."""

        df = pd.DataFrame(data=self.reference_data)

        for c in df.columns:
            col = df.loc[:, c]
            result = coalesce(col)
            self.assertTrue((result.fillna('nan') == col.fillna('nan')).all())
            self.assertTrue((result.index == col.index).all())

    def test_value_preservation(self):
        """Make sure valid values aren't overwritten by nulls."""

        df = pd.DataFrame(data=self.reference_data)

        result = coalesce(df['C'], df['A'])
        self.assertTrue((result == df['C']).all())

    def test_numeric_columns(self):
        """Test the basic case with numbers."""

        df = pd.DataFrame(data=self.reference_data)

        # Two columns
        result = coalesce(df['one'], df['two'])
        result = result.fillna('nan')
        self.assertTrue(
            (result == pd.Series([1., 2., 2.2, 'nan', 0.]))
            .all()
        )

        # Three columns
        result = coalesce(df['one'], df['two'], df['three'])
        self.assertTrue(
            (result == pd.Series([1., 2., 2.2, 3., 0.]))
            .all()
        )

    def test_index_mismatch(self):
        """Indexes can be different as long as they're the same length.
        The returned Series will have an index matching the first column's."""

        df = pd.DataFrame(data=self.reference_data)

        # Same-length columns with mismatched indexes compare just fine.
        a = df.loc[:, 'A']
        a.index = test_index = ['v', 'w', 'x', 'y', 'z']

        result = coalesce(a, df['C'])
        self.assertTrue(
            (result.index == test_index)
            .all()
        )
        self.assertTrue(
            (result.index != df['C'].index)
            .all()
        )
        self.assertTrue(
            (result.values == pd.Series(['c', 'aa', 'ccc', 'cccc', 'ccccc']).values)
            .all()
        )

        # Columns must be the same length, however.
        too_short = pd.Series(['foo', 'bar'])
        too_long = pd.Series(['foo', 'bar', 'baz', 'qux', 'quux', 'corge'])

        with self.assertRaises(Exception):
            result = coalesce(a, too_short)

        with self.assertRaises(Exception):
            result = coalesce(a, too_long)

    def test_cross_type_comparison(self):
        """Cross type comparison is allowed in the standard use case."""

        df = pd.DataFrame(data=self.reference_data)

        result = coalesce(df['A'], df['one'], df['E'])
        result = result.fillna('nan')
        self.assertTrue(
            (result == pd.Series([1, 'aa', 'nan', 'nan', 7]))
            .all()
        )

    def test_consider_null(self):
        """Test the optional keyword argument test_consider_null."""

        df = pd.DataFrame(data=self.reference_data)

        # Maybe zero is a bad number. Consider it null.
        result = coalesce(df['two'], df['three'], consider_null=[0])
        self.assertTrue(
            (result == pd.Series([2, 2, 2.2, 3, 3]))
            .all()
        )

        # consider_nulls takes multiple values.
        result = coalesce(df['D'], df['C'], consider_null=['d', ''])
        self.assertTrue(
            (result == pd.Series(['c', 'cc', 'ccc', 'cccc', 'ccccc']))
            .all()
        )

    @pytest.mark.skipif(pd.__version__ >= '0.24', reason='Pandas v0.24+ allows this comparison')
    def test_consider_null_cross_type(self):
        """Test the optional keyword argument test_consider_null."""
        df = pd.DataFrame(data=self.reference_data)

        # Don't allow cross-type comparison with mixed types...
        # ...for now.
        with self.assertRaises(TypeError):
            coalesce(df['two'], df['C'], consider_null=['cccc'])


if __name__ == '__main__':
    unittest.TestProgram()

# verbose testing
# suite = unittest.TestLoader().loadTestsFromTestCase(TestFrameManager)
# unittest.TextTestRunner(verbosity=2).run(suite)
