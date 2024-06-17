#!/usr/bin/env python
# coding=utf-8

import filecmp
import os
import unittest
from unittest import TestCase

import numpy as np
import pandas as pd
from pandas.testing import assert_frame_equal

from plaidcloud.utilities.connect import create_connection
from plaidcloud.utilities.remote.dimension import Dimensions
from plaidcloud.utilities.remote.dimension import MAIN
from plaidcloud.utilities.remote.dimension import ROOT

__author__ = 'Dave Parsons'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Dave Parsons']
__license__ = 'Proprietary'
__maintainer__ = 'Dave Parsons'
__email__ = 'dave.parsons@tartansolutions.com'

# Folders for comparison
BASELINE = './dim_baseline/'
FOLDER = './dim_current/'


conn = create_connection(verify_ssl=False)


class TestDimension(TestCase):
    """Test Redis Dimension code"""

    def assertFileEqual(self, file1, file2, **kwargs):
        return self.assertTrue(filecmp.cmp(file1, file2, shallow=False))

    def assertFrameEqual(self, df1, df2, **kwargs):
        return assert_frame_equal(df1, df2, check_names=True, check_like=True, **kwargs)

    def setUp(self):
        if not os.path.exists(BASELINE):
            os.makedirs(BASELINE)
        self.periods = 'periods_rpc_test'
        self.dims = Dimensions(conn=conn)
        self.dim = self.dims.get_dimension(name=self.periods, replace=False)
        return

    def test_001_load_hierarchy_main(self):
        df_main = pd.DataFrame(
            [
                [ROOT, 'Year'],
                ['Year', 'Q1'],
                ['Year', 'Q2'],
                ['Year', 'Q3'],
                ['Year', 'Q4'],
                ['Q1', 'January'],
                ['Q1', 'February'],
                ['Q1', 'March'],
                ['Q2', 'April'],
                ['Q2', 'May'],
                ['Q2', 'June'],
                ['Q3', 'July'],
                ['Q3', 'August'],
                ['Q3', 'September'],
                ['Q4', 'October'],
                ['Q4', 'November'],
                ['Q4', 'December'],
            ],
            columns=['ParentName', 'ChildName']
        )

        # Clear down the dimension and reload
        self.dim.clear()

        # main hierarchy
        df_results = self.dim.load_hierarchy_from_dataframe(df_main, 'ParentName', 'ChildName')
        df_results.to_csv(f'{FOLDER}df_main_load.csv', index=False)

        # Create a backup file to allow reloading in tests
        data = self.dims.backup(self.periods)
        with open(f'{FOLDER}periods.yaml', 'w') as file:
            file.write(data)

        self.assertFileEqual(f'{FOLDER}df_main_load.csv', f'{BASELINE}df_main_load.csv')
        return

    def test_002_save_hierarchy_main(self):
        # main hierarchy
        df = self.dim.save_hierarchy_to_dataframe(MAIN)
        df.drop(labels='index', axis=1, inplace=True)
        df.to_csv(f'{FOLDER}df_main_hierarchy.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_main_hierarchy.csv', f'{BASELINE}df_main_hierarchy.csv')
        return

    def test_003_load_hierarchy_halves(self):
        df_halves = pd.DataFrame(
            [
                [ROOT, 'H1', '~', 'halves'],
                [ROOT, 'H2', '~', 'halves'],
                ['H1', 'Q1', '+', 'halves'],
                ['H1', 'Q2', '+', 'halves'],
                ['H2', 'Q3', '+', 'halves'],
                ['H2', 'Q4', '+', 'halves'],
            ],
            columns=['ParentName', 'ChildName', 'ConsolidationType', 'Hierarchy']
        )

        # halves hierarchy
        df_results = self.dim.load_hierarchy_from_dataframe(df_halves, 'ParentName', 'ChildName',
                                                            'ConsolidationType', hierarchy='Hierarchy')
        df_results.to_csv(f'{FOLDER}df_halves_load.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_halves_load.csv', f'{BASELINE}df_halves_load.csv')
        return

    def test_004_save_hierarchy_halves(self):
        # halves hierarchy
        df = self.dim.save_hierarchy_to_dataframe('halves')
        df.drop(labels='index', axis=1, inplace=True)
        df.to_csv(f'{FOLDER}df_halves_hierarchy.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_halves_hierarchy.csv', f'{BASELINE}df_halves_hierarchy.csv')
        return

    def test_005_load_hierarchy_financial(self):
        df_financial = pd.DataFrame(
            [
                [ROOT, 'YTD', '+', 'financial'],
                [ROOT, 'YTG', '+', 'financial'],
                ['YTD', 'January', '+', 'financial'],
                ['YTD', 'February', '+', 'financial'],
                ['YTD', 'March', '+', 'financial'],
                ['YTD', 'April', '+', 'financial'],
                ['YTG', 'May', '-', 'financial'],
                ['YTG', 'June', '-', 'financial'],
                ['YTG', 'July', '-', 'financial'],
                ['YTG', 'August', '-', 'financial'],
                ['YTG', 'September', '-', 'financial'],
                ['YTG', 'October', '-', 'financial'],
                ['YTG', 'November', '-', 'financial'],
                ['YTG', 'December', '-', 'financial'],
            ],
            columns=['ParentName', 'ChildName', 'ConsolidationType', 'Hierarchy']
        )

        # financial hierarchy
        df_results = self.dim.load_hierarchy_from_dataframe(df_financial, 'ParentName', 'ChildName',
                                                            'ConsolidationType', hierarchy='Hierarchy')
        df_results.to_csv(f'{FOLDER}df_financial_load.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_financial_load.csv', f'{BASELINE}df_financial_load.csv')
        return

    def test_006_save_hierarchy_financial(self):
        # financial hierarchy
        df = self.dim.save_hierarchy_to_dataframe('financial')
        df.drop(labels='index', axis=1, inplace=True)
        df.to_csv(f'{FOLDER}df_financial_hierarchy.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_financial_hierarchy.csv', f'{BASELINE}df_financial_hierarchy.csv')
        return

    def test_007_load_hierarchy_errors(self):
        # This dataframe includes specific errors so check out the results dataframe
        df_test = pd.DataFrame(
            [
                ['', '', '+', 'main'],
                ['   ', '   ', '+', 'main'],
                ['Q5', '', '+', 'main'],
                [np.nan, np.nan, '+', 'main'],
                [None, None, '+', 'main'],
                ['None', 'None', '+', 'main'],
                ['Q5', 'Q5', '+', 'main'],
                ['Q5', ROOT, '+', 'main'],
                ['Q5', 'Donk:tober', '+', 'main'],
                ['Donk:tober', 'Janusday', '+', 'main'],
                ['Year', 'Q5', '+', 'main'],
                ['Year', 'Q5', '+', 'main'],
                ['Q4', 'Badtober', '+', 'halves'],
                ['Q6', 'Craptober', '+', ''],
            ],
            columns=['ParentName', 'ChildName', 'ConsolidationType', 'Hierarchy']
        )

        df_results = self.dim.load_hierarchy_from_dataframe(df_test, 'ParentName', 'ChildName',
                                                            'ConsolidationType', hierarchy='Hierarchy')
        df_results.to_csv(f'{FOLDER}df_complex_load.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_complex_load.csv', f'{BASELINE}df_complex_load.csv')

        return

    def test_008_load_save_aliases(self):
        df_aliases = pd.DataFrame(
            [
                ['Trimestre 1', 'French', 'Q1'],
                ['Trimestre 2', 'French', 'Q2'],
                ['Trimestre 3', 'French', 'Q3'],
                ['Trimestre 4', 'French', 'Q4'],
                ['Janvier', 'French', 'January'],
                ['Fevier', 'French', 'February'],
                ['Mars', 'French', 'March'],
                ['Avril', 'French', 'April'],
                ['Mai', 'French', 'May'],
                ['Juin', 'French', 'June'],
                ['Julliet', 'French', 'July'],
                ['Aout', 'French', 'August'],
                ['Septembre', 'French', 'September'],
                ['Octobre', 'French', 'October'],
                ['Novembre', 'French', 'November'],
                ['Decembre', 'French', 'December'],
                ['Haneri 1', 'Welsh', 'H1'],
                ['Haneri 2', 'Welsh', 'H2'],
                ['Ionawr', 'Welsh', 'January'],
                ['Chwefror', 'Welsh', 'February'],
                ['Mawrth', 'Welsh', 'March'],
                ['Ebrill', 'Welsh', 'April'],
                ['Mai', 'Welsh', 'May'],
                ['Mehefin', 'Welsh', 'June'],
                ['Gorffennaf', 'Welsh', 'July'],
                ['Awst', 'Welsh', 'August'],
                ['Medi', 'Welsh', 'September'],
                ['Hydref', 'Welsh', 'October'],
                ['Tachwedd', 'Welsh', 'November'],
                ['Rhagfyr', 'Welsh', 'December'],
                ['Январь', 'Russian', 'January'],
                ['Февраль', 'Russian', 'February'],
                ['Март', 'Russian', 'March'],
                ['Апрель', 'Russian', 'April'],
                ['Май', 'Russian', 'May'],
                ['Июнь', 'Russian', 'June'],
                ['Июль', 'Russian', 'July'],
                ['Август', 'Russian', 'August'],
                ['Сентябрь', 'Russian', 'September'],
                ['Октябрь', 'Russian', 'October'],
                ['Ноябрь', 'Russian', 'November'],
                ['Декабрь', 'Russian', 'December'],
                ['일월', 'Korean', 'January'],
                ['이월', 'Korean', 'February'],
                ['삼월', 'Korean', 'March'],
                ['사월', 'Korean', 'April'],
                ['오월', 'Korean', 'May'],
                ['유월', 'Korean', 'June'],
                ['칠월', 'Korean', 'July'],
                ['팔월', 'Korean', 'August'],
                ['구월', 'Korean', 'September'],
                ['시월', 'Korean', 'October'],
                ['십일월', 'Korean', 'November'],
                ['십이월', 'Korean', 'December'],
                ['☃️', 'Emoji', 'January'],
                ['💘', 'Emoji', 'February'],
                ['☘️', 'Emoji', 'March'],
                ['☔', 'Emoji', 'April'],
                ['🌺', 'Emoji', 'May'],
                ['🌞', 'Emoji', 'June'],
                ['🍦', 'Emoji', 'July'],
                ['🏖️', 'Emoji', 'August'],
                ['🍎', 'Emoji', 'September'],
                ['🎃', 'Emoji', 'October'],
                ['🍂', 'Emoji', 'November'],
                ['🎅', 'Emoji', 'December'],
            ],
            columns=['AliasValue', 'AliasName', 'NodeName']
        )

        # Aliases
        self.dim.load_aliases_from_dataframe(df_aliases, 'NodeName', 'AliasName', 'AliasValue')
        df = self.dim.save_aliases_to_dataframe(None)
        df.drop(labels='index', axis=1, inplace=True)
        df.sort_values(by=['name', 'node', 'value'], axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_aliases.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_aliases.csv', f'{BASELINE}df_aliases.csv')
        return

    def test_009_load_save_properties(self):
        df_properties = pd.DataFrame(
            [
                ['Magenta', 'Colour', ROOT],
                ['Purple', 'Colour', 'Year'],
                ['Red', 'Colour', 'Q1'],
                ['Orange', 'Colour', 'Q2'],
                ['Green', 'Colour', 'April'],
                ['Green', 'Colour', 'May'],
                ['Blue', 'Colour', 'July'],
                ['Blue', 'Colour', 'August'],
                ['Blue', 'Colour', 'September'],
                ['White', 'Colour', 'Q4'],
                ['Red', 'Colour', 'October'],
                ['Green', 'Colour', 'November'],
                ['Red', 'Colour', 'December'],
                ['Winter', 'Season', 'Q1'],
                ['Spring', 'Season', 'Q2'],
                ['Summer', 'Season', 'Q3'],
                ['Autumn', 'Season', 'Q4'],
            ],
            columns=['PropertyValue', 'PropertyName', 'NodeName']
        )

        # Properties
        self.dim.load_properties_from_dataframe(df_properties, 'NodeName', 'PropertyName', 'PropertyValue')
        df = self.dim.save_properties_to_dataframe(None)
        df.drop(labels='index', axis=1, inplace=True)
        df.sort_values(by=['name', 'node', 'value'], axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_properties.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_properties.csv', f'{BASELINE}df_properties.csv')
        return

    def test_010_load_save_values(self):
        df_values = pd.DataFrame(
            [
                [-10.0, 'Costs', 'January'],
                [-100.0, 'Costs', 'February'],
                [-1000.0, 'Costs', 'March'],
                [-20.0, 'Costs', 'April'],
                [-200.0, 'Costs', 'May'],
                [-2000.0, 'Costs', 'June'],
                [-30.0, 'Costs', 'July'],
                [-300.0, 'Costs', 'August'],
                [-3000.0, 'Costs', 'September'],
                [-40.0, 'Costs', 'October'],
                [-400.0, 'Costs', 'November'],
                [-4000.0, 'Costs', 'December'],
                [10.0, 'Profit', 'January'],
                [100.0, 'Profit', 'February'],
                [1000.0, 'Profit', 'March'],
                [20.0, 'Profit', 'April'],
                [200.0, 'Profit', 'May'],
                [2000.0, 'Profit', 'June'],
                [30.0, 'Profit', 'July'],
                [300.0, 'Profit', 'August'],
                [3000.0, 'Profit', 'September'],
                [40.0, 'Profit', 'October'],
                [400.0, 'Profit', 'November'],
                [4000.0, 'Profit', 'December'],
            ],
            columns=['Value', 'ValueName', 'NodeName']
        )

        # Values
        self.dim.load_values_from_dataframe(df_values, 'NodeName', 'ValueName', 'Value')
        df = self.dim.save_values_to_dataframe(None)
        df.drop(labels='index', axis=1, inplace=True)
        df.sort_values(by=['name', 'node', 'value'], axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_values.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_values.csv', f'{BASELINE}df_values.csv')
        return

    def test_011_get_hierarchy_dataframe(self):
        df = self.dim.get_hierarchy_dataframe(hierarchy=MAIN)
        df = df.reindex(columns=sorted(df.columns))
        df.to_csv(f'{FOLDER}df_get_hierarchy_main.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_hierarchy_main.csv', f'{BASELINE}df_get_hierarchy_main.csv')
        return

    def test_012_get_aliases_dataframe(self):
        df = self.dim.get_aliases_dataframe()
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_aliases.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_aliases.csv', f'{BASELINE}df_get_aliases.csv')
        return

    def test_013_get_attributes_dataframe(self):
        df = self.dim.get_attributes_dataframe()
        df.drop(labels='index', axis=1, inplace=True)
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_attributes.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_attributes.csv', f'{BASELINE}df_get_attributes.csv')
        return

    def test_014_get_consolidation_dataframe(self):
        df = self.dim.get_consolidation_dataframe('Costs', hierarchy=MAIN)
        df.to_csv(f'{FOLDER}df_get_consolidation_costs_main.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_consolidation_costs_main.csv', f'{BASELINE}df_get_consolidation_costs_main.csv')
        return

    def test_015_get_properties_dataframe(self):
        df = self.dim.get_properties_dataframe()
        df.drop(labels='index', axis=1, inplace=True)
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_properties.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_properties.csv', f'{BASELINE}df_get_properties.csv')
        return

    def test_016_get_values_dataframe(self):
        df = self.dim.get_values_dataframe()
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_values.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_values.csv', f'{BASELINE}df_get_values.csv')
        return

    def test_017_get_hierarchy_table(self):
        df = self.dim.hierarchy_table(hierarchy=MAIN)
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_hierarchy_table_main.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_hierarchy_table_main.csv', f'{BASELINE}df_get_hierarchy_table_main.csv')
        return

    def test_018_get_all_leaves(self):
        expected = ['April',
                    'August',
                    'December',
                    'February',
                    'January',
                    'Janusday',
                    'July',
                    'June',
                    'March',
                    'May',
                    'November',
                    'October',
                    'September']

        nodes = sorted(self.dim.get_all_leaves(hierarchy=MAIN))
        return self.assertListEqual(expected, nodes)

    def test_019_get_all_nodes(self):
        expected = ['!!root!!',
                    'April',
                    'August',
                    'December',
                    'Donk-tober',
                    'February',
                    'January',
                    'Janusday',
                    'July',
                    'June',
                    'March',
                    'May',
                    'November',
                    'October',
                    'Q1',
                    'Q2',
                    'Q3',
                    'Q4',
                    'Q5',
                    'September',
                    'Year']

        nodes = sorted(self.dim.get_all_nodes(hierarchy=MAIN))
        return self.assertListEqual(expected, nodes)

    def test_020_get_all_parents(self):
        expected = ['!!root!!', 'Donk-tober', 'Q1', 'Q2', 'Q3', 'Q4', 'Q5', 'Year']
        nodes = sorted(self.dim.get_all_parents(hierarchy=MAIN))
        return self.assertListEqual(expected, nodes)

    def test_021_get_ancestors(self):
        expected = [[0, 'February'], [1, 'Q1'], [2, 'Year'], [3, '!!root!!']]
        nodes = self.dim.get_ancestors('February', hierarchy=MAIN)
        return self.assertListEqual(expected, nodes)

    def test_022_get_ancestor_at_generation(self):
        expected = 'Year'
        node = self.dim.get_ancestor_at_generation('February', 1, hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_023_get_ancestor_at_level(self):
        expected = 'Year'
        node = self.dim.get_ancestor_at_level('February', 2, hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_024_get_bottom(self):
        expected = 'March'
        node = self.dim.get_bottom('Q1', hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_025_get_top(self):
        expected = 'January'
        node = self.dim.get_top('Q1', hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_026_get_down(self):
        expected = 'March'
        node = self.dim.get_down('Q1', 'February', hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_027_get_up(self):
        expected = 'January'
        node = self.dim.get_up('Q1', 'February', hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_028_get_children(self):
        expected = ['January', 'February', 'March']
        nodes = self.dim.get_children('Q1', hierarchy=MAIN)
        return self.assertListEqual(expected, nodes)

    def test_029_get_children_count(self):
        expected = 3
        count = self.dim.get_children_count('Q1', hierarchy=MAIN)
        return self.assertEqual(expected, count)

    def test_030_get_generation(self):
        expected = 2
        count = self.dim.get_generation('Q1', hierarchy=MAIN)
        return self.assertEqual(expected, count)

    def test_031_get_grandparent(self):
        expected = 'Year'
        node = self.dim.get_grandparent('February', hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_032_get_leaves(self):
        expected = [[2, 'January'],
                    [2, 'February'],
                    [2, 'March'],
                    [2, 'April'],
                    [2, 'May'],
                    [2, 'June'],
                    [2, 'July'],
                    [2, 'August'],
                    [2, 'September'],
                    [2, 'October'],
                    [2, 'November'],
                    [2, 'December'],
                    [3, 'Janusday']]

        nodes = self.dim.get_leaves('Year', hierarchy=MAIN)
        return self.assertEqual(expected, nodes)

    def test_033_get_leaves_at_generation(self):
        expected = [[2, 'January'],
                    [2, 'February'],
                    [2, 'March'],
                    [2, 'April'],
                    [2, 'May'],
                    [2, 'June'],
                    [2, 'July'],
                    [2, 'August'],
                    [2, 'September'],
                    [2, 'October'],
                    [2, 'November'],
                    [2, 'December']]

        nodes = self.dim.get_leaves_at_generation('Year', 2,  hierarchy=MAIN)
        return self.assertEqual(expected, nodes)

    def test_034_get_leaves_at_level(self):
        expected = [[3, 'January'],
                    [3, 'February'],
                    [3, 'March'],
                    [3, 'April'],
                    [3, 'May'],
                    [3, 'June'],
                    [3, 'July'],
                    [3, 'August'],
                    [3, 'September'],
                    [3, 'October'],
                    [3, 'November'],
                    [3, 'December']]

        nodes = self.dim.get_leaves_at_level('February', 0, hierarchy=MAIN)
        return self.assertEqual(expected, nodes)

    def test_035_get_parent(self):
        expected = 'Q1'
        nodes = self.dim.get_parent('February', hierarchy=MAIN)
        return self.assertEqual(expected, nodes)

    def test_036_get_parents(self):
        expected = [['financial', 'halves', 'main'], ['YTD', 'Q1', 'Q1']]
        nodes = self.dim.get_parents('February')
        return self.assertEqual(expected, nodes)

    def test_037_get_siblings(self):
        expected = ['January', 'February', 'March']
        nodes = self.dim.get_siblings('February', hierarchy=MAIN)
        return self.assertEqual(expected, nodes)

    def test_038_get_difference(self):
        expected = sorted(['Janusday', 'Year', 'Q5', 'Donk-tober'])
        nodes = sorted(self.dim.get_difference(['halves']))
        return self.assertEqual(expected, nodes)

    def test_039_get_intersection(self):
        expected = sorted(['!!root!!', 'April', 'August', 'December', 'February', 'January', 'July', 'June', 'March',
                           'May', 'November', 'October', 'Q1', 'Q2', 'Q3', 'Q4', 'September'])
        nodes = sorted(self.dim.get_intersection(['halves']))
        return self.assertEqual(expected, nodes)

    def test_040_get_union(self):
        expected = sorted(['!!root!!', 'April', 'August', 'December', 'Donk-tober', 'February', 'H1', 'H2', 'January',
                           'Janusday', 'July', 'June', 'March', 'May', 'November', 'October', 'Q1', 'Q2', 'Q3', 'Q4',
                           'Q5', 'September', 'Year'])
        nodes = sorted(self.dim.get_union(['halves']))
        return self.assertEqual(expected, nodes)

    def test_041_add_node_to_alt(self):
        expected = 'H2'
        self.dim.add_node('H2', 'Q5', '+', hierarchy='halves', after='Q4')
        node = self.dim.get_parent('Q5',  hierarchy='halves')
        return self.assertEqual(expected, node)

    def test_042_move_node_in_alt(self):
        expected = 'H1'
        self.dim.move_node('Q5', 'H1', hierarchy='halves', before='Q2')
        node = self.dim.get_parent('Q5',  hierarchy='halves')
        return self.assertEqual(expected, node)

    def test_043_rename_node(self):
        expected = 'Q5'
        self.dim.rename_node('Donk-tober', 'Davetober')
        node = self.dim.get_parent('Davetober',  hierarchy=MAIN)
        return self.assertEqual(expected, node)

    def test_044_delete_node(self):
        self.dim.delete_node('Year', 'Q5', hierarchy=MAIN)
        node = self.dim.node_exists('Q5')
        return self.assertFalse(node)

    def test_045_default_alias_dataframe(self):
        self.dim.set_default_aliases(primary='Welsh', secondary='French')
        df = self.dim.get_aliases_dataframe()
        df = df.reindex(columns=sorted(df.columns))
        df.sort_values(by=list(df.columns), axis=0, inplace=True)
        df.to_csv(f'{FOLDER}df_get_default_aliases.csv', index=False)
        self.assertFileEqual(f'{FOLDER}df_get_default_aliases.csv', f'{BASELINE}df_get_default_aliases.csv')
        pass

    def tearDown(self):
        self.dim = None
        self.dims = None
