
from __future__ import absolute_import
import unittest
import pytest
import os
import tempfile

from plaidcloud.utilities.dimension import Dimension, Node

__author__ = 'Dave Parsons'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Dave Parsons']
__license__ = 'Apache 2.0'
__maintainer__ = 'Dave Parsons'
__email__ = 'dave.parsons@tartansolutions.com'


class TestDimension(unittest.TestCase):
    """test stuff"""

    def setUp(self):
        """ Sets up the Dimension, while also testing a few methods.

        Tested methods:
            __init__
            add_node
            make_pretty_dimension
            add_alt_node
        """
        raw_data = """
        Periods,Periods,All Periods,10,~,
        Periods,Periods,All_Period_Input,8,~,
        Periods,Periods,Period Attributes,11,~,
        Periods,Period Attributes,YTD_2016,15,OR,
        Periods,Period Attributes,Period Calculation Management,15,OR,
        Periods,Period Attributes,Years,15,OR,
        Periods,Period Attributes,Demo Period,15,+,
        Periods,2016,2016_01,8,+,
        Periods,2016,2016_02,8,+,
        Periods,2016,2016_03,8,+,
        Periods,2016,2016_04,8,+,
        Periods,2016,2016_05,8,+,
        Periods,2016,2016_06,8,+,
        Periods,2016,2016_07,8,+,
        Periods,2016,2016_08,8,+,
        Periods,2016,2016_09,8,+,
        Periods,2016,2016_10,8,+,
        Periods,2016,2016_11,8,+,
        Periods,2016,2016_12,8,+,
        Periods,All Periods,2016,10,+,
        Periods,Period Calculation Management,Mix_Calculation_Basis,15,+,
        Periods,YTD_2016,YTD_2016_01,15,+,
        Periods,YTD_2016,YTD_2016_02,15,+,
        Periods,YTD_2016,YTD_2016_03,15,+,
        Periods,YTD_2016,YTD_2016_04,15,+,
        Periods,YTD_2016,YTD_2016_05,15,+,
        Periods,YTD_2016,YTD_2016_06,15,+,
        Periods,YTD_2016,YTD_2016_07,15,+,
        Periods,YTD_2016,YTD_2016_08,15,+,
        Periods,YTD_2016,YTD_2016_09,15,+,
        Periods,YTD_2016,YTD_2016_10,15,+,
        Periods,YTD_2016,YTD_2016_11,15,+,
        Periods,YTD_2016,YTD_2016_12,15,+,
        Periods,YTD_2016_01,2016_01,12,+,
        Periods,YTD_2016_02,2016_01,12,+,
        Periods,YTD_2016_02,2016_02,12,+,
        Periods,YTD_2016_03,2016_01,12,+,
        Periods,YTD_2016_03,2016_02,12,+,
        Periods,YTD_2016_03,2016_03,12,+,
        Periods,YTD_2016_04,2016_01,12,+,
        Periods,YTD_2016_04,2016_02,12,+,
        Periods,YTD_2016_04,2016_03,12,+,
        Periods,YTD_2016_04,2016_04,12,+,
        Periods,YTD_2016_05,2016_01,12,+,
        Periods,YTD_2016_05,2016_02,12,+,
        Periods,YTD_2016_05,2016_03,12,+,
        Periods,YTD_2016_05,2016_04,12,+,
        Periods,YTD_2016_05,2016_05,12,+,
        Periods,YTD_2016_06,2016_01,12,+,
        Periods,YTD_2016_06,2016_02,12,+,
        Periods,YTD_2016_06,2016_03,12,+,
        Periods,YTD_2016_06,2016_04,12,+,
        Periods,YTD_2016_06,2016_05,12,+,
        Periods,YTD_2016_06,2016_06,12,+,
        Periods,YTD_2016_07,2016_01,12,+,
        Periods,YTD_2016_07,2016_02,12,+,
        Periods,YTD_2016_07,2016_03,12,+,
        Periods,YTD_2016_07,2016_04,12,+,
        Periods,YTD_2016_07,2016_05,12,+,
        Periods,YTD_2016_07,2016_06,12,+,
        Periods,YTD_2016_07,2016_07,12,+,
        Periods,YTD_2016_08,2016_01,12,+,
        Periods,YTD_2016_08,2016_02,12,+,
        Periods,YTD_2016_08,2016_03,12,+,
        Periods,YTD_2016_08,2016_04,12,+,
        Periods,YTD_2016_08,2016_05,12,+,
        Periods,YTD_2016_08,2016_06,12,+,
        Periods,YTD_2016_08,2016_07,12,+,
        Periods,YTD_2016_08,2016_08,12,+,
        Periods,YTD_2016_09,2016_01,12,+,
        Periods,YTD_2016_09,2016_02,12,+,
        Periods,YTD_2016_09,2016_03,12,+,
        Periods,YTD_2016_09,2016_04,12,+,
        Periods,YTD_2016_09,2016_05,12,+,
        Periods,YTD_2016_09,2016_06,12,+,
        Periods,YTD_2016_09,2016_07,12,+,
        Periods,YTD_2016_09,2016_08,12,+,
        Periods,YTD_2016_09,2016_09,12,+,
        Periods,YTD_2016_10,2016_01,12,+,
        Periods,YTD_2016_10,2016_02,12,+,
        Periods,YTD_2016_10,2016_03,12,+,
        Periods,YTD_2016_10,2016_04,12,+,
        Periods,YTD_2016_10,2016_05,12,+,
        Periods,YTD_2016_10,2016_06,12,+,
        Periods,YTD_2016_10,2016_07,12,+,
        Periods,YTD_2016_10,2016_08,12,+,
        Periods,YTD_2016_10,2016_09,12,+,
        Periods,YTD_2016_10,2016_10,12,+,
        Periods,YTD_2016_11,2016_01,12,+,
        Periods,YTD_2016_11,2016_02,12,+,
        Periods,YTD_2016_11,2016_03,12,+,
        Periods,YTD_2016_11,2016_04,12,+,
        Periods,YTD_2016_11,2016_05,12,+,
        Periods,YTD_2016_11,2016_06,12,+,
        Periods,YTD_2016_11,2016_07,12,+,
        Periods,YTD_2016_11,2016_08,12,+,
        Periods,YTD_2016_11,2016_09,12,+,
        Periods,YTD_2016_11,2016_10,12,+,
        Periods,YTD_2016_11,2016_11,12,+,
        Periods,YTD_2016_12,2016_01,12,+,
        Periods,YTD_2016_12,2016_02,12,+,
        Periods,YTD_2016_12,2016_03,12,+,
        Periods,YTD_2016_12,2016_04,12,+,
        Periods,YTD_2016_12,2016_05,12,+,
        Periods,YTD_2016_12,2016_06,12,+,
        Periods,YTD_2016_12,2016_07,12,+,
        Periods,YTD_2016_12,2016_08,12,+,
        Periods,YTD_2016_12,2016_09,12,+,
        Periods,YTD_2016_12,2016_10,12,+,
        Periods,YTD_2016_12,2016_11,12,+,
        Periods,YTD_2016_12,2016_12,12,+,
        Periods,Demo Period,2016_11,12,+,
        """

        cr = """
        """

        raw_data = raw_data.split(cr)

        raw_data = [a for a in raw_data if a != '']

        raw_records = []

        for r in raw_data:
            raw_records.append(r.split(','))

        self.periods = Dimension()

        self.periods.name = raw_records[0][0]
        # print(self.periods.__dict__)
        self.periods.alternate_0 = self.__dict__.get('name', 'Unnamed') + "_Alternate_0"  # We need a 'default' alt hier

        # Print Debug Stuff
        # for item in raw_records:
        #     print(item)

        alt_records = []

        for item in raw_records:
            parent = item[1]
            child = item[2]
            attribute = item[3]
            consolidation_type = item[4]

            if attribute in ['8', '10']:
                self.periods.add_node(parent, child, consolidation_type)
            elif attribute in ['12', '15']:
                alt_records.append(item)

        # Making sure we add alt records after main hierarchy is built.
        for item in alt_records:
            parent = item[1]
            child = item[2]
            attribute = item[3]
            consolidation_type = item[4]
            self.periods.add_alt_node(parent, child, consolidation_type)

        pretty_dimension = self.periods.make_pretty_dimension()

        # Print Debug Stuff
        # print(pretty_dimension.getvalue())

    def testGetAltHierarchies(self):
        """ Grabs the alternate hierarchies.

        Tested methods:
            get_alt_hierarchies
        """
        alt = self.periods.get_alt_hierarchies()
        self.assertEqual(type(alt), type([]))

    def testSaveLoad(self):
        """ Saves the dimension and reloads it.

        Tested methods:
            save
            load
            make_pretty_dimension
        """
        temp_folder = tempfile.mkdtemp()
        self.periods.save(temp_folder+'dimension.hdf5')
        new_periods = Dimension()
        new_periods.load(temp_folder+'dimension.hdf5')
        self.assertIsNotNone(new_periods)

    def testGetAltNode(self):
        """ Gets an alternate node

        Tested methods:
            get_alt_node
        """
        result = self.periods.get_alt_node('Unnamed_Alternate_0', '2016_04')
        self.assertEqual(str(result), '<(Node ID: 2016_04 (+)>')

    def testGetAltParent(self):
        """ Tests getting a node's parent, and the
        ID of the parent

        Tested Methods:
            get_alt_parent
            get_alt_parent_id
            get_alt_parents
        """
        with self.assertRaises(Exception):
            self.periods.get_alt_parents('Unnamed_Alternate_0', '2016_04')
        self.assertEqual(type(self.periods.get_alt_parent('Unnamed_Alternate_0', '2016_04')), Node)
        self.assertEqual(type(self.periods.get_alt_parent_id('Unnamed_alternate_0', '2016_04')), str)

    def testGetAltFamily(self):
        """ Gets the grandparent, siblings, and children
        of the node

        Tested Methods:
            get_alt_grandparent
            get_alt_grandparent_id
            get_alt_siblings
            get_alt_sibling_ids
            get_alt_children
            get_alt_children_ids
        """
        self.assertEqual(
            str(self.periods.get_alt_grandparent('Unnamed_Alternate_0', '2016_04')),
            '<(Node ID: YTD_2016 (OR)>'
        )
        self.assertEqual(self.periods.get_alt_grandparent_id('Unnamed_Alternate_0', '2016_04'), 'YTD_2016')
        self.assertEqual(type(self.periods.get_alt_siblings('Unnamed_Alternate_0', '2016_04')), type([]))
        self.assertEqual(type(self.periods.get_alt_sibling_ids('Unnamed_Alternate_0', '2016_04')), type([]))
        self.assertEqual(type(self.periods.get_alt_children('Unnamed_Alternate_0', '2016_04')), type([]))
        self.assertEqual(type(self.periods.get_alt_children_ids('Unnamed_Alternate_0', '2016_04')), type([]))

    def testClearAlias(self):
        """ Clears out the alias data

        Tested methods:
            clear_alias
        """
        self.periods.clear_alias()
        self.assertEqual(self.periods.h_alias, {})
        pass

    def testGetNodeFromAlias(self):
        """ Gets a node and an ID from the dimension

        Tested methods:
            get_node_id_from_alias
            get_node_from_alias"""

        pass
        # TODO this method requires aliases. As of now,
        # the test data does not contain any aliases.

        # print('Aliases: {}'.format(self.periods.h_alias))
        # node = self.periods.get_node_from_alias('YTD_2016_12')
        # node_id = self.periods.get_node_id_from_alias('YTD_2016_12')

    def testProperty(self):
        """ Gets a property from a node

        Tested methods:
            set_property
            property"""

        self.periods.set_property('YTD_2016_12', 'test_property_value', 'test_property_name')
        self.assertEqual(self.periods.property('YTD_2016_12', 'test_property_name'), 'test_property_value')
        self.assertEqual(self.periods.get_node_ids_with_property('test_property_name')[0], 'YTD_2016_12')
        self.assertEqual(self.periods.get_node_ids_with_property_value('test_property_name',
                         'test_property_value')[0], 'YTD_2016_12')
        self.assertEqual(self.periods.get_properties()[0], 'test_property_name')
        self.periods.clear_property()

    def testSavePreprocessed(self):
        """ Tests the save method

        Tested Methods:
            save_preprocessed
        """
        temp_folder = tempfile.mkdtemp()
        temp_file = temp_folder+'test_preprocessed.hd5'
        self.periods.save_preprocessed(temp_file)
        fp = open(temp_file, 'rb')
        self.assertIsNotNone(fp)
        fp.close()
        os.remove(temp_file)

    def testSavePrettyHierarchy(self):
        """ Tests saving a pretty version of the file

        Tested methods:
            save_pretty_hierarchy
        """
        temp_folder = tempfile.mkdtemp()
        temp_file = temp_folder+'test_pretty.txt'
        self.periods.save_pretty_hierarchy(temp_file)
        fp = open(temp_file, 'r')
        self.assertIsNotNone(fp)
        fp.close()
        os.remove(temp_file)

    def testGetAllNodes(self):
        """ Tested methods:
            get_all_nodes_as_list
            get_node_count
        """
        nodes = self.periods.get_all_nodes_as_list()
        count = self.periods.get_node_count()

        self.assertEqual(len(nodes) + 1, count)

    @pytest.mark.skip("This does not work")
    def testSaveToAnalyze(self):
        # TODO probably should get the project/model
        # IDs automatically instead of manually.
        self.periods.set_property('YTD_2016_12', 'test_property_value', 'test_property_name')
        self.periods.export_to_analyze(
            1,
            'f4698650565a43b99a8a480607c796a5',
            # '93cc8d9aaa084316af53ab7c0e2054db',
            name='Test_Dimension'
        )
        self.periods.import_from_analyze(
            1,
            'f4698650565a43b99a8a480607c796a5',
            # '93cc8d9aaa084316af53ab7c0e2054db',
            name='Test_Dimension'
        )

    def tearDown(self):
        # self.clear()
        self.periods = None
