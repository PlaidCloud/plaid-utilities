#!/usr/bin/env python
# coding=utf-8
"""
A highly optimized class for bill of materials consolidation and costing
"""
from pandas import DataFrame
import os
import sys

DOCKER = os.environ.get('DOCKER') in ('transform', 'ipython')

if DOCKER:
    sys.path.append('/var/vol/code')

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2015, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Proprietary"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"


class Node(object):

    """A BOM hierarchy node object"""

    def __init__(self, parent_obj, child_id, makeup_volume=1):
        """Initializes node object

        :param parent_obj: parent object
        :type parent_obj: object
        :param child_id: child node key
        :type child_id: str or unicode
        :param consolidation: consolidation type +, -, or ~
        :type consolidation: str or unicode
        :returns: None
        :rtype: None
        """

        self.id = child_id
        self.parents = []
        self.children = {}
        self.cost = None
        self.override_cost = None

        # Set the reference to an assembly
        if parent_obj is not None:
            parent_obj.add_child(self, makeup_volume)

        # Create a reference so each child know who uses it
        if parent_obj not in self.parents:
            self.parents.append(parent_obj)

    def __repr__(self):
        return "<(Node ID: {} ({})>".format(self.id, self.get_cost())

    def get_cost(self):
        if self.cost is None:
            # Need to calculate the cost
            self.calculate_cost()

        return self.cost

    def set_cost(self, value):
        self.cost = value

    def set_override_cost(self, value):
        self.override_cost = value

    def add_child(self, child_obj, makeup_volume):
        """Add a child to the node

        :param child_obj: node object to add
        :type child_obj: object
        :param h: hierarchy - main or name of alt hierarchy
        :type h: str or unicode
        :returns: None
        :rtype: None
        """

        # Add the child to this parent's list of children
        # Also handles a makeup volume change
        self.children[child_obj] = makeup_volume

    def remove_child(self, child_id):
        """Removes child from node

        :param child_id: child node key to remove
        :type child_id: str or unicode
        :returns: None
        :rtype: None
        """

        current_children = self.get_children()
        temp_children = {}
        for c in current_children:
            if c.id != child_id:
                temp_children[c] = current_children[c]
        # The new list should be missing the child.
        self.children = temp_children

    def get_parents(self):
        """Returns parent object of node

        :returns: Parent object
        :rtype: object
        """

        return self.parents

    def get_siblings(self, parent_id):
        """Finds siblings of the node

        :returns: list of siblings node objects including current node
        :rtype: list
        """

        for p in self.parents:
            if p.id == parent_id:
                return p.get_children()

    def get_children(self):
        """Returns list of children node objects

        :returns: list of child node objects
        :rtype: list
        """

        return self.children

    def is_child_of(self, parent_id):
        """Checks if the node is a child of the specified parent

        :param parent_id: parent node key
        :type parent_id: str or unicode
        :returns: True if node descends from the parent
        :rtype: bool
        """

        for p in self.parents:
            if p.id == parent_id:
                return True
        return False

    def is_parent_of(self, child_id):
        """Checks if the node is a parent of the specified child

        :param child_id: child node key
        :type child_id: str or unicode
        :returns: True if child descends from the node
        :rtype: bool
        """
        for c in self.get_children():
            if c.id == child_id:
                return True
        return False

    def calculate_cost(self):
        """Calculates the roll-up cost of this node based on
        the costs of sub-components

        :returns: None
        :rtype: None
        """

        if self.override_cost is None:
            # Ask all children for their costs and multiply by makeup volume
            # This will invoke a recursive request for costs down to the
            # lowest level component
            cost = 0
            for c in self.children:
                makeup_volume = self.children[c]
                if makeup_volume != 0:
                    child_cost = c.get_cost()
                    makeup_cost = child_cost * self.children[c]
                    cost += makeup_cost
            self.cost = cost
        else:
            # An Override cost has been supplied
            # DO NOT calculate the cost, just use the override
            self.cost = self.override_cost

    def reset_cost(self):
        self.cost = None


class BOM(object):

    """BOM Hierarchy Class for fast BOM hierarchy operations"""

    def __init__(self, load_path=None):
        """Class init function sets up basic structure

        :param load_path: optional path to saved hierarchy load file to load initially
        :type load_path: str or unicode
        :returns: None
        :rtype: None
        """
        self.h_direct = {}
        self.h_children = {}

        self.clear()

        if load_path is not None:
            self.load(load_path)

    def add_node(self, parent_id, child_id, makeup_volume=1):
        """Adds a node to the main hierarchy

        :param parent_id: parent node key
        :type parent_id: str or unicode
        :param child_id: child node key
        :type child_id: str or unicode
        :param consolidation: consolidation type +, -, or ~
        :type consolidation: str or unicode
        :returns: None
        :rtype: None
        """

        try:
            parent_obj = self.get_node(parent_id)
        except:
            # Parent does not exist yet.  Handle out of sequence data gracefully.
            root_parent = self.get_node('root')
            parent_obj = Node(root_parent, parent_id)
            self.h_direct[parent_id] = parent_obj

        if child_id in self.h_direct:
            # This already exists.
            node = self.h_direct[child_id]
            parent_obj.add_child(node, makeup_volume)
        else:
            # Doesn't exist.  Simple add.
            node = Node(parent_obj, child_id, makeup_volume)
            self.h_direct[child_id] = node

    def delete_node(self, node_id):
        """Deletes the node and removes all aliases and properties

        :param node_id: node key
        :type node_id: str or unicode
        :returns: None
        :rtype: None
        """

        # Delete the node and index reference
        try:
            parents = self.get_parents(node_id)
        except:
            # Not present.  No need to do anything
            pass
        else:
            # Remove from main hierarchy
            del self.h_direct[node_id]

            for p in parents:
                p.remove_child(node_id)

    def get_node(self, node_id):
        """Gets the node object

        :param node_id: node key
        :type node_id: str or unicode
        :returns: Node object
        :rtype: object
        """

        try:
            return self.h_direct[node_id]
        except:
            raise Exception('No node found with the name %s' % node_id)

    def reset_costs(self):
        """Resets all costs to uncalculated value

        :returns: None
        :rtype: None
        """

        for node_id in self.h_direct:
            self.h_direct[node_id].reset_cost()

    def set_cost(self, node_id, value):
        self.h_direct[node_id].set_cost(value)

    def set_override_cost(self, node_id, value):
        self.h_direct[node_id].set_override_cost(value)

    def get_all_costs(self):
        """Gets the cost of all nodes

        :returns: node cost
        :rtype: pandas.DataFrame
        """

        final = []
        for node_id in self.h_direct:
            temp = (node_id, self.h_direct[node_id].get_cost())
            final.append(temp)

        headers = ['node', 'cost']
        df = DataFrame(final, columns=headers)
        return df

    def get_parents(self, node_id):
        """Finds parent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node object of parent
        :rtype: object
        """

        return self.get_node(node_id).get_parents()

    def get_parent_ids(self, node_id):
        """Finds parent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node key of parent
        :rtype: str or unicode
        """

        try:
            parents = self.get_parents(node_id)

            return [p.id for p in parents]
        except:
            return None

    def get_siblings(self, node_id, parent_id):
        """Finds sibling nodes of specified node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node objects of all siblings including the current node
        :rtype: list
        """

        return self.get_node(node_id).get_siblings(parent_id)

    def get_sibling_ids(self, node_id, parent_id):
        """Finds sibling nodes of specified node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node keys of all siblings including the current node
        :rtype: list
        """

        objs = self.get_siblings(node_id, parent_id)

        return [o.id for o in objs]

    def get_children(self, node_id):
        """Finds children of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of children node objects
        :rtype: list
        """

        return self.get_node(node_id).get_children()

    def get_children_ids(self, node_id):
        """Finds children of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of children node keys
        :rtype: list
        """

        objs = self.get_children(node_id)

        return [o.id for o in objs]

    def is_child_of(self, node_id, parent_id):
        """Check if node is a child of the parent node

        :param node_id: child node key
        :type node_id: str or unicode
        :param parent_id: parent node key
        :type parent_id: str or unicode
        :returns: True if the child descends from the parent
        :rtype: bool
        """

        return self.get_node(node_id).is_child_of(parent_id)

    def is_parent_of(self, node_id, child_id):
        """Checks if node is a parent of the child node

        :param node_id: parent node key
        :type node_id: str or unicode
        :param child_id: child node key
        :type child_id: str or unicode
        :returns: True if the child descends from parent
        :rtype: bool
        """

        return self.get_node(node_id).is_parent_of(child_id)

    def _get_main_list_format(self, node_id):
        """Generates the parent child list recursively for saving

        :param node_id: current node to process
        :type node_id: str or unicode
        :returns: List of lists with parent child information
        :rtype: list
        """

        final = []

        children = self.get_children(node_id)
        for c in children:
            temp = [str(node_id), str(c.id), children[c]]
            final.append(temp)

            sub_children = self._get_main_list_format(c.id)
            if len(sub_children) > 0:
                final += sub_children

        return final

    def save(self, path):
        """Saves the hierarchy, alias, and property info in one file

        :param path: File path to save out put
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        self.save_hierarchy(path, 'root')

    def load(self, path):
        """Loads hierarchy, alias, and propeperty

        :param path: File path to load
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        self.load_hierarchy(path)

    def get_bom(self, top_node='root'):
        """Created dataframe of BOM makeup structure

        :param top_node:
        :type top_node: str or unicode
        :returns: Parent Child Dataframe
        :rtype: pandas.DataFrame
        """

        headers = ['parent', 'child', 'makeup_volume']
        pc_list = self._get_main_list_format(top_node)

        df = DataFrame(pc_list, columns=headers)
        return df

    def load_dataframe(self, df):
        """Loads a well formed dataframe into the hierarchy object

        Columns expected:
        - parent
        - child
        - makeup_volume

        :param df: The dataframe containing at least parent and child columns
        :type df: dataframe
        :returns: None
        :rtype: None
        """

        if df is not None:

            column_info = []
            for column_name, data_type in df.dtypes.items():
                column_info.append(column_name)

            # Check to make sure all required columns are present
            if 'parent' not in column_info:
                raise Exception('Missing parent column. Found the following columns: {0}'.format(str(column_info)))

            if 'child' not in column_info:
                raise Exception('Missing child column. Found the following columns: {0}'.format(str(column_info)))

            if 'makeup_volume' not in column_info:
                raise Exception(
                    'Missing makeup_volume column. Found the following columns: {0}'.format(str(column_info)))

            # order the df columns (hierarchy, parent, child, consolidation_type)
            # this enables using itertuples instead of iterrows
            df = df[['parent', 'child', 'makeup_volume']]

            # Iterate over the data and build the hierachy using the add method
            for r in df.itertuples():
                # Tuple is formed as (index, hierarchy, parent, child, consolidation type)
                self.add_node(r[1], r[2], r[3])

    def clear(self):
        self.clear_hierarchy()

    def clear_hierarchy(self):
        """Clears the main and alternate hierarchies

        :returns: None
        :rtype: None
        """

        self.h_direct = {}
        self.h_children = {}

        node = Node(None, 'root')
        self.h_direct['root'] = node

    def _get_preprocessed_main_format(self, node_id, left=0, volume_multiplier=1, indent=0):
        """Generates a highly optimized reporting format for export of main hierarchy

        :param node_id: current node key
        :type node_id: str or unicode
        :param left: current left counter
        :type left: int
        :param consolidation_list: list of consolidation multipliers as json string
        :type consolidation_list: str
        :returns: list of parent child records
        :rtype: list
        """

        final = []

        # If this recursed event doesn't have any records return the same value for left and right
        right = left

        children = self.get_children(node_id)
        for c in children:
            makeup_volume = children[c]
            effective_volume = makeup_volume * volume_multiplier
            # Get the child records recursively
            sub_right, sub_children = self._get_preprocessed_main_format(c.id, left + 1, effective_volume, indent + 1)

            # Now figure out the right side number based on how many elements are below
            right = sub_right + 1
            if len(sub_children) > 0:
                is_leaf = False
            else:
                is_leaf = True

            temp = [str(node_id), str(c.id), makeup_volume, effective_volume, is_leaf, left, right, indent]
            final.append(temp)
            if is_leaf is False:
                final += sub_children

        return (right, final)

    def get_frame(self, table, top_node='root'):
        """Generates a highly optimized reporting format for export

        :param path: Absolute path to export location
        :type path: str or unicode
        :param top_node: node key to start export at
        :type top_node: str or unicode
        :returns: None
        :rtype: None
        """

        headers = ['parent', 'child', 'makeup_volume', 'effective_makeup_volume', 'leaf', 'left', 'right', 'indent']
        right, pc_list = self._get_preprocessed_main_format(top_node)
        df = DataFrame(pc_list, columns=headers)

        return df

    def get_pretty_frame(self, table, top_node='root'):
        indent = '  '
        template = '{0}{1} x {2} ({3})'

        right, pc_list = self._get_preprocessed_main_format(top_node)

        final = []
        for p in pc_list:
            temp = []
            parent = p[0]
            child = p[1]
            makeup_volume = p[2]
            effective_makeup_volume = p[3]
            #leaf = p[4]
            #left = p[5]
            #right = p[6]
            indent_mult = p[7]

            indent_txt = indent * indent_mult
            txt = template.format(indent_txt, makeup_volume, child, effective_makeup_volume)
            temp.append(txt)

            temp.append(parent)
            temp.append(child)
            temp.append(makeup_volume)
            temp.append(effective_makeup_volume)
            final.append(temp)

        headers = ['friendly', 'parent', 'child', 'makeup_volume', 'effective_makeup_volume']
        df = DataFrame(final, columns=headers)

        return df

    def get_node_count(self):
        """Provides number of main hierarchy nodes

        :returns: Node count
        :rtype: int
        """

        return len(self.h_direct)

    def save_hierarchy(self, path, top_node):
        raise NotImplementedError()

    def load_hierarchy(self, path):
        raise NotImplementedError()

#
# if __name__ == '__main__':
#     import timeit
#
#     leaf_node_id = 'f1'
#     parent_node_id = 'b1'
#     test_passes = 1000
#     fake_sets_to_generate = 10
#
#     # This is declared globally for testing purposes.  No reason for it otherwise.
#     h = BOM()
#
#     fake_data = [
#         ['a', 'b', 3],
#         ['a', 'c', 2],
#         ['a', 'd', 1],
#         ['b', 'e', 10],
#         ['b', 'f', 2],
#         ['c', 'f', 2],
#         ['c', 'g', 2],
#         ['a', 'f', 1]
#     ]
#
#     leaves = ('d', 'e', 'f', 'g')
#
#     print "Generating {0} fake assemblies".format(fake_sets_to_generate)
#     result = []
#     part_cost = {}
#
#     for fsg in xrange(fake_sets_to_generate):
#         for fd in fake_data:
#             parent = '{}{}'.format(fd[0], fsg)
#             child = '{}{}'.format(fd[1], fsg)
#             muv = fd[2]
#
#             if fd[1] in leaves:
#                 part_cost[child] = 0.1 * fsg * fd[2]
#
#             result.append((parent, child, muv))
#
#     print "Generation Complete.  Loading BOM Object..."
#
#     # Create starting nodes for the CO dimensions
#     #h.add_node('root', 51, '~')
#     #h.add_node('root', 52, '~')
#     #h.add_node('root', 53, '~')
#
#     record_count = 0
#     for r in result:
#         h.add_node(r[0], r[1], r[2])
#         record_count += 1
#
#     print "Loading the BOM Object Complete. {0} Records Loaded.".format(record_count)
#
#     print "Loading Leaf level costs..."
#
#     # Create starting nodes for the CO dimensions
#     #h.add_node('root', 51, '~')
#     #h.add_node('root', 52, '~')
#     #h.add_node('root', 53, '~')
#
#     record_count = 0
#     for pc in part_cost:
#         h.set_cost(pc, part_cost[pc])
#         record_count += 1
#
#     print "Loading leaf level part cost complete. {0} base costs set.".format(record_count)
#
#     print
#     print
#     print "Beginning Tests..."
#
#     print "Leaf Node Used for Tests is {0}".format(leaf_node_id)
#     print "Parent Node Used for Tests is {0}".format(parent_node_id)
#     print "{0} Test passes used for timing.".format(test_passes)
#
#     print "STARTING NODE COUNT is {0}".format(h.get_node_count())
#
#     print "-----------"
#     s = "h.get_node(leaf_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_parents(leaf_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_parent_ids(leaf_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_siblings(leaf_node_id, parent_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_sibling_ids(leaf_node_id, parent_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.is_child_of(leaf_node_id, parent_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.is_parent_of(parent_node_id, leaf_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, leaf_node_id, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_children(parent_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     s = "h.get_children_ids(parent_node_id)"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h, parent_node_id")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     print "MAXIMUM STRESS TEST - Get all Costs"
#     s = "h.get_all_costs()"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
#
#     print "-----------"
#     print "MAXIMUM STRESS TEST - Get the complete BOM"
#     s = "h.get_bom('root')"
#
#     print s
#     t = timeit.Timer(s, "from __main__ import h")
#     print "%.2f usec/pass" % (test_passes * t.timeit(number=test_passes) / test_passes)
