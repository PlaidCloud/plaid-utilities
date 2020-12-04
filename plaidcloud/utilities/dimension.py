#!/usr/bin/env python
# coding=utf-8
"""
A highly optimized class for fast dimensional hierarchy operations
"""

from __future__ import absolute_import
from plaidcloud.utilities import frame_manager
import json
import os
import pandas as pd
import subprocess
import sys
from pandas import DataFrame, HDFStore, Series
from six import integer_types, StringIO
import six


__author__ = "Paul Morel, Andrew Hodgson, Michael Rea"
__copyright__ = "Copyright 2010-2019, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Michael Rea"
__email__ = "michael.rea@tartansolutions.com"


class Node(object):

    """A hierarchy node object"""

    def __init__(self, parent_obj, child_id, consolidation='+'):
        """Initializes node object

        Args:
            parent_obj (Node, optional): Parent Object
            child_id (str, optional): Child node key
            consolidation (str): Consolidation Type

        Returns:
            None
        """

        self.id = child_id

        # Main hierarchy Info
        self.parent = parent_obj
        self.children = []
        self.consolidation = consolidation

        # Alt Hierarchy Info
        self.alt_parents = {}
        self.alt_consolidation = {}

        # Let the parent know they just had a new baby
        # An alt hierarchy rollup will have a None parent
        if self.parent is not None:
            self.parent.add_child(self)

        # Create a value object, which is a special little class for pretty
        # value getting and setting (e.g. `node.value['cost'] = 100`)
        self.value = Value(self)

    def __repr__(self):
        return "<(Node ID: {} ({})>".format(self.id, self.consolidation)

    def add_child(self, child_obj, h='main'):
        """Add a child to the node

        Args:
            child_obj (Node): Node object to Add
            h (str, optional): hierarchy - 'main' or name of alt hierarchy

        Returns:
             None
        """

        # Add the child to this parent's list of children
        if child_obj.id not in [c.id for c in self.children]:
            self.children.append(child_obj)

        if h == 'main':
            # Remove the child from earlier parent
            if child_obj.parent is not None and child_obj.parent.id != self.id:
                child_obj.parent.remove_child(child_obj.id)

            # Point the child to the current parent
            child_obj.parent = self
        else:
            child_obj.alt_parents[h] = self

    def remove_child(self, child_id):
        """Removes child from node

        Args:
            child_id (str): Child node key to remove

        Returns:
            None
        """

        current_children = self.get_children()
        temp_children = []
        for c in current_children:
            if c.id != child_id:
                temp_children.append(c)
        # The new list should be missing the child.
        self.children = temp_children

    def set_consolidation(self, consolidation_type):
        """Set the consolidation type of the node

        Args:
            consolidation_type (str): +, -, or ~

        Returns:
            None
        """

        if consolidation_type in ('~', '+', '-'):
            self.consolidation = consolidation_type

    def get_parent(self):
        """Returns parent object of node

        Returns:
            Node: The parent object
        """

        return self.parent

    def get_grandparent(self):
        """Returns grandparent object of node

        Returns:
            Node: The grandparent object
        """

        try:
            return self.parent.parent
        except:
            return None

    def get_siblings(self):
        """Finds siblings of the node

        Returns:
            list: list of siblings node objects including current node
        """

        return self.parent.get_children()

    def get_children(self):
        """Returns list of children node objects

        Returns:
            list: list of child node objects
        """

        return self.children

    def get_leaves(self):
        """Returns the full set of leaves below the node

        Returns:
            list: leaves below node as node objects
        """

        leaves = []

        children = self.get_children()
        if len(children) == 0:
            return [self]

        for c in children:
            c_leaves = c.get_leaves()
            leaves += c_leaves

        return leaves

    def is_child_of(self, parent_id):
        """Checks if the node is a child of the specified parent

        Args:
            parent_id (str): parent node key

        Returns:
            bool: True if node descends from the parent
        """

        try:
            if self.parent.id == parent_id:
                return True
            else:
                return False
        except:
            return False

    def is_parent_of(self, child_id):
        """Checks if the node is a parent of the specified child

        Args:
            child_id (str): child node key

        Returns:
            bool: True if child descends from the node
        """
        for c in self.get_children():
            if c.id == child_id:
                return True
        return False

    def set_alt_parent(self, alternate_hierarchy, parent_obj):
        """Adds node to an alternate parent

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :param parent_obj: parent node object
        :type parent_obj: Node
        :returns: None
        :rtype: None
        """

        # First make sure this child is not already a child in the main
        # This is to catch redundant additions
        if self.parent is not None:
            parent_id = self.parent.id
        else:
            parent_id = None

        if parent_id != parent_obj.id:
            self.alt_parents[alternate_hierarchy] = parent_obj
        else:
            parent_obj.add_child(self)

    def get_alt_parent(self, alternate_hierarchy):
        """Returns the parent within a specified alt hierarchy

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :returns: parent node object
        :rtype: Node
        """

        try:
            return self.alt_parents[alternate_hierarchy]
        except:
            # This must be a node brought over from adding a main hierarhcy parent node
            return self.get_parent()

    def get_alt_parents(self, alternate_hierarchy):
        """List of all alt hierarchy parents

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :returns: list of alt hierarchy parent objects
        :rtype: list
        """

        return self.alt_parents

    def get_alt_grandparent(self, alternate_hierarchy):
        """Returns the grandparent within a specified alt hierarchy

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :returns: grandparent node object
        :rtype: Node
        """

        return self.get_alt_parent(alternate_hierarchy).get_alt_parent(alternate_hierarchy)

    def get_alt_siblings(self, alternate_hierarchy):
        """Returns the list of siblings to the node in the specified
        alt hierarchy

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :returns: list of sibling node objects including current node
        :rtype: list
        """

        return self.get_alt_parent(alternate_hierarchy).get_children()

    def is_alt_child_of(self, alternate_hierarchy, parent_id):
        """Checks if the current node is a child of the specified parent

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :param parent_id: parent node key
        :type parent_id: str or unicode
        :returns: True if node is a child
        :rtype: bool
        """

        try:
            if self.get_alt_parent(alternate_hierarchy).id == parent_id:
                return True
            else:
                return False
        except:
            return False

    def set_alt_consolidation(self, alternate_hierarchy, consolidation_type):
        """Set the consolidation type of the node in an alt hierarchy

        :param alternate_hierarchy: alternate hierarchy key
        :type alternate_hierarchy: str or unicode
        :param consolidation_type: +, -, or ~
        :type consolidation_type: str or unicode
        :returns: None
        :rtype: None
        """

        if consolidation_type in ('~', '+', '-'):
            self.alt_consolidation[alternate_hierarchy] = consolidation_type


class Value(object):
    """A dict-like key-value store which sums values of the node's descendents.
    Nodes instantiate this during their creation. A Node's values are accessed
    using the standard __getitem__, __setitem__, and __delitem__ syntax with
    square brackets. If a value key is not set, 0 is returned.

    e.g.
        >>> node = Node(None, None)
        >>> node.value['x'] = 100
        >>> node.value['x']
        100
        >>> del node.value['x']
        >>> node.value['x']
        0

        # If `root` is a parent of `node_a` and `node_b`, sum children values:
        >>> root = Node(None, None)
        >>> node_a = Node(root, 'a')
        >>> node_b = Node(root, 'b')
        >>> root.value['x'] = 100
        >>> node_a.value['x'] = 25
        >>> node_b.value['x'] = 0.1
        >>> node_b.value['y'] = 0.5
        >>> root.value['x']
        100
        >>> root.value['y']
        0.5
        >>> del root.value['x']
        >>> root.value['x']
        25.1
        >>> root.value['y']
        0.5
        >>> root.value['y'] = 'Strings not allowed'
        Traceback (most recent call last):
        ...
        TypeError: For key 'y', value must be numeric; found 'Strings not allowed'

    Beware: there is a semantic name collision on "value". Values are groups of
    key-value pairs.
    """

    def __init__(self, node):
        """
        :param node: the Node object creating this Value object
        :type node: plaid.core.utility.hierarchy.Node
        """

        self.node = node
        self.value_dict = {}

    def __setitem__(self, value_name, value):
        """Set the Value key-value pair.
        Use the standard __setitem__ syntax: `value['x'] = 10`
        Only accept numeric values.

        :param value_name: The key of the pair
        :type value_name: str or unicode
        :param value: The value of the pair
        :type value: int, long, or float
        """

        acceptable_types = integer_types + (float, )
        if isinstance(value, acceptable_types):
            self.value_dict[value_name] = value
        else:
            raise TypeError("For key {!r}, value must be numeric; found {!r}"
                            .format(value_name, value))

    def __getitem__(self, value_name):
        """Get the value given a key in the Value object.
        Use the standard Python __getitem__ syntax: `value['x']`
        Returns the sum of the node's children's values for that item,
        defaulting at 0.

        :param value_name: The key of the pair
        :type value_name: str or unicode
        """

        try:
            item = self.value_dict[value_name]
        except KeyError:
            # If the value is not set, sum the children's values.
            # __getitem__ is called recursively. The base case is that a node
            # does not have any children, which results in a sum of 0.
            item = sum(n.value[value_name] for n in self.node.get_children())
        return item

    def __delitem__(self, value_name):
        """Delete the value given a key in the Value object.
        Use the standard Python __delitem__ syntax: `del value['x']`

        :param value_name: The key of the pair
        :type value_name: str or unicode
        """

        del self.value_dict[value_name]


class Dimension(object):

    """
    Dimension Class for fast hierarchy, alias, property & attribute operations
    Dimensions contain nodes (members) arranged into one or more hierarchies.
    Each node can possess things like aliases, and methods to manage traversal.
    """

    def __init__(self, **kwargs):
        """Class init function sets up basic structure

        :param load_path: optional path to saved hierarchy load file to load initially
        :type load_path: str or unicode
        :returns: None
        :rtype: None

        TODO: We need to trap and address accidental recursion.
        """

        self.h_direct = {}
        self.h_children = {}
        self.h_alias = {}
        self.h_property = {}
        self.a_direct = {}
        self.a_children = {}

        self.name = kwargs.get("name", "Unnamed")

        # Create a default Alternate hier to support quick ingestion of PCM data
        # for which alt hierarchies do not have names, and any other use case
        # that merits alt hier capability w/o need or want to name the alt hierarchy.

        # This name is lame, but what do you call a 'default' alternate hier.
        self.alternate_0 = self.name + "_Alternate_0"

        self.clear()
        self.add_alt_hierarchy(self.alternate_0)

        load_path = kwargs.get("load_path", None)

        if load_path is not None:
            self.load(load_path)

    def get_alt_hierarchies(self):
        """Returns current alt hierarchies

        :returns: List of alternate hierarchies
        :rtype: list
        """

        return list(self.a_children.keys())

    def add_alt_hierarchy(self, alternate_hierarchy):
        """Creates a new alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :returns: None
        :rtype: None
        """

        current_ahs = self.get_alt_hierarchies()

        if alternate_hierarchy not in current_ahs:
            self.a_children[alternate_hierarchy] = {}
            self.a_direct[alternate_hierarchy] = {}

            node = Node(None, 'root')
            self.a_direct[alternate_hierarchy]['root'] = node

    def add_alt_node(self, parent_id, child_id, consolidation_type, alternate_hierarchy=None):
        """Adds an existing main hierarchy node to the specified alternate hierarchy
        both leaves and folders can be added to the alt hierarchies

        20170721: Move alternate_hierarchy to last, optional argument.  If unspecified, we will default
        It to something we can live with, so as to support ingestion of PCM parentchild and
        be a bit more sturdy.

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param parent_id: parent node key
        :type parent_id: str or unicode
        :param child_id: child node key
        :type child_id: str or unicode
        :param consolidation_type:
        :type consolidation_type: str or unicode
        :returns: None
        :rtype: None
        """

        if alternate_hierarchy is None:
            alternate_hierarchy = self.alternate_0

        # first check to see if the child_id is defined in the main hierachy
        try:
            child_obj = self.get_node(child_id)
        except:
            # This is hopefully an alt group because adding a leaf is bad juju
            # Doing a check for group vs. alt leaf is really hard since the group may be added first
            # TODO - Look at adding a post loading proces to check for leaves disconnected from the main hierarchy
            child_obj = Node(None, child_id, consolidation_type)

        child_obj.set_alt_consolidation(alternate_hierarchy, consolidation_type)

        # Now deal with adding it to the alternate hierarchy
        try:
            parent_obj = self.get_alt_node(alternate_hierarchy, parent_id)
        except:
            # Parent does not exist yet.  Handle out of sequence data gracefully.
            alt_root_parent = self.get_alt_node(alternate_hierarchy, 'root')
            parent_obj = Node(None, parent_id, '~')  # This is non-main hierarchy node
            alt_root_parent.add_child(parent_obj, alternate_hierarchy)
            self.a_direct[alternate_hierarchy][parent_id] = parent_obj

        if child_id in self.a_direct[alternate_hierarchy]:
            # This already exists.
            # Could have been added as an out of sequence member
            # Need to move the node to the proper parent
            parent_obj.add_child(child_obj, alternate_hierarchy)
        else:
            # Doesn't exist.  Simple add.
            parent_obj.add_child(child_obj, alternate_hierarchy)
            self.a_direct[alternate_hierarchy][child_id] = child_obj

    def export_to_analyze(self, cloud, project, model=None, name='Unnamed_Dimension', top_node='root'):
        """Exports the current Dimension to Analyze

        As dimensions are essentially 3 different tables, they
        can be stored in/loaded from analyze.

        :param cloud: the cloud ID to save in
        :type cloud: int
        :param project: the project ID to save in
        :type project: int
        :param model: the Model ID to save in, or None for a project-level table
        :type model: int or None
        :param name: A friendly name for the tables.
        :type name: str or Unicode
        :param top_node: the top node of the dimension
        :type top_node: str or unicode"""

        main_name = '{}_hierarchy_table'.format(name)
        alias_name = '{}_alias_table'.format(name)
        property_name = '{}_property_table'.format(name)

        # hierarchy, alias, property
        # Create Pandas frames for the main hierarchy,
        # the alias table, and the property dict.

        headers = ['hierarchy', 'parent', 'child', 'consolidation_type']
        alias_headers = property_headers = ['name', 'node', 'value']
        main_list = self._get_main_list_format(top_node)
        alts = self.get_alt_hierarchies()
        if len(main_list) > 0:
            for a in alts:
                pc_list_alt = self._get_alt_list_format(a)
                main_list += pc_list_alt

        property_list = [
            [prop, node, value]
            for prop, data in self.h_property.items()
            for node, value in data.items()
        ]
        alias_list = [
            [alias, node, value]
            for alias, data in self.h_alias.items()
            for node, value in data.items()
        ]

        df = DataFrame(main_list, columns=headers, dtype=str).fillna('')
        alias_df = DataFrame(alias_list, columns=alias_headers, dtype=str).fillna('')
        property_df = DataFrame(property_list, columns=property_headers, dtype=str).fillna('')

        # Save the frames to Analyze
        raise NotImplementedError()
        # if model is not None:
        #     frame_manager.save_frame(df, cloud, project, model, main_name)
        #     frame_manager.save_frame(alias_df, cloud, project, model, alias_name)
        #     frame_manager.save_frame(property_df, cloud, project, model, property_name)
        # else:
        #     frame_manager.save_frame(df, cloud, project, model,
        #                              '{}{}'.format(frame_manager.PROJECT_FRAME_PREFIX, main_name))
        #
        #     frame_manager.save_frame(alias_df, cloud, project, model,
        #                              '{}{}'.format(frame_manager.PROJECT_FRAME_PREFIX, alias_name))
        #
        #     frame_manager.save_frame(property_df, cloud, project, model,
        #                              '{}{}'.format(frame_manager.PROJECT_FRAME_PREFIX, property_name))

    def get_alt_node(self, alternate_hierarchy, node_id):
        """Gets the node from the alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: node object
        :rtype: Node
        """

        try:
            return self.a_direct[alternate_hierarchy][node_id]
        except:
            # This must be member inside of a folder that was added
            try:
                return self.h_direct[node_id]
            except:
                raise Exception("No node named %s in specified alternate hierarchy or the main" % node_id)

    def get_alt_parent(self, alternate_hierarchy, node_id):
        """Gets the node's parent within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: parent object
        :rtype: Node
        """

        return self.get_alt_node(alternate_hierarchy, node_id).get_alt_parent(alternate_hierarchy)

    def get_alt_parent_id(self, alternate_hierarchy, node_id):
        """Gets the node's parent within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node keys
        :type node_id: str or unicode
        :returns: parent node key
        :rtype: str or unicode
        """

        return self.get_alt_parent(alternate_hierarchy, node_id).id

    def get_alt_parents(self, node_id, hierarchy):
        """Finds all the alt hierarchy parents of the node

        :param node_id: node key
        :type node_id: str or unicode
        :param hierarchy: alt hierarchy key
        :type hierarchy: str or unicode
        :returns: List of alt parent objects
        :rtype: list
        """

        return self.get_node(node_id).get_alt_parents(hierarchy)

    def get_alt_parent_ids(self, node_id, hierarchy):
        #"""Finds all the alt hierarchy parents of the node

        #:param node_id: node key
        #:type node_id: str or unicode
        #:returns: list of alt parent node keys
        #:rtype: list
        #"""

        objs = self.get_alt_parents(node_id, hierarchy)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_alt_grandparent(self, alternate_hierarchy, node_id):
        """Returns the grandparent of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: grandparent node object
        :rtype: Node
        """

        return self.get_alt_node(alternate_hierarchy, node_id).get_alt_grandparent(alternate_hierarchy)

    def get_alt_grandparent_id(self, alternate_hierarchy, node_id):
        """Returns the grandparent of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: grandparent node key
        :rtype: str or unicode
        """

        return self.get_alt_grandparent(alternate_hierarchy, node_id).id

    def get_alt_siblings(self, alternate_hierarchy, node_id):
        """Finds the siblings of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of sibling node objects including current node
        :rtype: list
        """

        return self.get_alt_node(alternate_hierarchy, node_id).get_alt_siblings(alternate_hierarchy)

    def get_alt_sibling_ids(self, alternate_hierarchy, node_id):
        """Finds the siblings of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of sibling node keys including current node
        :rtype: list
        """

        objs = self.get_alt_siblings(alternate_hierarchy, node_id)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_alt_children(self, alternate_hierarchy, node_id):
        """Finds the children of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of children node objects
        :rtype: list
        """

        return self.get_alt_node(alternate_hierarchy, node_id).get_children()

    def get_alt_children_ids(self, alternate_hierarchy, node_id):
        """Finds the children of the node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of children node objects
        :rtype: list
        """

        objs = self.get_alt_children(alternate_hierarchy, node_id)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_alt_leaves(self, alternate_hierarchy, node_id):
        """Finds the leaves below a node within a specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of leaf level node objects
        :rtype: list
        """

        return self.get_alt_node(alternate_hierarchy, node_id).get_leaves()

    def get_alt_leaf_ids(self, alternate_hierarchy, node_id):
        """Finds the leaves below a node within a specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of leaf level node keys
        :rtype: list
        """

        objs = self.get_alt_leaves(alternate_hierarchy, node_id)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def set_alt_consolidation(self, alternate_hierarchy, node_id, consolidation_type='+'):
        """Sets the consolidation type of a node within the specified alt hierarchy

        :param alternate_hierarchy: alt hierarchy key
        :type alternate_hierarchy: str or unicode
        :param node_id: node key
        :type node_id: str or unicode
        :param consolidation_type: consolidation type +, -, or ~
        :type consolidation_type: str or unicode
        :returns: None
        :rtype: None
        """

        if consolidation_type in ('~', '+', '-'):
            node = self.get_alt_node(alternate_hierarchy, node_id)
            node.set_alt_consolidation(alternate_hierarchy, consolidation_type)

    def add_node(self, parent_id, child_id, consolidation='+'):
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

        if parent_id == child_id:
            raise Exception("Child cannot have itself as Parent")

        try:
            parent_obj = self.get_node(parent_id)
        except:
            # Parent does not exist yet.  Handle out of sequence data gracefully.
            root_parent = self.get_node('root')
            parent_obj = Node(root_parent, parent_id, '~')
            self.h_direct[parent_id] = parent_obj

        if child_id in self.h_direct:
            # This already exists.
            # Could have been added as an out of sequence member
            # Need to move the node to the proper parent
            node = self.h_direct[child_id]
            node.set_consolidation(consolidation)
            parent_obj.add_child(node)
        else:
            # Doesn't exist.  Simple add.
            node = Node(parent_obj, child_id, consolidation)
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
            parent = self.get_parent(node_id)
            alt_parents = self.get_alt_parents(node_id, '')
        except:
            # Not present.  No need to do anything
            pass
        else:
            # Remove from main hierarchy
            del self.h_direct[node_id]
            parent.remove_child(node_id)

            # Remove from alternate hierarchies
            for apk in alt_parents:
                try:
                    del self.a_direct[apk][node_id]
                except:
                    # this is not directly added to alt hierarchy
                    pass
                alt_parent = self.get_alt_parent(apk, node_id)
                alt_parent.remove_child(node_id)

        # Remove aliases
        for k in self.h_alias:
            try:
                del self.h_alias[k][node_id]
            except:
                # doesn't have an alias here
                pass

        # Remove properties
        for k in self.h_property:
            try:
                del self.h_property[k][node_id]
            except:
                # doesn't have a property here
                pass

    def get_node(self, node_id):
        """Gets the node object

        :param node_id: node key
        :type node_id: str or unicode
        :returns: Node object
        :rtype: Node
        """

        try:
            return self.h_direct[node_id]
        except:
            raise Exception('No node found with the name %s' % node_id)

    def get_parent(self, node_id):
        """Finds parent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node object of parent
        :rtype: Node
        """

        return self.get_node(node_id).get_parent()

    def get_parent_id(self, node_id):
        """Finds parent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node key of parent
        :rtype: str or unicode
        """

        try:
            return self.get_parent(node_id).id
        except:
            return None

    def get_grandparent(self, node_id):
        """Finds grandparent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node object of grandparent
        :rtype: Node
        """

        return self.get_node(node_id).get_grandparent()

    def get_grandparent_id(self, node_id):
        """Finds grandparent of node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node key of grandparent
        :rtype: str or unicode
        """

        try:
            return self.get_grandparent(node_id).id
        except:
            return None

    def get_siblings(self, node_id):
        """Finds sibling nodes of specified node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node objects of all siblings including the current node
        :rtype: list
        """

        return self.get_node(node_id).get_siblings()

    def get_sibling_ids(self, node_id):
        """Finds sibling nodes of specified node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: node keys of all siblings including the current node
        :rtype: list
        """

        objs = self.get_siblings(node_id)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

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

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_leaves(self, node_id):
        """Finds all leaves below the node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of leaf level node objects
        :rtype: list
        """

        return self.get_node(node_id).get_leaves()

    def get_leaf_ids(self, node_id):
        """Finds all leaves below the node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of leaf level node keys
        :rtype: list
        """

        objs = self.get_leaves(node_id)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_leaves_at_level(self, node_id, level, absolute=True, _current_level=0):
        """Finds leaves of a branch at the specified level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: number of generations to descend for leaves
        :type level: int
        :param absolute: True if level is from root, False from node
        :type absolute: bool
        :param _current_level: private variable for recursion
        :type _current_level: int
        :returns: list of leaf level node objects
        :rtype: list
        """

        final = []
        descendants = self.get_descendants_at_level(node_id, level, absolute)

        for d in descendants:
            if len(d.get_children()) == 0:
                # This is a leaf
                final.append(d)

        return final

    def get_leaf_ids_at_level(self, node_id, level, absolute=True, _current_level=0):
        """Finds leaves of a branch at the specified level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: number of generations to descend for leaves
        :type level: int
        :param absolute: True if level is from root, False from node
        :type absolute: bool
        :param _current_level: private variable for recursion
        :type _current_level: int
        :returns: list of leaf level node keys
        :rtype: list
        """

        objs = self.get_leaves_at_level(node_id, level, absolute)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def get_descendants_at_level(self, node_id, level, absolute=True, _current_level=0):
        """Finds all node types of a branch at the specified level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: number of generations to descend for leaves
        :type level: int
        :param absolute: True if level is from root, False from node
        :type absolute: bool
        :param _current_level: private variable for recursion
        :type _current_level: int
        :returns: list of leaf level node objects
        :rtype: list
        """

        if absolute is True:
            # need to determine how deep we already are
            _current_level = self.get_generation(node_id)

        final = []
        if _current_level <= level:
            children = self.get_children(node_id)

            if level == _current_level:
                for c in children:
                    final.append(c)

            elif _current_level < level:
                for c in children:
                    # Go deeper if we haven't hit the limit level yet
                    child_leaves = self.get_descendants_at_level(c.id, level, False, _current_level + 1)

                    if len(child_leaves) > 0:
                        final += child_leaves

        return final

    def get_descendant_ids_at_level(self, node_id, level, absolute=True, _current_level=0):
        """Finds all node types of a branch at the specified level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: number of generations to descend for leaves
        :type level: int
        :param absolute: True if level is from root, False from node
        :type absolute: bool
        :param _current_level: private variable for recursion
        :type _current_level: int
        :returns: list of leaf level node keys
        :rtype: list
        """

        objs = self.get_descendants_at_level(node_id, level, absolute)

        ids = []
        for o in objs:
            ids.append(o.id)

        return ids

    def import_from_analyze(self, cloud, project, model=None, name='Unnamed_Dimension'):
        """Imports a dimension from Analyze

        :param cloud: Cloud ID from analyze
        :type cloud: int
        :param project: Project ID from analyze
        :type project: str or unicode
        :param model: Model id from Analyze (none for project import)
        :type model: str or unicode
        :param name: Friendly name
        :type name: str or unicode
        :returns: None
        :rtype: None"""

        main_name = '{}_hierarchy_table'.format(name)
        alias_name = '{}_alias_table'.format(name)
        property_name = '{}_property_table'.format(name)

        raise NotImplementedError()
        #
        # # Load the Dimension
        # if model is not None:
        #     main_result = frame_manager.load_frame(cloud, project, model, main_name)
        # else:
        #     main_result = frame_manager.load_project_frame(cloud, project, main_name)
        # self.load_dataframe(main_result)
        #
        # self.h_alias = {}
        # self.h_property = {}
        #
        # # Load the alias table
        # if model is not None:
        #     alias_result = frame_manager.load_frame(cloud, project, model, alias_name)
        # else:
        #     alias_result = frame_manager.load_project_frame(cloud, project, alias_name)
        # if len(alias_result):
        #     for index, row in alias_result:
        #         if row['name'] not in self.h_alias:
        #             self.h_alias[row['name']] = {}
        #         self.h_alias[row['name']][row['node']] = row['value']
        #
        # # Load the property table.
        # if model is not None:
        #     property_result = frame_manager.load_frame(cloud, project, model, property_name)
        # else:
        #     property_result = frame_manager.load_project_frame(cloud, project, property_name)
        # if len(property_result):
        #     for _, row in property_result.iterrows():
        #         if row['name'] not in self.h_property:
        #             self.h_property[row['name']] = {}
        #         self.h_property[row['name']][row['node']] = row['value']

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

    def is_descendent_of(self, node_id, ancestor_id):
        """Checks if node is a decendant of an ancestor node

        :param node_id: node key
        :type node_id: str or unicode
        :param ancestor_id: node key of ancestor
        :type ancestor_id: str or unicode
        :returns: True if the node is an ancestor
        :rtype: bool
        """

        aids = self.get_ancestor_ids(node_id)

        if ancestor_id in aids:
            return True
        else:
            return False

    def is_below_group(self, node_id, group_id):
        """Checks if a node is contained in a group

        :param node_id: node key
        :type node_id: str or unicode
        :param group_id: node key of group
        :type group_id: str or unicode
        :returns: True if node is contained in group
        :rtype: bool
        """

        children = self.get_children_ids(node_id)

        if len(children) == 0:
            return False

        if node_id in children:
            return True
        else:
            for c in children:
                if self.is_below_group(node_id, c) is True:
                    return True
            return False

    def get_parent_below_group(self, child_id, alternate_hierarchy, group_id='root'):
        """Finds the parent of the node within an alternate hierachy group

        :param child_id: node key
        :type child_id: str or unicode
        :param alternate_hierarchy: alternate hierarchy group
        :type alternate_hierarchy: str or unicode
        :param group_id: node key of upper level group
        :type group_id: str or unicode
        :returns: parent object
        :rtype: Node
        """

        children = self.get_alt_children(alternate_hierarchy, group_id)

        for c in children:
            if c.id == child_id:
                # Return the current node if it is the parent
                return self.get_alt_node(alternate_hierarchy, group_id)
            else:
                parent_obj = self.get_parent_below_group(child_id, alternate_hierarchy, c.id)

                if parent_obj is not None:
                    return parent_obj
        return None

    def get_parent_below_group_id(self, child_id, alternate_hierarchy, group_id='root'):
        """Finds the parent of the node within an alternate hierachy group

        :param child_id: node key
        :type child_id: str or unicode
        :param alternate_hierarchy: alternate hierarchy group
        :type alternate_hierarchy: str or unicode
        :param group_id: node key of upper level group
        :type group_id: str or unicode
        :returns: parent key
        :rtype: str or unicode
        """

        parent_node = self.get_parent_below_group(child_id, alternate_hierarchy, group_id)

        if parent_node is not None:
            return parent_node.id
        else:
            return None

    def get_descendents(self, node_id):
        """Finds all descendants of the node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of descendant objects
        :rtype: list
        """
        children = self.get_children(node_id)

        for c in children:
            lower_children = self.get_descendents(c.id)
            if len(lower_children) > 0:
                children += lower_children

        return children

    def get_descendent_ids(self, node_id):
        """Finds all descendants of the node

        :param node_id: node key
        :type node_id: str or unicode
        :returns: list of descendant IDs
        :rtype: list
        """

        children = self.get_children_ids(node_id)

        for c in children:
            lower_children = self.get_descendent_ids(c)
            if len(lower_children) > 0:
                children += lower_children

        return children

    def get_ancestor(self, node_id, level):
        """Traversas up the hierarchy to find the specified ancestor

        :param node_id: node key
        :type node_id: str or unicode
        :param level: generations to go back for ancenstor
        :type level: int
        :returns: ancestor node
        :rtype: object
        """

        return self.get_node(self.get_ancestor_id(node_id, level))

    def get_ancestor_id(self, node_id, level):
        """Traversas up the hierarchy to find the specified ancestor

        :param node_id: node key
        :type node_id: str or unicode
        :param level: generations to go back for ancenstor
        :type level: int
        :returns: ancestor node key
        :rtype: str or unicode
        """

        ancestors = self.get_ancestor_ids(node_id)

        if len(ancestors) >= level:
            return ancestors[level - 1]
        else:
            return 'root'

    def get_ancestors(self, node_id):
        """Returns an ordered list of the node lineage objects

        :param node_id: node key
        :type node_id: str or unicode
        :returns: Node lineage
        :rtype: list
        """
        lineage = self.get_ancestor_ids(node_id)

        lineage_nodes = []
        for l in lineage:
            lineage_nodes.append(self.get_node(l))

        return lineage_nodes

    def get_ancestor_ids(self, node_id):
        """Returns an ordered list of the node lineage keys

        :param node_id: node key
        :type node_id: str or unicode
        :returns: Node lineage
        :rtype: list
        """

        lineage = []
        parent = self.get_parent_id(node_id)

        if parent not in ('root', None):
            lineage.append(parent)
            upper_parents = self.get_ancestor_ids(parent)
            if len(upper_parents) > 0:
                lineage += upper_parents

        return lineage

    def get_ancestor_at_level(self, node_id, level):
        """Traverses up the hierarchy to find the specified ancestor at a given level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: level of ancestor
        :type level: int
        :returns: ancestor node key
        :rtype: str or unicode
        """

        return self.get_node(self.get_ancestor_id_at_level(node_id, level))

    def get_ancestor_id_at_level(self, node_id, level):
        """Traverses up the hierarchy to find the specified ancestor at a given level

        :param node_id: node key
        :type node_id: str or unicode
        :param level: level of ancestor
        :type level: int
        :returns: ancestor node key
        :rtype: str or unicode
        """
        ancestors = self.get_ancestor_ids(node_id)

        if len(ancestors) == 0:
            return None
        elif level > len(ancestors):
            return None
        else:
            return ancestors[-level]

    def get_intersection(self, main_node_id, alt_nodes):
        """Finds the leaves contained in all sets

        :param main_node_id: node key in main hierarchy. root will work.
        :type main_node_id: str or unicode
        :param alt_nodes:list of alternate hierarchies to use
        :type alt_nodes: list of alt namespaces or list of (namespace, node)
        :returns: Set of leaves in all input sets
        :rtype: list
        """

        if main_node_id in ('root', None):
            # Don't waste time getting all the leaves
            # We know anything in the alternates will
            # be a match
            check_main = False
            final = []
        else:
            final = self.get_leaf_ids(main_node_id)
            check_main = True

        if isinstance(alt_nodes, str):
            # handle the specification of just one alt hier as string
            alt_nodes = [alt_nodes]

        for a in alt_nodes:
            if isinstance(a, str):
                start_node = 'root'
                hier = a
            else:
                # A tuple is specified to allow a non-root starting point in alt
                start_node = a[1]
                hier = a[0]

            a_set = self.get_alt_leaf_ids(hier, start_node)
            if len(a_set) > 0:
                if check_main is False:
                    final = a_set
                    check_main = True
                else:
                    final = set(a_set).intersection(final)
            else:
                return ()
        return final

    def get_difference(self, main_node_id, alt_nodes):
        """Finds the leaves not contained in all sets

        :param main_node_id: node key in main hierarchy. root will work.
        :type main_node_id: str or unicode
        :param alt_nodes: list of alternate hierarchies to use
        :type alt_nodes: list
        :returns: Set of leaves contained in only one of the input sets
        :rtype: list
        """

        if main_node_id is None:
            # Don't waste time getting all the leaves
            # We know anything in the alternates will
            # show a difference
            check_main = False
            final = []
        else:
            final = self.get_leaf_ids(main_node_id)
            check_main = True

        if isinstance(alt_nodes, str):
            # handle the specification of just one alt hier as string
            alt_nodes = [alt_nodes]

        for a in alt_nodes:
            if isinstance(a, str):
                start_node = 'root'
                hier = a
            else:
                # A tuple is specified to allow a non-root starting point in alt
                start_node = a[1]
                hier = a[0]

            a_set = self.get_alt_leaf_ids(hier, start_node)

            if len(a_set) > 0:
                if check_main is False:
                    final = set(a_set)
                    check_main = True
                else:
                    if len(final) > 0:
                        final_1 = set(list(set(final) - set(a_set)))
                        final_2 = set(list(set(a_set) - set(final)))
                        final = final_1.union(final_2)
                    else:
                        # The difference must be the a_set since final is empty
                        final = set(a_set)

        return final

    def get_generation(self, node_id):
        """Returns the generation of the node in the main hierarchy

        :param node_id: node key
        :type node_id: str or unicode
        :returns: Generation
        :rtype: int
        """

        aids = self.get_ancestor_ids(node_id)
        return len(aids) + 1  # should be +1, right?  If I have 7 ancestors, then I am generation 8.

    def set_consolidation(self, node_id, consolidation_type='+'):
        """Sets the consolidation type on a node

        :param node_id: node key
        :type node_id: str or unicode
        :param consolidation_type: +, -, or ~ are acceptable
        :type consolidation_type: str or unicode
        :returns: None
        :rtype: None
        """

        if consolidation_type in ('~', '+', '-'):
            node = self.get_node(node_id)
            node.consolidation = consolidation_type

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
            temp = ['main', str(node_id), str(c.id), str(c.consolidation)]
            final.append(temp)

            sub_children = self._get_main_list_format(c.id)
            if len(sub_children) > 0:
                final += sub_children

        return final

    def _get_alt_list_format(self, alternate_hierarchy, node_id='root'):
        """Generates the alt parent child list recursively for saving

        :param alternate_hierarchy: name of alternate hierachy
        :type alternate_hierarchy: str or unicode
        :param node_id: current node to process
        :type node_id: str or unicode
        :returns: List of lists with parent child information
        :rtype: list
        """

        final = []

        children = self.get_alt_children(alternate_hierarchy, node_id)
        for c in children:
            try:
                consolidation_type = str(c.alt_consolidation[alternate_hierarchy])
            except:
                # This is coming from the main consolidation
                consolidation_type = str(c.consolidation)

            temp = [
                alternate_hierarchy,
                str(node_id),
                str(c.id),
                consolidation_type
            ]
            final.append(temp)

            if c.parent is None:
                # c.parent will be None for nodes added only to
                #   the alt hierarchy and not present in the main.
                #   If this is the case we need to go deeper, otherwise
                #   children will be dictated by the main or the node is
                #   a leaf already.
                sub_children = self._get_alt_list_format(alternate_hierarchy, c.id)
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
        self._save_hierarchy_to_txt(path)
        self._save_alias_to_txt(path)
        self._save_property_to_txt(path)

    def load(self, path):
        """Loads hierarchy, alias, and property

        :param path: File path to load
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        self._load_hierarchy_from_txt(path)
        self._load_alias_from_txt(path)
        self._load_property_from_txt(path)

    def _save_hierarchy_to_txt(self, path, top_node='root'):
        """Saves hierarchy information to txt file

        :param path:
        :type path: str or unicode
        :param top_node:
        :type top_node: str or unicode
        :returns: None
        :rtype: None
        """

        headers = ['hierarchy', 'parent', 'child', 'consolidation_type']
        key = 'hierarchy'
        pc_list = self._get_main_list_format(top_node)

        #TODO:  @adams has a chinese way to do this.  Here it is:
        #pc_list = list(itertools.chain(
            #self._get_main_list_format(top_node),
            #*[
                #self._get_alt_list_format(a)
                #for a in self.get_alt_hierarchies()
            #]
        #))

        alts = self.get_alt_hierarchies()
        for a in alts:
            pc_list_alt = self._get_alt_list_format(a)

        row_max = len(pc_list)

        #TODO:  We should convert this and all df.to_csv's to use the new typed_psv functions.
        df = DataFrame(pc_list, columns=headers, dtype=str).fillna('')
        df.to_csv(path, index=False, sep='|')

    def load_csv(self, path, header=0, sep=',', headers=None):
        """Loads the main and alternate hierarchies from a csv file

        :param path: path to csv file
        :type path: str or unicode
        :param header: line the headers are on.  Default 0 for none.
        :type header: int
        :param sep: data separater
        :type sep: str or unicode
        :param headers: header list or header rename dict
        :type headers: list or dict
        :returns: None
        :rtype: None
        """

        df = DataFrame.from_csv(path, header, sep)

        if isinstance(headers, dict):
            # Remap the current headers to the parent child etc
            # headers={'one' : 'hierarchy', 'two' : 'parent', ...}
            df = df.rename(columns=headers)

        elif isinstance(headers, list):
            # Set the headers
            df.columns = headers

        self.load_dataframe(df)

    def load_dataframe(self, df):
        """Loads a well formed dataframe into the hierarchy object

        Columns expected:
        - parent
        - child
        - hierarchy (optional - defaults to main)
        - consolidation_type (optional - defaults to +)

        :param df: The dataframe containing at least parent and child columns
        :type df: dataframe
        :returns: None
        :rtype: None
        """

        if df is not None:

            column_info = [col_name for col_name, data_type in six.iteritems(df.dtypes)]

            # Check to make sure all required columns are present
            if 'parent' not in column_info:
                raise Exception('Missing parent column. Found the following columns: {0}'.format(str(column_info)))

            if 'child' not in column_info:
                raise Exception('Missing child column. Found the following columns: {0}'.format(str(column_info)))

            # Set some defaults if these are missing
            if 'hierarchy' not in column_info:
                df.hierarchy = 'main'

            if 'consolidation_type' not in column_info:
                df.consolidation_type = '+'

            # order the df columns (hierarchy, parent, child, consolidation_type)
            # this enables using itertuples instead of iterrows
            df = df[['hierarchy', 'parent', 'child', 'consolidation_type']]

            # Find the list of alt hierarchies in the data
            df_ah = Series(df.hierarchy.unique())

            for ah in six.iteritems(df_ah):
                if ah[1] != 'main':
                    self.add_alt_hierarchy(ah[1])

            # Load the main data
            df_main = df[df.hierarchy == 'main']

            # Iterate over the data and build the hierachy using the add method
            for r in df_main.itertuples():
                # Tuple is formed as (index, hierarchy, parent, child, consolidation type)
                self.add_node(r[2], r[3], r[4])

            # Load the alt data
            df_alt = df[df.hierarchy != 'main']

            # Iterate over the data and build the hierachy using the add_alt method
            for r in df_alt.itertuples():
                # Tuple is formed as (index, hierarchy, parent, child, consolidation type)
                self.add_alt_node(r[2], r[3], r[4], r[1])

    def load_alias_dataframe(self, df):
        """Loads a well formed alias dataframe into the hierarchy object

        Columns expected:
        - node_id
        - alias
        - alias_name (optional - defaults to Description)

        :param df: The dataframe containing at least node_id and alias columns
        :type df: dataframe
        :returns: None
        :rtype: None
        """

        if df is not None:

            column_info = []
            for column_name, data_type in six.iteritems(df.dtypes):
                column_info.append(column_name)

            # Check to make sure all required columns are present
            if 'node_id' not in column_info:
                raise Exception('Missing node_id column. Found the following columns: {0}'.format(str(column_info)))

            if 'alias' not in column_info:
                raise Exception('Missing alias column. Found the following columns: {0}'.format(str(column_info)))

            # Set some defaults if these are missing
            if 'alias_name' not in column_info:
                df.hierarchy = 'Description'

            # order the df columns (hierarchy, parent, child, consolidation_type)
            # this enables using itertuples instead of iterrows
            df = df[['node_id', 'alias', 'alias_name']]

            # Iterate over the data and set the aliases
            for r in df.itertuples():
                # Tuple is formed as (index, node_id, alias, alias_name)
                self.set_alias(r[1], r[2], r[3])

    def _load_hierarchy_from_txt(self, path):
        """Loads dimsension information from saved txt file

        :param path: Absolute path to file
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        df = pd.read_csv(path, sep="|")

        self.load_dataframe(df)

    def clear(self):
        self.clear_hierarchy()
        self.clear_alias()
        self.clear_property()

    def clear_hierarchy(self):
        """Clears the main and alternate hierarchies

        :returns: None
        :rtype: None
        """

        self.h_direct = {}
        self.h_children = {}

        node = Node(None, 'root')
        self.h_direct['root'] = node

        # Clear the alternate hierarchies
        self.a_direct = {}
        self.a_children = {}

    def alias(self, node_id, alias_name='description'):
        """Retrieves the alias for a node

        :param node_id: node key
        :type node_id: str or unicode
        :param alias_name: alias type
        :type alias_name: str or unicode
        :returns: Alias
        :rtype: str or unicode
        """

        try:
            return self.h_alias[alias_name][node_id]
        except:
            return node_id

    def set_alias(self, node_id, alias, alias_name='description'):
        """Sets an alias for the specified node

        :param node_id: node key
        :type node_id: str or unicode
        :param alias: The alias to apply
        :type alias: str or unicode
        :param alias_name: The alias type
        :type alias_name: str or unicode
        :returns: None
        :rtype: None
        """

        if alias is not None:
            try:
                self.h_alias[alias_name][node_id] = alias
            except:
                if alias_name not in self.h_alias:
                    self.h_alias[alias_name] = {}
                    self.h_alias[alias_name][node_id] = alias
        else:
            # this deletes the alias if it exists
            try:
                del self.h_alias[alias_name][node_id]
            except:
                pass

    def _save_alias_to_txt(self, path):
        """Saves alias data to txt with key in first
        column and each alias type in a separate column

        :param path: Absolute path to file
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        # TODO: Incorporate adams' refactor suggestions & test:
        #aliases = sorted(self.h_alias.keys()
        #headers = ['key'] + aliases
        #node_keys = nd_union(*[self.h_alias[a].keys() for a in aliases])

        headers = ['key']
        aliases = sorted(self.h_alias.keys())
        node_keys = set()
        for a in aliases:
            headers.append(a)
            node_keys.update(list(self.h_alias[a].keys()))

        final = []
        # Now create the comprehensive list
        # TODO, consider adams' refactor suggestion:
        #final = [[n] + [self.h_alias.get(a, {}).get(n) for a in aliases] for n in node_keys]
        #
        # From adams:
        #  You could also use plaidtools.functions.try_except instead of the get chain.
        #  Again, you don't have to do it this way.
        #
        for n in node_keys:
            temp = [n]
            for a in aliases:
                try:
                    temp.append(self.h_alias[a][n])
                except:
                    temp.append(None)
            final.append(temp)

        key = 'alias'
        row_max = len(final)

        if row_max > 0: #TODO do this w/o converting to a DF first, maybe?
            df = DataFrame(final, columns=headers, dtype=str).fillna('')
            df.to_csv(path, index=False, sep='|')

    def _load_alias_from_txt(self, path):
        """Loads saved alias txt file

        :param path: Absolute path to file
        :type path: str or unicode
        :returns: None
        :rtype: None
        """
        df = pd.read_csv(path)

        if df is not None:
            # Find the alias types which are stored as headers
            # Tuple is formed as (index, key, alias1, alias2, ...., aliasn)
            headers = list(df.columns)[1:]

            # Iterate over the data and build the hierachy using the add method
            # TODO:  Adams says:
            # It would be better to do this with enumerate, like:
            #  for col_pos, h in enumerate(headers, 2):
            for r in df.itertuples():
                col_pos = 2
                for h in headers:
                    if r[col_pos] is not None:
                        self.set_alias(r[1], r[col_pos], h)
                    col_pos += 1

    def clear_alias(self):
        """Clears all the alias information

        :returns: None
        :rtype: None
        """

        self.h_alias = {}

    def get_node_id_from_alias(self, alias, alias_name=None):
        """Finds the node_id using an alias

        :param alias: alias of the node
        :type alias: str or unicode
        :param alias_name: alias type
        :type alias_name: str or unicode
        :returns: node key
        :rtype: str or unicode
        """

        primary_node_id = None

        # This is an expensive operation so use sparingly
        # If given a hint of which alias type then use it
        if alias_name is not None:
            for node_id, alias_value in six.iteritems(self.h_alias[alias_name]):
                if alias_value == alias:
                    primary_node_id = node_id
                    break
        else:
            # Need to search through all alias types until we find it
            # ultra expensive operation
            # Get the primary node_id
            if alias in self.h_direct:
                primary_node_id = alias
            else:
                # Might be an alias
                for a in self.h_alias:
                    alias_list = self.h_alias[a]
                    for node_id, alias_value in six.iteritems(alias_list):
                        if alias_value == alias:
                            primary_node_id = node_id
                            break

        if primary_node_id is None:
            raise Exception("No node named %s was found either in primary node names or aliases." % alias)
        else:
            return primary_node_id

    def get_node_from_alias(self, alias, alias_name=None):
        """Finds the node object using the alias

        :param alias: alias of the node
        :type alias: str or unicode
        :param alias_name: alias type
        :type alias_name: str or unicode
        :returns: Node object
        :rtype: object
        """

        # This is an expensive operation so use sparingly
        node_id = self.get_node_id_from_alias(alias, alias_name)
        try:
            return self.get_node(node_id)
        except:
            # Must not be a main hierarchy node.
            alt_hiers = self.a_direct
            for ah in alt_hiers:
                try:
                    # exit here if we find a match
                    return self.get_alt_node(ah, node_id)
                except:
                    pass

            # no match could be found in any hierarchy
            raise Exception("No node object could be found in main or alternate hierarchies for node_id %s" % node_id)

    def property(self, node_id, property_name, inherit=False, alias_name=None):
        """Retrieves the specified property

        :param node_id: node key
        :type node_id: str or unicode
        :param property_name: property type
        :type property_name: str or unicode
        :param inherit: flag to indicate whether the property can be inherited from an ancestor
        :type inherit: bool
        :param alias_name: alias type
        :type alias_name: str or unicode
        :returns: property value
        :rtype: str or unicode
        """

        # Get the primary node_id
        # TODO is this actually needed? Why ask for a Node ID if you're just going
        # to replace it?
        # node_id = self.get_node_id_from_alias(node_id, alias_name=alias_name)

        try:
            return self.h_property[property_name][node_id]
        except:
            if inherit is False:
                return None
            else:
                # Now try to find an ancestor with the property set
                ancestors = self.get_ancestor_ids(node_id)

                # Go in order of youngest to oldest
                # Want to use the closest ancestor possible
                for a in ancestors:
                    prop = self.property(a, property_name, False)

                    if prop is not None:
                        return prop

    def set_property(self, node_id, property_value, property_name, force_string=False, alias_name=None):
        """Sets a node property

        :param node_id: node key
        :type node_id: str or unicode
        :param property_value: property value
        :type property_value: str or unicode
        :param property_name: property type
        :type property_name: str or unicode
        :param alias_name: alias type
        :type alias_name: str or unicode
        :returns: None
        :rtype: None
        """

        # Get the primary node_id
        # node_id = self.get_node_id_from_alias(node_id, alias_name=alias_name)

        if property_value is not None:

            if force_string is True:
                val = str(property_value)
            else:
                val = property_value

            if property_name not in self.h_property:
                # This property type doesn't exist yet
                self.h_property[property_name] = {}

            self.h_property[property_name][node_id] = val
        else:
            # this deletes the property if it exists
            try:
                del self.h_property[property_name][node_id]
            except:
                pass

    def get_node_ids_with_property(self, property_name, inherit=False):
        """Finds nodes with the specified property set with any value

        :param property_name: The property name
        :type property_name: str or unicode
        :param inherit: inherit property from ancestor if not set directly
        :type inherit: bool
        :returns: List of node keys with property set
        :rtype: list
        """

        final = []

        if inherit is False:
            # Simple case
            try:
                final = list(self.h_property[property_name].keys())
            except:
                pass
        else:
            # Complex and expensive case
            for n in self.h_direct:
                if self.property(n, property_name, True) is not None:
                    final.append(n)

        return final

    def get_node_ids_with_property_value(self, property_name, property_value, inherit=False):
        """Finds nodes with the specified property set with any value

        :param property_name: The property name
        :type property_name: str or unicode
        :param property_value: required property value
        :type property_value: str, unicode, or float
        :param inherit: inherit property from ancestor if not set directly
        :type property_value: bool
        :returns: List of node keys with the specified value
        :rtype: list
        """

        final = []
        if inherit is False:
            # Simple case
            try:
                nodes = list(self.h_property[property_name].keys())
            except:
                nodes = []

            for n in nodes:
                if self.h_property[property_name][n] == property_value:
                    final.append(n)
        else:
            # Complex and expensive case
            for n in self.h_direct:
                if self.property(n, property_name, True) == property_value:
                    final.append(n)

        return final

    def _save_property_to_txt(self, path):
        """aves property data to txt with key in first
        column and each property type in a separate column

        :param path: Absolute file path
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        headers = ['key']
        properties = sorted(self.h_property.keys())
        node_keys = set()
        for p in properties:
            headers.append(p)
            node_keys.update(list(self.h_property[p].keys()))

        final = []
        # Now create the comprehensive list
        for n in node_keys:
            temp = [n]
            for p in properties:
                try:
                    temp.append(json.dumps(self.h_property[p][n]))
                except:
                    temp.append(None)
            final.append(temp)

        key = 'property'
        row_max = len(final)

        if row_max > 0:
            df = DataFrame(final, columns=headers, dtype=str).fillna('')
            df.to_csv(path, index=False, sep="|")

    def _load_property_from_txt(self, path):
        """Loads the saved property information from txt file

        :param path: Absolute file path
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        df = pd.read_csv(path)

        if df is not None:
            # Find the property types which are stored as headers
            # Tuple is formed as (index, key, property1, property2, ...., propertyn)
            headers = list(df.columns)[1:]

            # Iterate over the data and build the hierachy using the add method
            # TODO: Incorporate @adams recommended changes.
            #   for cell, h in zip(r[2:], h):
            #   what goes in this line here then? val = json.loads(r[col_pos])
            #   more comfortable changing w/ a handy test case.
            for r in df.itertuples():
                col_pos = 2
                for h in headers:
                    if r[col_pos] is not None:
                        try:
                            val = json.loads(r[col_pos])
                        except:
                            val = None
                        self.set_property(r[1], val, h)
                    col_pos += 1

    def clear_property(self):
        """Clears all currently stored properties

        :returns: None
        :rtype: None
        """

        self.h_property = {}

    def get_properties(self):
        """Displays the list of current properties

        :returns: List of current properties
        :rtype: List
        """
        return list(self.h_property.keys())

    def _get_preprocessed_main_format(self, node_id, left=0, consolidation_list=[]):
        """Generates a highly optimized reporting format for export of main hierarchy

        :param node_id: current node key
        :type node_id: str or unicode
        :param left: current left counter
        :type left: int
        :param consolidation_list: list of consolidation multipliers as json string
        :type consolidation_list: list
        :returns: list of parent child records
        :rtype: list
        """

        final = []

        # If this recursed event doesn't have any records return the same value for left and right
        right = left

        children = self.get_children(node_id)
        for c in children:
            # Convert the string into a numerical constant
            if c.consolidation == '+':
                c_factor = 1
            elif c.consolidation == '-':
                c_factor = -1
            else:
                c_factor = 0

            # Add the multiplier on the stack
            child_consolidation_list = consolidation_list[:]
            child_consolidation_list.append(c_factor)

            # Get the child records recursively
            sub_right, sub_children = self._get_preprocessed_main_format(c.id, left + 1, child_consolidation_list)

            # Now figure out the right side number based on how many elements are below
            right = sub_right + 1
            if len(sub_children) > 0:
                is_leaf = False
            else:
                is_leaf = True

            temp = ['main', str(node_id), str(c.id), str(c.consolidation), is_leaf,
                    left, right, json.dumps(child_consolidation_list)]
            final.append(temp)
            if is_leaf is False:
                final += sub_children

        return right, final

    def _get_preprocessed_alt_format(self, alternate_hierarchy, node_id='root', left=0, consolidation_list=[]):
        """Generates a highly optimized reporting format for export of alt hierarchies

        :param alternate_hierarchy: alternate hierarchy name
        :type alternate_hierarchy: str or unicode
        :param node_id: current node key
        :type node_id: str or unicode
        :param left: current left counter
        :type left: int
        :param consolidation_list: list of consolidation multipliers as json string
        :type consolidation_list: list
        :returns: list of parent child records
        :rtype: list
        """
        final = []

        # If this recursed event doesn't have any records return the same value for left and right
        right = left

        children = self.get_alt_children(alternate_hierarchy, node_id)
        for c in children:
            # Get the consolidation type
            # If this is an alt node then the consolidation type will be specified in the alt area
            # If this is coming straight from the main then we need to grab it from there

            try:
                # Try the alt first
                con_type = c.alt_consolidation[alternate_hierarchy]
            except:
                # Must be a main hierarchy member only
                con_type = c.consolidation

            # Convert the string into a numerical constant
            if con_type == '+':
                c_factor = 1
            elif con_type == '-':
                c_factor = -1
            else:
                c_factor = 0

            # Add the multiplier on the stack
            child_consolidation_list = consolidation_list[:]
            child_consolidation_list.append(c_factor)

            # Get the child records recursively
            sub_right, sub_children = self._get_preprocessed_alt_format(
                alternate_hierarchy, c.id, left + 1, child_consolidation_list)

            # Now figure out the right side number based on how many elements are below
            right = sub_right + 1
            if len(sub_children) > 0:
                is_leaf = False
            else:
                is_leaf = True

            temp = [alternate_hierarchy, str(node_id), str(c.id), str(c.consolidation),
                    is_leaf, left, right, json.dumps(child_consolidation_list)]
            final.append(temp)
            if is_leaf is False:
                final += sub_children

        return right, final

    def _compress_frame(self, path):
        """Performs a packing operation on the HDF5 file

        :param path: Absolute file path to HDF5 file
        :type path: str or unicode
        :returns: None
        :rtype: None
        """

        # Only run this on a posix machine
        if os.name == 'posix':
            try:
                compressed_path = path + '.comp'

                subprocess.call(
                    [
                        "ptrepack",
                        "-o",
                        "--chunkshape=auto",
                        "--propindexes",
                        "--complevel=9",
                        "--complib=blosc",
                        "--overwrite-nodes",
                        path,
                        compressed_path
                    ], stderr=sys.stdout
                )
            except:
                # this appears to fail on windows currently
                # https://github.com/PyTables/PyTables/issues/133
                pass
            else:
                # Looks like windows may fail to compress the file but not error
                # Check to make sure the compressed file exists first
                # If the compressed version isn't there just leave the uncompressed one
                if os.path.exists(compressed_path) is True:
                    os.unlink(path)
                    os.rename(compressed_path, path)

    def save_preprocessed(self, path, top_node='root'):
        """Generates a highly optimized reporting format for export

        :param path: Absolute path to export location
        :type path: str or unicode
        :param top_node: node key to start export at
        :type top_node: str or unicode
        :returns: None
        :rtype: None
        """

        headers = ['hierarchy', 'parent', 'child', 'consolidation_type', 'leaf', 'left', 'right', 'consolidation']
        key = 'table'
        right, pc_list = self._get_preprocessed_main_format(top_node)

        alts = self.get_alt_hierarchies()
        for a in alts:
            right, pc_list_alt = self._get_preprocessed_alt_format(a, 'root', right + 1)

            if len(pc_list_alt) > 0:
                pc_list += pc_list_alt

        row_max = len(pc_list)

        df = DataFrame(pc_list, columns=headers)
        store = HDFStore(path)
        store.put(key, df, format='fixed')
        store.close()
        self._compress_frame(path)

    def make_pretty_dimension(self, use_alias=None, show_both=True, top_node='root'):
        # Open the text file for writing
        indent = '  '
        template = '{0}{1} {2} ({3}) ({4})'

        buf = StringIO()

        buf.write('**** MAIN HIERARCHY ****\n')
        buf.write('\n')

        # Main Hierarchy Representation
        right, pc_list = self._get_preprocessed_main_format(top_node)

        for p in pc_list:
            consolidation = p[3]
            if use_alias is not None:
                if show_both is True:
                    node_id = ' | '.join((p[2], self.alias(p[2], use_alias)))
                else:
                    node_id = self.alias(p[2], use_alias)
            else:
                node_id = p[2]

            effective_consolidation = json.loads(p[7])
            effective_consolidation_txt = str(effective_consolidation).strip('[]')
            indent_txt = indent * len(effective_consolidation)

            txt = template.format(indent_txt, consolidation, node_id, len(
                effective_consolidation), effective_consolidation_txt)
            buf.write('{0}\n'.format(txt))

        # Alternate Hierarchy Representation
        buf.write('\n')
        buf.write('\n')
        buf.write('**** ALTERNATE HIERARCHIES ****\n')
        alts = self.get_alt_hierarchies()

        if len(alts) == 0:
            buf.write('\n')
            buf.write('No Alternate Hierarchies Found\n')

        for a in alts:
            buf.write('\n')
            buf.write('\n')
            buf.write('------ {0} ------\n'.format(a))
            right, pc_list_alt = self._get_preprocessed_alt_format(a, 'root', right + 1)

            for p in pc_list_alt:
                consolidation = p[3]
                if use_alias is not None:
                    if show_both is True:
                        node_id = ' | '.join((p[2], self.alias(p[2], use_alias)))
                    else:
                        node_id = self.alias(p[2], use_alias)
                else:
                    node_id = p[2]

                effective_consolidation = json.loads(p[7])
                effective_consolidation_txt = str(effective_consolidation).strip('[]')
                indent_txt = indent * len(effective_consolidation)

                txt = template.format(indent_txt, consolidation, node_id, len(
                    effective_consolidation), effective_consolidation_txt)
                buf.write('{0}\n'.format(txt))

            return buf

    def save_pretty_hierarchy(self, path, use_alias=None, show_both=True, top_node='root'):
        # Open the text file for writing
        indent = '  '
        template = '{0}{1} {2} ({3}) ({4})'

        with open(path, 'w') as f:
            f.write('**** MAIN HIERARCHY ****\n')
            f.write('\n')

            # Main Hierarchy Representation
            right, pc_list = self._get_preprocessed_main_format(top_node)

            for p in pc_list:
                consolidation = p[3]
                if use_alias is not None:
                    if show_both is True:
                        node_id = ' | '.join((p[2], self.alias(p[2], use_alias)))
                    else:
                        node_id = self.alias(p[2], use_alias)
                else:
                    node_id = p[2]

                effective_consolidation = json.loads(p[7])
                effective_consolidation_txt = str(effective_consolidation).strip('[]')
                indent_txt = indent * len(effective_consolidation)

                txt = template.format(indent_txt, consolidation, node_id, len(
                    effective_consolidation), effective_consolidation_txt)
                f.write('{0}\n'.format(txt))

            # Alternate Hierarchy Representation
            f.write('\n')
            f.write('\n')
            f.write('**** ALTERNATE HIERARCHIES ****\n')
            alts = self.get_alt_hierarchies()

            if len(alts) == 0:
                f.write('\n')
                f.write('No Alternate Hierarchies Found\n')

            for a in alts:
                f.write('\n')
                f.write('\n')
                f.write('------ {0} ------\n'.format(a))
                right, pc_list_alt = self._get_preprocessed_alt_format(a, 'root', right + 1)

                for p in pc_list_alt:
                    consolidation = p[3]
                    if use_alias is not None:
                        if show_both is True:
                            node_id = ' | '.join((p[2], self.alias(p[2], use_alias)))
                        else:
                            node_id = self.alias(p[2], use_alias)
                    else:
                        node_id = p[2]

                    effective_consolidation = json.loads(p[7])
                    effective_consolidation_txt = str(effective_consolidation).strip('[]')
                    indent_txt = indent * len(effective_consolidation)

                    txt = template.format(indent_txt, consolidation, node_id, len(
                        effective_consolidation), effective_consolidation_txt)
                    f.write('{0}\n'.format(txt))

    def get_all_nodes_as_list(self, use_alias=None, show_both=False, top_node='root'):
        """Generates a list of dicts; 1 dict per hierarchy node

        :param use_alias: Nodes can have mulitiple aliases. Specify which one to use
        :type use_alias: str or unicode
        :param show_both: Show id along with alias
        :type show_both: bool
        :param top_node: Top node of hierarchy
        :type top_node: str or unicode
        :returns: List of nodes
        :rtype: list
        """

        right, pc_list = self._get_preprocessed_main_format(top_node)

        node_list = []

        for p in pc_list:
            consolidation = p[3]
            if use_alias is not None:
                if show_both is True:
                    node_id = ' | '.join((p[2], self.alias(p[2], use_alias)))
                else:
                    node_id = self.alias(p[2], use_alias)
            else:
                node_id = p[2]

            effective_consolidation = json.loads(p[7])

            node_dict = {
                'Consolidation Type': consolidation,
                'Generation': len(effective_consolidation),
                'Node ID': node_id
            }
            node_list.append(node_dict)

        return node_list

    def get_node_count(self):
        """Provides number of main hierarchy nodes

        :returns: Node count
        :rtype: int
        """

        return len(self.h_direct)


