#!/usr/bin/env python
# coding=utf-8
"""
A highly optimized class for fast dimensional hierarchy operations
"""

import os
import io
import uuid
import pandas as pd
import numpy as np

__author__ = 'Dave Parsons'
__copyright__ = 'Copyright 2010-2022, Tartan Solutions, Inc'
__credits__ = ['Dave Parsons']
__license__ = 'Apache 2.0'
__maintainer__ = 'Dave Parsons'
__email__ = 'dave.parsons@tartansolutions.com'

# These must be synced with plaid/app/analyze/utility/dimension.py
ROOT = '!!root!!'
MAIN = 'main'
DEFAULT = '!!default!!'
VALID_CONSOL = ['~', '+', '-', '|', '&']
USE_DATAFRAME_LOADING = os.environ.get("USE_DATAFRAME_LOADING", "true") == "true"

def validate_uuid4(uuid_string):
    # Check validity of UUID V4 string
    try:
        uuid.UUID(uuid_string, version=4)
    except ValueError:
        # is not a valid hex code for a UUID.
        return False
    return True


class Dimensions:
    """
    Dimensions Class for retrieves all dimensions for a specific key (model) in Redis
    """

    # noinspection PyUnresolvedReferences,PyCompatibility,PyTypeChecker
    def __init__(self, conn):
        """__init__(self, auth_id, key)
        Class init function sets up basic structure

        Args:
            conn (Connection): plaidtools connection object

        Returns:
            Dimensions: new Dimensions object
        """
        self.conn = conn
        self.project_id = conn.project_id
        self.dims = self.conn.rpc.analyze.dimension
        self.ROOT = ROOT
        self.MAIN = MAIN

    # --------------------------------------------------------------------------------------------------
    # ==== DIMENSIONS METHODS ==========================================================================
    # --------------------------------------------------------------------------------------------------
    def add_dimension(self, name, path='/'):
        """add_dimension(name)
        Creates a new dimension

        Args:
            name (str): Dimension key
            path (str): Path to dimension object

        Returns:
            dimensions (dim): Dimension object
        """
        self.dims.create(project_id=self.project_id, path=path, name=name, memo='')
        dim = Dimension(conn=self.conn, name=name)
        return dim

    def copy_dimension(self, src, dest, dest_project_id=None):
        """copy_dimension(src, dest, dest_project_id=None))
        Copies a dimension

        Args:
            src (str): Current dimension unique ID
            dest (str): New dimension unique ID
            dest_project_id (str): Analyze project_id used as key to saved hierarchy in Redis

        Returns:
            None
        """
        # TODO: @DAVE - needs additional details passed to RPC
        self.dims.copy_dimension(project_id=self.project_id, src=src, dest=dest, dest_project_id=dest_project_id)

    def delete_dimension(self, name):
        """delete_dimension(name)
        Deletes a dimension

        Args:
            name (str): Dimension unique ID

        Returns:
            None
        """
        self.dims.delete(project_id=self.project_id, dimension_id=name)

    def get_dimension(self, name, replace=False):
        """get_dimension(name, replace=False)
        Gets or create a dimension

        Args:
            name (str): Unique name for hierarchy dimension
            replace (bool): Flag to replace current dimension with new one
        Returns:
            dim: Dimension object
        """
        if '/' in name:
            # This is a path, split into name and path
            path = '/'.join(name.split('/')[:-1])
            name = name.split('/')[-1]
        else:
            # This must be a name only.
            path = '/'
        # Recreate the dimension if replace is true
        if self.dims.is_dimension(project_id=self.project_id, name=name):
            dim = Dimension(conn=self.conn, name=name)
            if replace:
                dim.clear()
        else:
            dim = self.add_dimension(name=name, path=path)
        return dim

    def get_dimensions(self):
        """get_dimensions()
        Gets dimensions

        Args:

        Returns:
            dict: result dict of dicts keyed by unique dimension ID with the following properties
                - name (str): Dimension Name
                - dim (Dimension: Dimension object
        """
        gst_dims = self.dims.dimensions(project_id=self.project_id, id_filter=None, sort=None,
                                        keys=None, member_details=False)

        dimensions = {}
        for gst_dim in gst_dims:
            dimensions[gst_dim['id']] = [gst_dim['name'], Dimension(conn=self.conn, name=gst_dim['name'])]
        return dimensions

    def is_dimension(self, name):
        """is_dimension(name)
        Checks that a dimension exists

        Args:
            name (str): Unique name for hierarchy dimension

        Returns:
            bool: Does the dimension exist
        """
        return self.dims.is_dimension(project_id=self.project_id, name=name)

    def rename_dimension(self, old, new):
        """rename_dimension(old, new)
        Renames a dimension

        Args:
            old (str): Current dimension unique ID
            new (str): New dimension unique ID

        Returns:
            None
        """
        # TODO: @Dave - Needs calling update function
        self.dims.rename_dimension(project_id=self.project_id, old=old, new=new)
        
    def get_dimension_names(self):
        """get_dimension_names(name)
        Returns a list of all dimension names

        Args:

        Returns:
            list: names of all dimensions in the project
        """
        dims = self.get_dimensions()
        return [dims[i][0] for i in dims]
    
    def get_dimension_objects(self):
        """get_dimension_names(name)
        Returns a list of all dimension objects

        Args:

        Returns:
            list: all dimensions in the project
        """
        dims = self.get_dimensions()
        return [dims[i][1] for i in dims]

    # --------------------------------------------------------------------------------------------------
    # ==== MAPPING METHODS =============================================================================
    # --------------------------------------------------------------------------------------------------
    def add_mapping(self, name, table, column):
        """add_mapping(name, table, column)
        Adds a new table column mapping

        Args:
            name (str): Dimension unique ID
            table (str): Table name
            column (str): Column name

        Returns:
            None
        """
        self.dims.add_mapping(project_id=self.project_id, name=name, table=table, column=column)

    def delete_mapping(self, table, column):
        """delete_mapping(table, column)
        Deletes a table column mapping

        Args:
            table (str): Table name
            column (str): Column name

        Returns:
            None
        """
        self.dims.delete_mapping(project_id=self.project_id, table=table, column=column)

    def get_dimension_tables(self, name):
        """get_dimension_tables(name)
        Return tables using the dimension in a column mapping

        Args:
            name (str): Dimension unique ID

        Returns:
            dict: Dict of tables & columns
        """
        return self.dims.get_dimension_tables(project_id=self.project_id, name=name)

    def get_table_dimensions(self, table):
        """get_table_dimensions(table)
        Return dimensions ussed by a table in column mappings

        Args:
            table (str): Table name

        Returns:
            dict: Dict of columns to dimensions
        """
        return self.dims.get_table_dimensions(project_id=self.project_id, table=table)

    # --------------------------------------------------------------------------------------------------
    # ==== IMPORT/EXPORT METHODS =======================================================================
    # --------------------------------------------------------------------------------------------------
    def backup(self, name):
        """backup(name)
        Backup all nodes and hierarchies in dimension

        Args:
            name (str): Dimension unique ID

        Returns:
            yaml (str): Dimension persisted in YAML format
        """
        data = self.dims.backup(project_id=self.project_id, name=name)
        return data

    def backup_all(self):
        """backup_all()
        Backup all dimensions in project

        Args:

        Returns:
            yaml (str): Dimensions persisted in YAML format
        """
        data = self.dims.backup_all(project_id=self.project_id)
        return data

    def restore(self, data):
        """restore(data)
        Restore dimension in project

        Args:
            data (str): Dimension persisted in YAML format

        Returns:
            None
        """
        # Load the YAML into dict
        self.dims.restore(project_id=self.project_id, data=data)

    def restore_all(self, data):
        """restore_all(data)
        Restore all dimensions in project

        Args:
            data (str): Dimensions persisted in YAML format

        Returns:
            None
        """
        self.dims.restore_all(project_id=self.project_id, data=data)


class Dimension:
    """
    Dimension Class for fast hierarchy, alias, property & attribute operations
    Dimensions contain nodes (members) arranged into one or more hierarchies.
    Each node can possess things like aliases, and methods to manage traversal.
    """

    def __init__(self, conn, name, clear=False):
        """conn, name, clear=False)
        Class init function sets up basic structure

        Args:
            conn (Connection): plaidtools connection object
            name (str): Unique name for hierarchy dimension
            clear (bool): Clear the Dimension's existing data
        Returns:
            Dimension: new Dimension object
        """

        self.ROOT = ROOT
        self.MAIN = MAIN
        self.conn = conn
        self.dim = self.conn.rpc.analyze.dimension
        self.project_id = conn.project_id
        if validate_uuid4(name):
            self.id = name
            self.name = self.dim.dimension(project_id=self.project_id, dimension_id=self.id, keys=['name'])['name']
        else:
            self.name = name
            self.id = self.dim.lookup_by_name(project_id=self.project_id, name=self.name)
        if clear is True:
            self.clear()

    def __getattr__(self, item):
        """Hopefully a catch-all for RPCs that may be added to the RPC methods in plaid, but not yet implemented here
            Just calls directly through
        """
        def rpc_wrapper(**kwargs):
            getattr(self.dim, item)(project_id=self.project_id, name=self.name, **kwargs)
        return rpc_wrapper

    # --------------------------------------------------------------------------------------------------
    # ==== DIMENSION METHODS ===========================================================================
    # --------------------------------------------------------------------------------------------------
    def reload(self):
        """reload()
        Load nodes and hierarchies

        Args:

        Returns:
            None
        """
        self.dim.reload(project_id=self.project_id, name=self.name)

    def clear(self):
        """clear()
        Clears the main and alternate hierarchies

        Args:

        Returns:
            None
        """
        self.dim.clear(project_id=self.project_id, name=self.name)

    # --------------------------------------------------------------------------------------------------
    # ==== HIERARCHY METHODS ===========================================================================
    # --------------------------------------------------------------------------------------------------
    def get_alt_hierarchies(self):
        """get_alt_hierarchies()
        Returns current alt hierarchies

        Args:

        Returns:
            list: List of alternate hierarchies
        """
        return self.dim.get_alt_hierarchies(project_id=self.project_id, name=self.name)

    def add_alt_hierarchy(self, hierarchy):
        """add_alt_hierarchy(hierarchy)
        Creates a new alt hierarchy

        Args:
            hierarchy (str): Alternate hierarchy key

        Returns:
            None
        """
        self.dim.add_alt_hierarchy(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def clear_alt_hierarchy(self, hierarchy):
        """clear_alt_hierarchy(hierarchy)
        Clears an alt hierarchy

        Args:
            hierarchy (str): Alternate hierarchy unique ID

        Returns:
            None
        """
        self.dim.clear_alt_hierarchy(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def delete_alt_hierarchy(self, hierarchy):
        """delete_alt_hierarchy(hierarchy)
        Deletes an alt hierarchy

        Args:
            hierarchy (str): Alternate hierarchy unique ID

        Returns:
            None
        """
        self.dim.delete_alt_hierarchy(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def is_hierarchy(self, hierarchy):
        """is_hierarchy(hierarchy)
        Checks hierarchy exists

        Args:
            hierarchy (str): Alternate hierarchy unique ID

        Returns:
            bool: Does the hierarchy exist
        """
        return self.dim.is_hierarchy(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def rename_alt_hierarchy(self, old, new):
        """rename_alt_hierarchy(old, new)
        Renames an alt hierarchy

        Args:
            old (str): Current alternate hierarchy unique ID
            new (str): New alternate hierarchy unique ID

        Returns:
            None
        """
        self.dim.rename_alt_hierarchy(project_id=self.project_id, name=self.name, old=old, new=new)

    def sort_alt_hierarchy(self, hierarchy, ordering=None, alpha=True):
        """sort_alt_hierarchy(hierarchy, ordering=None, alpha=True)
        Sort an alt hierarchy

        Args:
            hierarchy (str): Alternate hierarchy unique ID
            ordering (str): DESC/desc to sort in descending order
            alpha (bool): True = sort alphanumerically (default)
                          False = sort numerically

        Returns:
            None
        """
        self.dim.sort_alt_hierarchy(project_id=self.project_id, name=self.name, hierarchy=hierarchy, ordering=ordering, alpha=alpha)

    # --------------------------------------------------------------------------------------------------
    # ==== NODE METHODS ================================================================================
    # --------------------------------------------------------------------------------------------------
    def add_node(self, parent, child, consolidation='+', hierarchy=MAIN, before=None, after=None):
        """add_node(project_id, name, parent, child, consolidation='+', hierarchy=MAIN, before=None, after=None)
        Adds an existing main hierarchy node to the specified hierarchy both leaves and folders can be
        added to the hierarchies

        Args:
            parent (str): parent node key
            child (str): child node key
            consolidation (str): Consolidation Type (+, -, or ~)
            hierarchy (str): Hierarchy unique ID
            before (str): node to insert before
            after (str): node to insert after

        Returns:
            tuple: parent_added (bool), node_added (bool), node_moved (bool)
        """
        return self.dim.add_node(
            project_id=self.project_id, name=self.name, parent=parent, child=child,
            consolidation=consolidation, hierarchy=hierarchy, before=before, after=after,
        )

    def add_nodes(self, parent, children, consolidation='+', hierarchy=MAIN, before=None, after=None):
        """add_nodes(pparent, children, consolidation='+', hierarchy=MAIN, before=None, after=None)
        Adds an existing main hierarchy nodes to the specified hierarchy both leaves and folders can be
        added to the hierarchies

        Args:
            parent (str): parent node key
            children (list): child node keys
            consolidation (str): Consolidation Type (+, -, or ~)
            hierarchy (str): Hierarchy unique ID
            before (str): node to insert before
            after (str): node to insert after

        Returns:
            None
        """
        self.dim.add_nodes(project_id=self.project_id, name=self.name, parent=parent, children=children,
                           consolidation=consolidation, hierarchy=hierarchy, before=before, after=after)

    def clone_node(self, parent, child, hierarchy=MAIN):
        """Clones a node, copying all properties, attributes etc

        Args:
            parent (str): parent node key
            child (str): child node key
            hierarchy:

        Returns:

        """
        self.dim.clone_node(project_id=self.project_id, name=self.name, parent=parent,
                            child=child,  hierarchy=hierarchy)

    def delete_node(self, parent, child, hierarchy=MAIN):
        """delete_node(project_id, name, parent, child, hierarchy=MAIN)
        Deletes the node and removes all aliases and properties

        Args:
            parent (str): parent node key
            child (str): child node key
            hierarchy (str): Hierarchy unique ID

        Returns:
            None
        """
        try:
            self.dim.delete_node(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)
        except:
            pass

    def delete_nodes(self, parent, children, hierarchy=MAIN):
        """delete_nodes(project_id, name, parent, child, hierarchy=MAIN)
        Deletes the nodes and removes all aliases and properties

        Args:
            parent (str): parent node key
            children (str): child node key
            hierarchy (str): Hierarchy unique ID

        Returns:
            None
        """
        self.dim.delete_nodes(project_id=self.project_id, name=self.name, parent=parent, children=children, hierarchy=hierarchy)

    def move_node(self, child, new_parent, hierarchy=MAIN, before=None, after=None):
        """move_node(parent, child, consolidation='+', hierarchy=MAIN, before=None, after=None)
        Moves an existing node within the specified hierarchy both leaves
        Args:
            child (str): child node key
            new_parent (str): new parent node
            hierarchy (str): Hierarchy unique ID
            before (str): node to insert before
            after (str): node to insert after

        Returns:
            str: New parent of node
        """
        self.dim.move_node(project_id=self.project_id, name=self.name, child=child,
                           new_parent=new_parent, hierarchy=hierarchy, before=before, after=after)

    def move_nodes(self, moves, new_parent, hierarchy='main', before=None, after=None):
        """move_nodes(project_id, name, moves, hierarchy='main', before=None, after=None)
        Moves a set of hierarchy nodes to the specified hierarchy both leaves and folders can be
        added to the hierarchies

        Args:
            moves (list of dict): list of moves, containing 'parent' and 'child'
            new_parent (str): new parent node key
            hierarchy (str): Hierarchy unique ID
            before (str): node to insert before
            after (str): node to insert after

        Returns:
            str: New parent of node
        """
        self.dim.move_nodes(project_id=self.project_id, name=self.name, moves=moves, new_parent=new_parent,
                            hierarchy=hierarchy, before=before, after=after)

    def rename_node(self, old, new, force=False):
        """rename_node(old, new, force)
        Renames the node

        Args:
            old (str): Current node unique ID
            new (str): New node unique ID
            force (bool): Forces the node rename by renaming the current node out of the way

        Returns:
            None
        """
        self.dim.rename_node(project_id=self.project_id, name=self.name, old=old, new=new, force=force)

    def reorder_nodes(self, ancestor, children, hierarchy=MAIN):
        """reorder_nodes(self, ancestor, children, hierarchy=MAIN)
        Reorders the nodes under the ancestor

        Args:
            ancestor (str): Current node unique ID
            children (list): Node ids in new order
            hierarchy (str): Hierarchy unique ID

        Returns:
            None
        """
        self.dim.reorder_nodes(project_id=self.project_id, name=self.name,
                               ancestor=ancestor, children=children, hierarchy=hierarchy)

    def get_all_leaves(self, hierarchy=MAIN):
        """get_all_leaves(hierarchy=MAIN)
        Returns all leaf nodes in hierarchy
        Args:
            hierarchy (str): Hierarchy unique ID

        Returns:
            set: Set of all node names
        """
        return self.dim.get_all_leaves(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def get_all_nodes(self, hierarchy=None):
        """get_all_nodes()
        Returns all nodes used in dimension or hierarchy
        Args:
            hierarchy (str): Hierarchy unique ID or None returns all dim nodes

        Returns:
            set: Set of all node names
        """
        return self.dim.get_all_nodes(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def get_all_parents(self, hierarchy=MAIN):
        """get_all_parents(hierarchy=MAIN)
        Returns all parent nodes in hierarchy
        Args:
            hierarchy (str): Hierarchy unique ID

        Returns:
            set: Set of all node names
        """
        return self.dim.get_all_parents(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def get_node_details(self, node, hierarchy='main'):
        """get_node_details(node, hierarchy=MAIN)
        Returns detailed information about a node
        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID
        Returns:
            dict: Dict with child node unique identifiers/consolidations/leaf/aliases etc.
        """
        return self.dim.get_node_details(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    # --------------------------------------------------------------------------------------------------
    # ==== SHIFT METHODS ===============================================================================
    # --------------------------------------------------------------------------------------------------
    def shift_node_right(self, parent, child, hierarchy=MAIN):
        """shift_node_right(parent, child, hierarchy=MAIN)
        Move the node one generation down, looking upwards in the hierarchy for a suitable parent (if there is one)
        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Alt Hierarchy Key
        Returns:
            new parent (str): New parent node or None if cannot be moved
        """
        return self.dim.shift_node_right(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def shift_node_left(self, parent, child, hierarchy=MAIN):
        """shift_node_left(parent, child, hierarchy=MAIN)
        Move the node one generation up (if possible)
        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Alt Hierarchy Key
        Returns:
            new parent (str): New parent node or None if cannot be moved
        """
        return self.dim.shift_node_left(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def shift_node_up(self, parent, child, hierarchy=MAIN):
        """shift_node_up(parent, child, hierarchy=MAIN)
        Move the node above prior node within the same generation
        If top within parent, it moves to the next parent above at the same generation as it's parent (if there is one)
        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Alt Hierarchy Key
        Returns:
            new parent (str): New parent node or None if cannot be moved
        """
        return self.dim.shift_node_up(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def shift_node_down(self, parent, child, hierarchy=MAIN):
        """shift_node_down(parent, child, hierarchy=MAIN)
        Move the node below following node within the same generation
        If bottom within parent, it moves to the next parent below at the same generation as it's parent (if there is one)
        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Alt Hierarchy Key
        Returns:
            new parent (str): New parent node or None if cannot be moved
        """
        return self.dim.shift_node_down(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    # --------------------------------------------------------------------------------------------------
    # ==== NAVIGATION METHODS ==========================================================================
    # --------------------------------------------------------------------------------------------------
    def get_ancestor_at_generation(self, node, generation, hierarchy=MAIN):
        """get_ancestor_at_generation(node, generation, hierarchy=MAIN)
        Traverses up the hierarchy to find the specified ancestor

        Args:
            node (str): Unique hierarchy node identifier
            generation (int): Number of generations to traverse for ancestor
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Ancestor node unique identifier
        """
        return self.dim.get_ancestor_at_generation(project_id=self.project_id, name=self.name, node=node, generation=generation, hierarchy=hierarchy)

    def get_ancestor_at_level(self, node, level, hierarchy=MAIN):
        """get_ancestor_at_level(node, level, hierarchy=MAIN)
        Traverses up the hierarchy to find the specified ancestor

        Args:
            node (str): Unique hierarchy node identifier
            level (int): Number of levels to go back for ancestor
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Ancestor node unique identifier
        """
        return self.dim.get_ancestor_at_level(project_id=self.project_id, name=self.name, node=node, level=level, hierarchy=hierarchy)

    def get_ancestors(self, node, hierarchy=MAIN):
        """get_ancestors(node, hierarchy=MAIN)
        Returns an ordered list of the node lineage objects

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of lists (level, node)
        """
        return self.dim.get_ancestors(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_bottom(self, node, hierarchy=MAIN):
        """get_bottom(node, hierarchy=MAIN)
        Returns the bottom node of the children in the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Unique node identifier
        """
        return self.dim.get_bottom(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_children(self, node, hierarchy=MAIN):
        """get_children(node, hierarchy=MAIN)
        Finds the children of the node within the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of child node unique identifiers
        """
        return self.dim.get_children(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_children_with_details(self, node, hierarchy=MAIN):
        """get_children_with_details(node, hierarchy=MAIN)
        Finds the children of the node within the specified hierarchy and returns additonal details

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of dicts with child node unique identifiers/consolidations/leaf
        """
        return self.dim.get_children_with_details(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_children_count(self, node, hierarchy=MAIN):
        """get_children_count(node, hierarchy=MAIN)
        Finds number of children of the node within the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            int: Count of child node unique identifiers
        """
        return self.dim.get_children_count(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_descendents(self, node, hierarchy=MAIN):
        """get_descendents(node, hierarchy=MAIN)
        Finds all descendants of the node

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of descendent node unique identifiers
        """
        return self.dim.get_descendents(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_descendents_at_generation(self, node, generation, hierarchy=MAIN):
        """get_descendents_at_generation(node, generation, hierarchy=MAIN)
        Finds all node types of a branch at the specified level

        Args:
            node (str): Unique hierarchy node identifier
            generation (int): Number of generations to descend for leaves
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of node unique identifiers at the specified level
        """
        return self.dim.get_descendents_at_generation(project_id=self.project_id, name=self.name, node=node,
                                                      generation=generation, hierarchy=hierarchy)

    def get_difference(self, hierarchies):
        """get_difference(hierarchies)
        Difference of nodes between main and alternate hierarchies

        Args:
            hierarchies (list): list of alternate hierarchies to use

        Returns:
            list: Difference of all nodes across hierarchies
        """
        return self.dim.get_difference(project_id=self.project_id, name=self.name, hierarchies=hierarchies)

    def get_down(self, parent, child, hierarchy=MAIN):
        """get_down(parent, child, hierarchy=MAIN)
        Returns the next node of the children in the specified hierarchy

        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Unique node identifier
        """
        return self.dim.get_down(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def get_generation(self, node, hierarchy=MAIN):
        """get_generation(node, hierarchy=MAIN)
        Returns the generation of the node in the main hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            int: Generation of node
        """
        return self.dim.get_generation(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_grandparent(self, node, hierarchy=MAIN):
        """get_grandparent(node, hierarchy=MAIN)
        Returns the grandparent of the node within the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Grandparent node
        """
        return self.dim.get_grandparent(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_hierarchy(self, node=ROOT, hierarchy=MAIN, alias=None, generation=None, leaf_only=False):
        """get_hierarchy(hierarchy=MAIN, alias=None, generation=None, leaf_only=False)
        Returns the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID
            alias (str): Alias unique ID
            generation (int): Generation to descend to or None for all
            leaf_only (bool): Only return leaf nodes no parents

        Returns:
            dict: Hierarchy from specified node with node details
        """
        return self.dim.get_hierarchy(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy,
                                      alias=alias, generation=generation, leaf_only=leaf_only)

    def get_intersection(self, hierarchies):
        """get_intersection(hierarchies)
        Intersection of nodes between main and alternate hierarchies

        Args:
            hierarchies (list): list of alternate hierarchies to use

        Returns:
            list: Intersection of all nodes across hierarchies
        """
        return self.dim.get_intersection(project_id=self.project_id, name=self.name, hierarchies=hierarchies)

    def get_leaves(self, node, hierarchy=MAIN):
        """get_leaves(node, hierarchy=MAIN)
        Finds the leaves below a node within a specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of leaf level node objects
        """
        return self.dim.get_leaves(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_leaves_at_generation(self, node, generation, hierarchy=MAIN):
        """get_leaves_at_generation(node, generation, hierarchy=MAIN)
        Finds leaves of a branch at the specified level

        Args:
            node (str): Unique hierarchy node identifier
            generation (int): Number of generations to descend for leaves
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of leaf level node objects
        """
        return self.dim.get_leaves_at_generation(project_id=self.project_id, name=self.name, node=node, generation=generation, hierarchy=hierarchy)

    def get_leaves_at_level(self, node, level, hierarchy=MAIN):
        """get_leaves_at_level(node, level, hierarchy=MAIN)
        Finds leaves of a branch at the specified level

        Args:
            node (str): Unique hierarchy node identifier
            level (int): Number of levels to ascend for leaves
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of leaf level node objects
        """
        return self.dim.get_leaves_at_level(project_id=self.project_id, name=self.name, node=node, level=level, hierarchy=hierarchy)

    def get_leaves_for_nodes(self, nodes, hierarchy=MAIN):
        """Gets the leaves associated with each of the nodes

        Args:
            nodes (list): List of nodes for which to get leaves
            hierarchy (str): The hierarchy in which to look

        Returns:
            dict with nodes as keys, values as list of leaves
        """
        return self.dim.get_leaves_for_nodes(project_id=self.project_id, name=self.name, nodes=nodes, hierarchy=hierarchy)

    def get_node_count(self, hierarchy=MAIN):
        """get_node_count(hierarchy=MAIN)
        Provides number of hierarchy nodes

        Args:
            hierarchy (str): Hierarchy unique ID

        Returns:
            int: Node count
        """
        return self.dim.get_node_count(project_id=self.project_id, name=self.name, hierarchy=hierarchy)

    def get_parent(self, node, hierarchy=MAIN):
        """get_parent(node, hierarchy=MAIN)
        Gets the node's parent within the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Parent node
        """
        return self.dim.get_parent(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_parents(self, node):
        """get_parents(node)
        Finds all the hierarchy parents of the node

        Args:
            node (str): Unique hierarchy node identifier

        Returns:
            list: Tuple of hierarchy and parent
        """
        return self.dim.get_parents(project_id=self.project_id, name=self.name, node=node)

    def get_leaf_position(self, origin_node, target_node, hierarchy=MAIN):
        """get_leaf_position(origin_node, target_node, hierarchy=MAIN)
        Gets the leaf position of the target_node relative to the origin_node, as if
        in an ordered list of all leaves. Will be negative if the target_node is
        before the origin_node, positive if the target_node is after the origin_node.

        Args:
            origin_node (str): Unique hierachy node identifier for the node to measure from
            target_node (str): Unique hierarchy node identifier for the node to measure to
            hierarchy (str): Hierarchy unique ID

        Returns:
            int: the leaf position of the target node relative to the origin node
        """
        return self.dim.get_leaf_position(
            project_id=self.project_id,
            name=self.name,
            origin_node=origin_node,
            target_node=target_node,
            hierarchy=hierarchy,
        )

    def get_node_by_leaf_position(self, origin_node, leaf_position, hierarchy=MAIN):
        """get_node_by_leaf_position(origin_node, leaf_position, hierarchy=MAIN)
        Gets the node's parent within the specified hierarchy

        Args:
            origin_node (str): Unique hierarchy identifier for the node to measure from
            leaf_position (str): the relative position from the origin_node to look for a result node
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: The leaf node that is N steps away from the origin_node, when
                 considering all leaf nodes in order, where N is leaf_position. Negative
                 leaf_position means the result will be earlier in that order than
                 the origin_node, while positive leaf_position means the result will
                 be later in that order than the origin_node.
        """
        return self.dim.get_node_by_leaf_position(
            project_id=self.project_id,
            name=self.name,
            origin_node=origin_node,
            leaf_position=leaf_position,
            hierarchy=hierarchy,
        )

    def get_siblings(self, node, hierarchy=MAIN):
        """get_siblings(node, hierarchy=MAIN)
        Finds the siblings of the node within the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            list: List of sibling node objects including current node
        """
        return self.dim.get_siblings(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_top(self, node, hierarchy=MAIN):
        """get_top(node, hierarchy=MAIN)
        Returns the top node of the children in the specified hierarchy

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Unique node identifier
        """
        return self.dim.get_top(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def get_union(self, hierarchies):
        """get_union(hierarchies)
        Union of nodes between main and alternate hierarchies

        Args:
            hierarchies (list): list of alternate hierarchies to use

        Returns:
            list: Union of all nodes across hierarchies
        """
        return self.dim.get_union(project_id=self.project_id, name=self.name, hierarchies=hierarchies)

    def get_up(self, parent, child, hierarchy=MAIN):
        """get_up(parent, child, hierarchy=MAIN)
        Returns the previous node of the children in the specified hierarchy

        Args:
            parent (str): Parent Node Key
            child (str): Child Node Key
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Unique node identifier
        """
        return self.dim.get_up(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def is_below_group(self, node, group_id, hierarchy=MAIN):
        """is_below_group(node, group_id, hierarchy=MAIN)
        Checks if a node is contained in a group

        Args:
            node (str): Unique hierarchy node identifier
            group_id (str): Group node unique identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if node is contained in group
        """
        return self.dim.is_below_group(project_id=self.project_id, name=self.name, node=node, group_id=group_id, hierarchy=hierarchy)

    def is_bottom(self, parent, child, hierarchy=MAIN):
        """is_bottom(parent, child, hierarchy=MAIN)
        Check if node is the bottom child of the parent node

        Args:
            parent (str): Parent node ID
            child (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if the child descends from the parent
        """
        return self.dim.is_bottom(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def is_child_of(self, node, parent, hierarchy=MAIN):
        """is_child_of(node, parent, hierarchy=MAIN)
        Check if node is a child of the parent node

        Args:
            node (str): Unique hierarchy node identifier
            parent (str): Parent node ID
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if the child descends from the parent
        """
        return self.dim.is_child_of(project_id=self.project_id, name=self.name, node=node, parent=parent, hierarchy=hierarchy)

    def is_descendent_of(self, node, ancestor_id, hierarchy=MAIN):
        """is_descendent_of(node, ancestor_id, hierarchy=MAIN)
        Checks if node is a decendant of an ancestor node

        Args:
            node (str): Unique hierarchy node identifier
            ancestor_id (str): Node ID of ancestor
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if the node is an ancestor
        """
        return self.dim.is_descendent_of(project_id=self.project_id, name=self.name, node=node,
                                         ancestor_id=ancestor_id,
                                         hierarchy=hierarchy)

    def is_parent_of(self, parent, child, hierarchy=MAIN):
        """is_parent_of(parent, child, hierarchy=MAIN)
        Checks if node is a parent of the child node

        Args:
            parent (str): Unique hierarchy node identifier
            child (str): Child node unique identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if the child descends from parent
        """
        return self.dim.is_parent_of(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def is_top(self, parent, child, hierarchy=MAIN):
        """is_top(parent, child, hierarchy=MAIN)
        Check if node is the top child of the parent node

        Args:
            parent (str): Parent node ID
            child (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if the child descends from the parent
        """
        return self.dim.is_top(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def is_leaf(self, node, hierarchy=MAIN):
        """is_leaf(node, hierarchy=MAIN)
        Check if node is a leaf

        Args:
            node (str): node ID
            hierarchy (str): Hierarchy unique ID
        """
        return self.dim.is_leaf(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def is_parent(self, node, hierarchy=MAIN):
        """is_parent(node, hierarchy=MAIN)
        Check if node is a parent

        Args:
            node (str): node ID
            hierarchy (str): Hierarchy unique ID
        """
        return self.dim.is_parent(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def node_exists(self, node):
        """node_exists(node)
        Returns if the specified node exists else False

        Args:
        node (str): Unique hierarchy node identifier

        Returns:
            bool: True if node exists
        """
        return self.dim.node_exists(project_id=self.project_id, name=self.name, node=node)

    def node_in_hierarchy(self, node, hierarchy):
        """node_in_hierarchy(node, hierarchy)
        Returns True if specified node exists in hierarchy else False

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Hierarchy unique ID

        Returns:
            bool: True if node is in specified hierarchy
        """
        return self.dim.node_in_hierarchy(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy)

    def sort_children(self, node, hierarchy, ordering=None, alpha=True):
        """sort_children(node, hierarchy, ordering=None, alpha=True)
        Sort a parent's children

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Alternate hierarchy unique ID
            ordering (str): DESC/desc to sort in descending order
            alpha (bool): True = sort alphanumerically (default)
                          False = sort numerically

        Returns:
            None
        """
        self.dim.sort_children(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy, ordering=ordering, alpha=alpha)

    def sort_descendents(self, node, hierarchy, ordering=None, alpha=True):
        """sort_descendents(node, hierarchy, ordering=None, alpha=True)
        Sort a parent's descendents

        Args:
            node (str): Unique hierarchy node identifier
            hierarchy (str): Alternate hierarchy unique ID
            ordering (str): DESC/desc to sort in descending order
            alpha (bool): True = sort alphanumerically (default)
                          False = sort numerically

        Returns:
            None
        """
        self.dim.sort_descendents(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy, ordering=ordering, alpha=alpha)

    def which_hierarchies(self, node):
        """which_hierarchies(node)
        Returns the hierarchies from the specified node

        Args:
            node (str): Unique hierarchy node identifier

        Returns:
            list: Hierarchies containing node
        """
        return self.dim.which_hierarchies(project_id=self.project_id, name=self.name, node=node)

    # --------------------------------------------------------------------------------------------------
    # ==== ATTRIBUTE METHODS ===========================================================================
    # --------------------------------------------------------------------------------------------------
    def get_all_attributes(self):
        """get_all_attributes()
        Returns all attributes in dimension

        Args:

        Returns:
            dict: Dict of dicts by hierarchy/node/parent atrribute
        """
        return self.dim.get_all_attributes(project_id=self.project_id, name=self.name)

    def get_all_inherited_attributes(self):
        """get_all_inherited_attributes()
        Returns all attributes including inherited attributes in dimension

        Args:

        Returns:
            dict : Dict of dicts
                - node (str): Unique hierarchy node identifier
                - attribute (str): Attribute
                - hierarchy (str): Hierarchy unique ID
                - inherited (bool): Inherited value returned
                - ancestor (str): Node holding inherited attribute

        """
        return self.dim.get_all_inherited_attributes(project_id=self.project_id, name=self.name)

    def which_attributes(self, node):
        """which_attributes(node)
        Returns the hierarchies and attribute node from the specified node

        Args:
            node (str): Unique hierarchy node identifier

        Returns:
            list: list of dicts with hHierarchies and attribute node for the node
        """
        return self.dim.which_attributes(project_id=self.project_id, name=self.name, node=node)

    # --------------------------------------------------------------------------------------------------
    # ==== CONSOLIDATION METHODS =======================================================================
    # --------------------------------------------------------------------------------------------------
    def get_consolidation(self, parent, child, hierarchy=MAIN):
        """get_consolidation(node, hierarchy=MAIN)
        Gets the consolidation type of a node within the specified alt hierarchy

        Args:
            parent (str): Parent node ID
            child (str): Child node ID
            hierarchy (str): Hierarchy unique ID

        Returns:
            str: Consolidation type of node  ('~', '+', '-', '|', '&')
                 ~ = None
                 + = Add
                 - = Subtract
                 | = OR
                 & = AND
        """
        return self.dim.get_consolidation(project_id=self.project_id, name=self.name, parent=parent, child=child, hierarchy=hierarchy)

    def set_consolidation(self, parent, child, consolidation='+', hierarchy=MAIN):
        """set_consolidation(node, parent, consolidation='+', hierarchy=MAIN)
        Sets the consolidation type of a node within the specified alt hierarchy

        Args:
            parent (str): Parent node ID
            child (str): Unique hierarchy node identifier
            consolidation: Consolidation type ('~', '+', '-', '|', '&')
                           ~ = None
                           + = Add
                           - = Subtract
                           | = OR
                           & = AND
            hierarchy (str): Hierarchy unique ID

        Returns:
            None
        """
        self.dim.set_consolidation(project_id=self.project_id, name=self.name, parent=parent, child=child,
                                   consolidation=consolidation, hierarchy=hierarchy)

    # --------------------------------------------------------------------------------------------------
    # ==== ALIAS METHODS ===============================================================================
    # --------------------------------------------------------------------------------------------------
    def get_default_aliases(self):
        """get_default_aliases()
        Adds a new alias

        Args:

        Returns:
            dict - primary (str): Primary alias unique ID
                   secondary (str): Secondary alias unique ID
        """
        return self.dim.get_default_aliases(project_id=self.project_id, name=self.name)

    def set_default_aliases(self, primary=None, secondary=None):
        """set_default_aliases(primary, secondary)
        Adds a new alias

        Args:
            primary (str): Primary alias unique ID
            secondary (str): Secondary alias unique ID

        Returns:
            None
        """
        self.dim.set_default_aliases(project_id=self.project_id, name=self.name, primary=primary, secondary=secondary)

    def add_alias(self, alias):
        """add_alias(alias)
        Adds a new alias

        Args:
            alias (str): Alias unique ID

        Returns:
            None
        """
        self.dim.add_alias(project_id=self.project_id, name=self.name, alias=alias)

    def delete_alias(self, alias):
        """delete_alias(alias)
        Delete an alias

        Args:
            alias (str): Alias unique ID

        Returns:
            None
        """
        self.dim.delete_alias(project_id=self.project_id, name=self.name, alias=alias)

    def get_aliases(self):
        """get_aliases(p)
        Returns current alias names

        Args:

        Returns:
            list: List of aliases types
        """
        return self.dim.get_aliases(project_id=self.project_id, name=self.name)

    def is_alias(self, alias):
        """is_alias(alias)
        Checks alias exists

        Args:
            alias (str): Alias unique ID

        Returns:
            bool: Does the alias exist
        """
        return self.dim.is_alias(project_id=self.project_id, name=self.name, alias=alias)

    def rename_alias(self, old, new):
        """rename_alias(old, new)
        Renames an alias

        Args:
            old (str): Current alias unique ID
            new (str): New alias unique ID

        Returns:
            None
        """
        self.dim.rename_alias(project_id=self.project_id, name=self.name, old=old, new=new)

    def delete_node_alias(self, node, alias):
        """delete_node_alias(node, alias)
        Creates a new node alias

        Args:
            node (str): Unique hierarchy node identifier
            alias (str): Alias ID

        Returns:
            None
        """
        self.dim.delete_node_alias(project_id=self.project_id, name=self.name, node=node, alias=alias)

    def get_all_aliases(self):
        """get_all_aliases()
        Return all values in dimension

        Args:

        Returns:
            dict: Dict of dicts by alias name/node/alias
        """
        return self.dim.get_all_aliases(project_id=self.project_id, name=self.name)

    def get_node_alias(self, node, alias):
        """get_node_alias(node, alias)
        Gets an alias for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            alias (str): Alias type

        Returns:
            str: Alias of node
        """
        return self.dim.get_node_alias(project_id=self.project_id, name=self.name, node=node, alias=alias)

    def get_node_from_alias(self, alias, value):
        """get_node_from_alias( alias, value)
        Finds the node object using the alias

        Args:
            alias (str): Alias type
            value (str): Alias of node

        Returns:
            str: Node name
        """
        return self.dim.get_node_from_alias(project_id=self.project_id, name=self.name, alias=alias, value=value)

    def set_node_alias(self, node, alias, value):
        """set_node_alias(node, alias, value)
        Sets an alias for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            alias (str): Alias type
            value (str): Alias of node

        Returns:
            None
        """
        self.dim.set_node_alias(project_id=self.project_id, name=self.name, node=node, alias=alias, value=value)

    def set_node_aliases(self, node_alias_values):
        """Sets multiple node aliases at once

        Args:
            node_alias_values (list): List of dicts containing
                node (str): Unique hierarchy node identifier
                alias (str): Alias type
                value (str): Alias of node

        Returns:
            None
        """
        self.dim.set_node_aliases(project_id=self.project_id, name=self.name, node_alias_values=node_alias_values)

    def which_aliases(self, node):
        """which_aliases(node)
        Returns the aliases used by the specified node

        Args:
            node (str): Unique alias node identifier

        Returns:
            dict: Aliases containing node and values
        """
        return self.dim.which_aliases(project_id=self.project_id, name=self.name, node=node)

    # --------------------------------------------------------------------------------------------------
    # ==== PROPERTY METHODS ============================================================================
    # --------------------------------------------------------------------------------------------------
    # noinspection PyShadowingBuiltins
    def add_property(self, property, type=None, display=None, role=None, config=None):
        """add_property(property, type=None, display=None, role=None, config=None)
        Adds a new property

        Args:
            property (str): Property unique ID
            type (str): property type for data editor
            display (str): property display option
            role (str): role in allocations (if dimensions is used this way)
            config (dict): property config for a type

        Returns:
            None
        """
        self.dim.add_property(project_id=self.project_id, name=self.name, property=property, type=type,
                              display=display, role=role, config=config)

    # noinspection PyShadowingBuiltins
    def delete_property(self, property):
        """delete_property(property)
        Delete an property

        Args:
            property (str): Property unique ID

        Returns:
            None
        """
        self.dim.delete_property(project_id=self.project_id, name=self.name, property=property)

    def get_properties(self):
        """get_properties()
        Displays the list of current properties

        Args:

        Returns:
            list: List of current property types
        """
        return self.dim.get_properties(project_id=self.project_id, name=self.name)

    def get_property_config(self, property):
        """get_property_config(property)
        Get the config for the property

        Args:
            property (str): Property unique ID

        Returns:
            dict: type: JSON config
        """
        return self.dim.get_property_config(project_id=self.project_id, name=self.name, property=property)

    def set_property_config(self, property, type, display, role, config):
        """set_property_config(property, type, display, role, config)
        Set the config for the property

        Args:
            property (str): Property unique ID
            type (str): Property type ID
            display (str): Property display option
            role (str): role in allocations (if dimensions is used this way)
            config (str): JSON config string
        Returns:
            None
        """
        self.dim.set_property_config(project_id=self.project_id, name=self.name, property=property,
                                     type=type, display=display, role=role, config=config)

    def is_property(self, property):
        """is_property(property)
        Checks property exists

        Args:
            property (str): Property unique ID

        Returns:
            bool: Does the property exist
        """
        return self.dim.is_property(project_id=self.project_id, name=self.name, property=property)

    def rename_property(self, old, new):
        """rename_property(old, new)
        Renames a property

        Args:
            old (str): Current property unique ID
            new (str): New property unique ID

        Returns:
            None
        """
        self.dim.rename_property(project_id=self.project_id, name=self.name, old=old, new=new)

    def clear_property(self, property):
        """Clear all values of a property

        Args:
            property (str): Property unique ID

        Returns:
            None
        """
        self.dim.clear_property(project_id=self.project_id, name=self.name, property=property)

    def delete_node_property(self, node, property):
        """Delete a property

        Args:
            node (str): Unique hierarchy node identifier
            property (str): Property type

        Returns:
            None
        """
        self.dim.delete_node_property(project_id=self.project_id, name=self.name, node=node, property=property)

    def get_all_properties(self, inherit=False, hierarchy=None, only_properties=None):
        """get_all_properties(inherit=False, hierarchy=None)
        Returns all properties including inherited properties in dimension

        Args:
            inherit (bool): Find inherited property
            hierarchy (str, optional): Hierarchy unique ID or None returns all hierarchies
            only_properties (list, optional): Restrict the properties returned

        Returns:
            dict : Dict of dicts
                - node (str): Unique hierarchy node identifier
                - property(str): Property type
                - value (str): Value or None
                - hierarchy (str): Hierarchy unique ID
                - inherited (bool): Inherited value returned
                - ancestor (str): Node holding inherited value

        """
        return self.dim.get_all_properties(project_id=self.project_id, name=self.name, inherit=inherit,
                                           hierarchy=hierarchy, only_properties=only_properties)

    def get_node_property(self, node, property, inherit=False, hierarchy=MAIN):
        """get_node_property(node, property, inherit=False, hierarchy=MAIN)
        Gets a property for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            property (str): Property type
            inherit (bool): Find inherited property
            hierarchy (str): Hierarchy unique ID

        Returns:
            dict:
                - node (str): Unique hierarchy node identifier
                - property(str): Property type
                - value (str): Value or None
                - hierarchy (str): Hierarchy unique ID
                - inherited (bool): Inherited value returned
                - ancestor (str): Node holding inherited value
        """
        return self.dim.get_node_property(project_id=self.project_id, name=self.name, node=node,
                                          property=property, inherit=inherit, hierarchy=hierarchy)

    def get_nodes_from_property(self, property, value):
        """get_nodes_from_property(property, value)
        Finds the node objects using the property

        Args:
            property (str): Property type
            value (str): Property value

        Returns:
            list: Node names
        """
        return self.dim.get_nodes_from_property(project_id=self.project_id, name=self.name,
                                                property=property, value=value)

    def set_node_property(self, node, property, value):
        """set_node_property(node, property, value)
        Sets a propoerty for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            property (str): Property type
            value (str): Property value

        Returns:
            None
        """
        self.dim.set_node_property(project_id=self.project_id, name=self.name, node=node, property=property, value=value)

    def set_node_properties(self, node_property_values):
        """Sets multiple node properties at once

        Args:
            node_property_values (list): List of dicts containing node properties to set
                node (str): Unique hierarchy node identifier
                property (str): Property type
                value (str): Property value

        Returns:
            None
        """
        self.dim.set_node_properties(project_id=self.project_id, name=self.name, node_property_values=node_property_values)

    def which_properties(self, node):
        """which_properties(node)
        Returns the properties used by the specified node

        Args:
            node (str): Unique property node identifier

        Returns:
            dict: Properties containing node and values
        """
        return self.dim.which_properties(project_id=self.project_id, name=self.name, node=node)

    # --------------------------------------------------------------------------------------------------
    # ==== VALUE METHODS ===============================================================================
    # --------------------------------------------------------------------------------------------------
    def add_value(self, value):
        """add_value(value)
        Adds a new value

        Args:
            value (str): Value unique ID

        Returns:
            None
        """
        self.dim.add_value(project_id=self.project_id, name=self.name, value=value)

    def delete_value(self, value):
        """delete_value(value)
        Delete a value

        Args:
            value (str): Value unique ID

        Returns:
            None
        """
        self.dim.delete_value(project_id=self.project_id, name=self.name, value=value)

    def get_values(self):
        """get_values()
        Displays the list of current values

        Args:

        Returns:
            set: Set of current value types
        """
        return self.dim.get_values(project_id=self.project_id, name=self.name)

    def is_value(self, value):
        """is_value(value)
        Checks value exists

        Args:
            value (str): Value unique ID

        Returns:
            bool: Does the value exist
        """
        return self.dim.is_value(project_id=self.project_id, name=self.name, value=value)

    def rename_value(self, old, new):
        """rename_value(old, new)
        Renames a value

        Args:
            old (str): Current value unique ID
            new (str): New value unique ID

        Returns:
            None
        """
        self.dim.rename_value(project_id=self.project_id, name=self.name, old=old, new=new)

    def delete_node_value(self, node, value):
        """delete_node_value(node, value)
        Delete a value

        Args:
            node (str): Unique hierarchy node identifier
            value (str): Value type

        Returns:
            None
        """
        self.dim.delete_node_value(project_id=self.project_id, name=self.name, node=node, value=value)

    def get_all_values(self):
        """get_all_values()
        Returns all values in dimension

        Args:

        Returns:
            dict: Dict of dicts by value name/node/value
        """
        return self.dim.get_all_values(project_id=self.project_id, name=self.name)

    def get_node_value(self, node, value):
        """get_node_value(node, value)
        Get value for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            value (str): Value name

        Returns:
            float: Value of node for specified value name
        """
        return self.dim.get_node_value(project_id=self.project_id, name=self.name, node=node, value=value)

    def get_nodes_from_value(self, value, number):
        """get_nodes_from_value(value, value)
        Finds the node objects using the value

        Args:
            value (str): Value type
            number (float): Value number

        Returns:
            list: Node names
        """
        return self.dim.get_nodes_from_value(project_id=self.project_id, name=self.name, value=value, number=number)

    def set_node_value(self, node, value, number):
        """set_node_value(node, value, value)
        Sets a propoerty for the specified node

        Args:
            node (str): Unique hierarchy node identifier
            value (str): Value type
            number (float): Value number

        Returns:
            None
        """
        self.dim.set_node_value(project_id=self.project_id, name=self.name, node=node, value=value, number=number)

    def set_node_values(self, node_value_values):
        """Sets multiple node values at once

        Args:
            node_value_values (list): List of dicts containing
                node (str): Unique hierarchy node identifier
                value_name (str): Value name
                value (float): Value to set for value name


        Returns:
            None
        """
        self.dim.set_node_values(project_id=self.project_id, name=self.name, node_value_values=node_value_values)

    def which_values(self, node):
        """which_values(node)
        Returns the values used by the specified node

        Args:
            node (str): Unique value node identifier

        Returns:
            dict: Values containing node and values
        """
        return self.dim.which_values(project_id=self.project_id, name=self.name, node=node)

    # --------------------------------------------------------------------------------------------------
    # ==== LOAD METHODS ================================================================================
    # --------------------------------------------------------------------------------------------------
    # noinspection PyUnusedLocal
    def load_hierarchy_from_dataframe(self, df, parents, children, consolidations=None, consol_default='+',
                                      hierarchy=MAIN, clear=False):
        """load_hierarchy_from_dataframe(self, df, parents, children, consolidations, consol_default, hierarchy, clear)
        Bulk loads a hierarchy from a Dataframe
        Args:
            df (Dataframe): Datafame with P/C nodes
            parents (str): Column with parent nodes
            children (str): Column with children nodes
            consolidations (str): Column with consolidations nodes
            consol_default (str): consolidation type '~', '+', '-', '|', '&'
            hierarchy (str): alt hierarchy key or column in dataframe
            clear (bool): Clear the hierarchy before loading

        Returns:
            dataframe:
                    - hierarchy (str): hierarchy ID
                    - parent (str): parent node ID
                    - child (str): child node ID
                    - consolidation (str): consolidation type
                    - status (bool): Success True or False
                    - code (int): Result code
                    - message (str): Message string
        """
        if not isinstance(df, pd.DataFrame):
            raise Exception('df parameter is not a valid dataframe')

        if parents not in df:
            raise Exception(f'Parents column {parents} does not exist')

        if children not in df:
            raise Exception(f'Children column {children} does not exist')

        if consolidations:
            default_consol = False
            if consolidations not in df:
                raise Exception(f'Consolidations column {consolidations} does not exist')
        else:
            default_consol = True
            if consol_default not in VALID_CONSOL:
                raise Exception(f'Consolidation default value {consol_default} is invalid')

        # Remove any duplicate rows
        df.drop_duplicates(inplace=True)

        # Tidy up data by converting to empty strings
        df = df.replace(r'^\s*$', '', regex=True).replace('None', '')
        df.fillna('', inplace=True)

        # Do we have a hierarchy column?
        if hierarchy in df.columns:
            default_hier = False
            load_hiers = list(np.unique(df[hierarchy]))
            if len(load_hiers) == 0:
                raise Exception(f'Hierarchy {hierarchy} column is null')
        else:
            default_hier = True
            load_hiers = [hierarchy]

        if USE_DATAFRAME_LOADING:
            json_df = self._encode_dataframe(df)
            results_df = self.dim.load_hierarchy_from_dataframe(project_id=self.project_id, name=self.name, df=json_df,
                                                                parents=parents, children=children,
                                                                consolidations=consolidations,
                                                                consol_default=consol_default, hierarchy=hierarchy)
            return_df = self._decode_dataframe(results_df)
            return return_df

        # Add/clear any necessary hierarchies
        for load_hier in load_hiers:
            if not self.is_hierarchy(load_hier) and load_hier != '':
                self.add_alt_hierarchy(load_hier)
            if clear:
                self.clear_alt_hierarchy(load_hier)

        # Node sets for use below
        node_sets = {
            load_hier: self.get_all_nodes(load_hier)
            for load_hier in set(load_hiers + [MAIN])
        }

        def _get_result(node, status, code, message):
            return {
                'hierarchy': node['hierarchy'],
                'parent': node['parent'],
                'child': node['child'],
                'consolidation': node['consol'],
                'status': status,
                'code': code,
                'message': message,
            }

        # Results list for dataframe
        results = []
        for index, row in df.iterrows():
            node = {
                'parent': row[parents] or ROOT,
                'child': row[children],
                'consol': row[consolidations] if not default_consol and row[consolidations] in VALID_CONSOL else consol_default,
                'hierarchy': row[hierarchy] if not default_hier else hierarchy
            }
            # only send nodes with a hierarchy set (unless defaulted)
            if not node['hierarchy']:
                results.append(_get_result(node, False, -32, 'Hierarchy is null'))
            # Child cannot be empty, ROOT or equal to parent
            elif not node['child']:
                results.append(_get_result(node, False, -2, 'Child is null'))
            elif node['child'] == ROOT:
                results.append(_get_result(node, False, -4, 'Child cannot be Root'))
            elif node['child'] == node['parent']:
                results.append(_get_result(node, False, -8, 'Child is the same as Parent'))
            # Cannot add to main node in an alternative hierarchy
            elif node['parent'] in node_sets[MAIN] and node['parent'] != ROOT and node['hierarchy'] != MAIN:
                results.append(_get_result(node, False, -16, 'Alt hierarchy cannot modify Main nodes'))
            else:
                node_added = False
                node_moved = False
                parent_added = False
                add_result = self.add_node(
                    parent=node['parent'],
                    child=node['child'],
                    consolidation=node['consol'],
                    hierarchy=node['hierarchy'],
                )
                if add_result:  # In case None is returned
                    parent_added, node_added, node_moved = add_result
                if node_added:
                    results.append(_get_result(node, True, 2, 'Child added'))
                elif node_moved:
                    results.append(_get_result(node, True, 8, 'Child moved to new parent'))
                else:
                    results.append(_get_result(node, True, 0, 'No change'))
                if parent_added:
                    results.append(_get_result(node, True, 4, 'Parent added'))

        df_results = pd.DataFrame(results, columns=['hierarchy', 'parent', 'child', 'consolidation',
                                                    'status', 'code', 'message'])

        return df_results

    # noinspection PyUnusedLocal
    def load_aliases_from_dataframe(self, df, nodes, names, values):
        """Bulk loads aliases from a Dataframe

        Args:
            df (Dataframe): Dataframe with P/C nodes
            nodes (str): Column with node names
            names (str): Column with alias names
            values (str): Column with alias values
        Returns:
            None
        """
        # Basic error checking
        if not isinstance(df, pd.DataFrame):
            raise Exception('df parameter is not a valid dataframe')
        if not isinstance(nodes, str):
            raise Exception('nodes parameter is not a valid string')
        if not isinstance(names, str):
            raise Exception('names parameter is not a valid string')
        if not isinstance(values, str):
            raise Exception('values parameter is not a valid string')

        # Check that the columns exist in the dataframe
        if nodes not in df:
            raise Exception(f'Nodes column {nodes} does not exist')
        if names not in df:
            raise Exception(f'Names column {names} does not exist')
        if values not in df:
            raise Exception(f'Values column {values} does not exist')

        # Remove any duplicate rows, coping with lists
        df = df.loc[df.astype(str).drop_duplicates().index]

        if USE_DATAFRAME_LOADING:
            json_df = self._encode_dataframe(df)
            return self.dim.load_aliases_from_dataframe(
                project_id=self.project_id, name=self.name, df=json_df, nodes=nodes, names=names, values=values
            )

        aliases = self.get_aliases()
        # Create any new aliases
        for alias_name in np.unique(df[names]):
            if alias_name not in aliases:
                self.add_alias(alias_name)

        # Add aliases
        return self.set_node_aliases(
            [
                {
                    'node': row[nodes],
                    'alias': row[names],
                    'value': row[values]
                } for index, row in df.iterrows()
            ]
        )

    # noinspection PyUnusedLocal
    def load_properties_from_dataframe(self, df, nodes, names, values):
        """Bulk loads properties from a Dataframe

        Args:
            df (Dataframe): Dataframe with P/C nodes
            nodes (str): Column with node names
            names (str): Column with property names
            values (str): Column with property values
        Returns:
            None
        """
        # Basic error checking
        if not isinstance(df, pd.DataFrame):
            raise Exception('df parameter is not a valid dataframe')
        if not isinstance(nodes, str):
            raise Exception('nodes parameter is not a valid string')
        if not isinstance(names, str):
            raise Exception('names parameter is not a valid string')
        if not isinstance(values, str):
            raise Exception('values parameter is not a valid string')

        # Check that the columns exist in the dataframe
        if nodes not in df:
            raise Exception(f'Nodes column {nodes} does not exist')
        if names not in df:
            raise Exception(f'Names column {names} does not exist')
        if values not in df:
            raise Exception(f'Values column {values} does not exist')

        # Remove any duplicate rows, coping with lists
        df = df.loc[df.astype(str).drop_duplicates().index]

        if USE_DATAFRAME_LOADING:
            json_df = self._encode_dataframe(df)
            return self.dim.load_properties_from_dataframe(
                project_id=self.project_id, name=self.name, df=json_df, nodes=nodes, names=names, values=values
            )

        # Create any new properties
        properties = self.get_properties()
        for prop_name in np.unique(df[names]):
            if prop_name not in properties:
                self.add_property(prop_name)

        # Add properties
        return self.set_node_properties(
            [
                {
                    'node': row[nodes],
                    'property': row[names],
                    'value': row[values]
                } for index, row in df.iterrows()
            ]
        )

    # noinspection PyUnusedLocal
    def load_values_from_dataframe(self, df, nodes, names, values):
        """Bulk loads values from a Dataframe

        Args:
            df (Dataframe): Dataframe with P/C nodes
            nodes (str): Column with node names
            names (str): Column with value names
            values (str): Column with value values
        Returns:
            None
        """
        # Basic error checking
        if not isinstance(df, pd.DataFrame):
            raise Exception('df parameter is not a valid dataframe')
        if not isinstance(nodes, str):
            raise Exception('nodes parameter is not a valid string')
        if not isinstance(names, str):
            raise Exception('names parameter is not a valid string')
        if not isinstance(values, str):
            raise Exception('values parameter is not a valid string')

        # Check that the columns exist in the dataframe
        if nodes not in df:
            raise Exception(f'Nodes column {nodes} does not exist')
        if names not in df:
            raise Exception(f'Names column {names} does not exist')
        if values not in df:
            raise Exception(f'Values column {values} does not exist')

        # Remove any duplicate rows, coping with lists
        df = df.loc[df.astype(str).drop_duplicates().index]

        if USE_DATAFRAME_LOADING:
            json_df = self._encode_dataframe(df)
            return self.dim.load_values_from_dataframe(
                project_id=self.project_id, name=self.name, df=json_df, nodes=nodes, names=names, values=values
            )

        all_values = self.get_values()
        # Create any new values
        for val_name in np.unique(df[names]):
            if val_name not in all_values:
                self.add_value(val_name)

        # Add values
        return self.set_node_values(
            [
                {
                    'node': row[nodes],
                    'value_name': row[names],
                    'value': row[values]
                } for index, row in df.iterrows()
            ]
        )

    # noinspection PyUnusedLocal
    def load_from_table_flat(self, table, columns, top=None, consolidations=None, consol_default='+',
                             hierarchy=MAIN, alias_columns=None, property_columns=None, value_columns=None,
                             leaf_child: str = None):
        """load_from_table(table, parents, children, consolidations, consol_default, hierarchy, connection')
        Bulk loads a dimension from an Analyze table with flattened hierarchy

        Args:
            table (str): Name of table to query
            columns (list): Column names with flattened hierarchy
            top (str): Top level name to start dimension hierarchy
            consolidations (str): Column with consolidations nodes
            consol_default (str): consolidation type +, -, or ~
            hierarchy (str): alt hierarchy key
            alias_columns (list): List of columns containing alias values, alias name is column name
            property_columns (list): List of columns containing property values, property name is column name
            value_columns (list): List of columns containing values, value name is column name
            leaf_child (str, optional): Column containing leaf level - to match with alias/properties/values - if omitted, will be derived

        Returns:
            None
        """
        self.dim.load_from_table_flat(project_id=self.project_id, name=self.name, table=table, columns=columns, top=top,
                                      consolidations=consolidations, consol_default=consol_default,
                                      hierarchy=hierarchy, alias_columns=alias_columns, property_columns=property_columns,
                                      value_columns=value_columns, leaf_child=leaf_child)

    # noinspection PyUnusedLocal
    def load_from_table_pc(self, table, parents, children, consolidations=None, consol_default='+',
                           hierarchy=MAIN, alias_columns=None, property_columns=None, value_columns=None):
        """load_from_table(table, parents, children, consolidations, consol_default, hierarchy, connection')
        Bulk loads a dimension from an Analyze table
        Args:
            table (str): Name of table to query
            parents (str): Column with parent nodes
            children (str): Column with children nodes
            consolidations (str): Column with consolidations nodes
            consol_default (str): consolidation type +, -, or ~
            hierarchy (str): alt hierarchy key
            alias_columns (list): List of columns containing alias values, alias name is column name
            property_columns (list): List of columns containing property values, property name is column name
            value_columns (list): List of columns containing values, value name is column name

        Returns:
            None
        """
        self.dim.load_from_table_pc(project_id=self.project_id, name=self.name, table=table, parents=parents,
                                    children=children, consolidations=consolidations, consol_default=consol_default,
                                    hierarchy=hierarchy, alias_columns=alias_columns, property_columns=property_columns,
                                    value_columns=value_columns)

    # --------------------------------------------------------------------------------------------------
    # ==== SAVE METHODS ================================================================================
    # --------------------------------------------------------------------------------------------------
    def save_hierarchy_to_dataframe(self, hierarchy=None):
        """save_hierarchy_to_dataframe(hierarchy=MAIN)
        Get hierarchy as a Dataframe for reloading
        Args:
            hierarchy (str, list or none) - List of hierarchies to save, None means all

        Returns:
            df (Dataframe): Dataframe with hierarchy data
        """
        json_df = self.dim.save_hierarchy_to_dataframe(project_id=self.project_id, name=self.name, hierarchy=hierarchy)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def save_aliases_to_dataframe(self, alias=None):
        """save_aliases_to_dataframe(alias=None)
        Get aliases as a Dataframe for reloading
        Args:
            alias (str, list or none) - List of aliases to save, None means all

        Returns:
            df (Dataframe): Dataframe with alias nodes and values
        """
        json_df = self.dim.save_aliases_to_dataframe(project_id=self.project_id, name=self.name, alias=alias)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def save_properties_to_dataframe(self, property=None):
        """save_properties_to_dataframe(property=None)
        Get properties as a Dataframe for reloading
        Args:
            property (str, list or none) - List of properties to save, None means all
        Returns:
            df (Dataframe): Dataframe with property nodes and values
        """
        json_df = self.dim.save_properties_to_dataframe(project_id=self.project_id, name=self.name, property=property)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def save_values_to_dataframe(self, value=None):
        """save_values_to_dataframe(value=None))
        Get values as a Dataframe for reloading
        Args:
            value (str, list or none) - List of values to save, None means all

        Returns:
            df (Dataframe): Dataframe with values nodes and values
        """
        json_df = self.dim.save_values_to_dataframe(project_id=self.project_id, name=self.name, value=value)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def save_parent_child_to_db(self, hierarchy: str, table_id: str):
        self.dim.save_parent_child_to_db(project_id=self.project_id, name=self.name, hierarchy=hierarchy, table_id=table_id)

    def save_node_leaves_to_db(self, hierarchy: str, table_id: str):
        self.dim.save_node_leaves_to_db(
            project_id=self.project_id,
            name=self.name,
            hierarchy=hierarchy,
            table_id=table_id,
        )

    # --------------------------------------------------------------------------------------------------
    # ==== GET DATAFRAME METHODS =======================================================================
    # --------------------------------------------------------------------------------------------------
    def get_aliases_dataframe(self):
        """get_aliases_dataframe()
        Get aliases as a Dataframe
        Args:

        Returns:
            df (Dataframe): Dataframe with alias nodes and values
        """
        json_df = self.dim.get_aliases_dataframe(project_id=self.project_id, name=self.name)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def get_attributes_dataframe(self):
        """get_attributes_dataframe(hierarchy=MAIN)
        Get attributes as a Dataframe
        Args:

        Returns:
            df (Dataframe): Dataframe with attribute nodes and values
        """
        json_df = self.dim.get_attributes_dataframe(project_id=self.project_id, name=self.name)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def get_consolidation_dataframe(self, value, hierarchy=MAIN):
        """get_consolidation_dataframe(value, hierarchy=MAIN)
        Returns consolidated values for nodes in hierarchy

        Args:
            value (str): Value name to process
            hierarchy (str): Hierarchy unique ID to process

        Returns:
            df (Dataframe): Dataframe with hierarchy plus input values & consolidated values
        """
        json_df = self.dim.get_consolidation_dataframe(project_id=self.project_id, name=self.name,
                                                       value=value, hierarchy=hierarchy)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def get_hierarchy_dataframe(self, hierarchy=MAIN):
        """get_hierarchy_dataframe(hierarchy=MAIN)
        Get hierarchy as a Dataframe
        Args:
            hierarchy (str): Hierarchy unique ID

        Returns:
            df (Dataframe): Datafame with hierarchy data
        """
        json_df = self.dim.get_hierarchy_dataframe(project_id=self.project_id, name=self.name, hierarchy=hierarchy)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def get_properties_dataframe(self):
        """get_properties_dataframe()
        Get properties as a Dataframe
        Args:

        Returns:
            df (Dataframe): Datafame with property nodes and values
        """
        json_df = self.dim.get_properties_dataframe(project_id=self.project_id, name=self.name)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    def get_values_dataframe(self):
        """get_values_dataframe()
        Save values into a Dataframe
        Args:

        Returns:
            df (Dataframe): Datafame with alias nodes and values
        """
        json_df = self.dim.get_values_dataframe(project_id=self.project_id, name=self.name)
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    # --------------------------------------------------------------------------------------------------
    # ==== FLATTENED DATAFRAME METHODS  ================================================================
    # --------------------------------------------------------------------------------------------------
    def dimension_table(self):
        """dimension_table()
        All Dimension Hierarchy data flattened into a dict of dataframes
        Args:

        Returns:
            dict: Dict of Datafames with hierarchy data
                - hierarchically sorted nodes
                - attributes/aliases/properties/values appended as columns
        """
        json_dict_df = self.dim.dimension_table(project_id=self.project_id, name=self.name)
        table = {}
        for hierarchy, df in json_dict_df.items():
            table[hierarchy] = self._decode_dataframe(df)
            table[hierarchy].reset_index(inplace=True)
        return table

    def hierarchy_table(self, hierarchy=MAIN, inherit_properties=True, include_inherited_columns=False):
        """Hierarchy data flattened into a dataframe

        Args:
            hierarchy (str): Hierarchy unique ID
            inherit_properties (bool, optional): If the properties returned in the dataframe should inherit from above
            include_inherited_columns (bool, optional): If additional inherited property columns should be included, [*.inherited, *.ancestor]

        Returns:
            df (Dataframe): Dataframe with hierarchy data
                - hierarchically sorted nodes
                - attributes/aliases/properties/values appended as columns
        """
        json_df = self.dim.hierarchy_table(
            project_id=self.project_id,
            name=self.name,
            hierarchy=hierarchy,
            inherit_properties=inherit_properties,
            include_inherited_columns=include_inherited_columns
        )
        df = self._decode_dataframe(json_df)
        df.reset_index(inplace=True)
        return df

    # --------------------------------------------------------------------------------------------------
    # ==== DATAFRAME RPC METHODS  ======================================================================
    # --------------------------------------------------------------------------------------------------
    @staticmethod
    def _decode_dataframe(json_df) -> pd.DataFrame:
        return pd.read_json(io.StringIO(json_df), orient='table', precise_float=True)

    @staticmethod
    def _encode_dataframe(df: pd.DataFrame):
        return df.to_json(orient='table', index=False)

    # --------------------------------------------------------------------------------------------------
    # ==== RECURSIVE METHODS ===========================================================================
    # --------------------------------------------------------------------------------------------------
    def ascend(self, node, hierarchy=MAIN, alias=None):
        """ascend(node, hierarchy=MAIN, alias=None)
        Ascends hierarchy from node

        Args:
            node (str): Node alias name
            hierarchy (str): Hierarchy unique ID
            alias (str): Alias type

        Returns:
            List: List of lists
                  (int): of node generation
                  (str): node unique identifier
        """
        return self.dim.ascend(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy, alias=alias)

    def descend(self, node, hierarchy=MAIN, alias=None):
        """descend(node, hierarchy=MAIN, alias=None)
        Descends hierarchy from node

        Args:
            node (str): Node alias name
            hierarchy (str): Hierarchy unique ID
            alias (str): Alias type

        Returns:
            List: List of lists
                  (int): of node generation
                  (str): node unique identifier
        """
        return self.dim.descend(project_id=self.project_id, name=self.name, node=node, hierarchy=hierarchy, alias=alias)
