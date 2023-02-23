#!/usr/bin/env python
# coding=utf-8

from plaidcloud.utilities.remote.dimension import Dimensions
from plaidcloud.utilities.remote.dimension import Dimension

__author__ = 'Dave Parsons'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Dave Parsons']
__license__ = 'Apache 2.0'
__maintainer__ = 'Dave Parsons'
__email__ = 'dave.parsons@tartansolutions.com'


# @DAVE back story, I initially tried adding methods within the Dimensions class, but I realized I was doing things in
# a way that felt inconsistent with what I saw.  I'm putting this stuff here for now. The use case is UDF helpers. I feel what's
# being done here can definitely be done more efficiently.  I also have a lot of questions:
#
# Questions:
# Why do Dimensions methods like copy_dimension() not return the dimension object?  Shouldn't they?
# Why is there no Dimensions.get_dimension() method?
#
# Needs:
# Dimension editing methods need to allow for expected usage and guard against edge cases.
#
# - Like PCM, if a node is added, but already exists, it should just be re-parented.

# - A dimension object should have a method to clear an existing hierarchy.

# - move_node method should not need to know old parent.  User should not need to know old parent in order to reparent, just like PCM.

# - If a node is added to a parent that doesn't already exist in the dimension, the new parent should be added to the root of the
#   dimension, like PCM.
#   Currently, *invisible* nodes can be added.  You can add children to a parent that doesn't exist.
#   Later, if the parent is added to the dimension as a child, then its children will show up too.

# - Guide rails (and possibly feedback / logging if things being requested are not allowed, or if they are likely to yield
#   an unexpected result.)
#   - What happens if a node exists in alternate hierarchy (a) but somebody tries to do something with it as if it is in hierarchy (b)

# If a node is added to an alternate hierarchy that does not yet exist, the alternate hierarchy should be created.


def instantiate_dimension(conn, dim_name, replace=False):
    """
    'update' == False starts from scratch.
    """
    dims = Dimensions(conn=conn)

    if '/' in dim_name:
        # This is a path.  Perform a path lookup.
        path = '/'.join(dim_name.split('/')[:-1])
        dim_name = dim_name.split('/')[-1]
    else:
        # This must be a name only.  Perform a name lookup
        dim_name = dim_name
        path = '/'

    if replace is True:
        dims.delete_dimension(name=dim_name)
        dims.add_dimension(name=dim_name)  # Add dimension method should probably automatically delete if exists first.

        dim = Dimension(conn=conn, name=dim_name)
        # dim.clear()
    else:

        dim = Dimension(conn=conn, name=dim_name)
        # TODO - We need to add the dimension if it does not already exist with dims.add_dimension.  duh.
        # TODO: How do we do this?  We need to see if Dimension exists in Dimensions, but how?  @Dave, I could not do it, I'm probably being dense.
    return dim


def load_parentchild_frame(dim, df, root=None):
    """
    load_parentchild_frame(dim, df, root=None)
    Create or update dimension nodes from a parentchild dataframe

    Args:
        dim (obj): Dimension
        df (dataframe): Dataframe containing the following required columns:
            ParentName (str): name of parent column
            ChildName (str): name of child column
            ConsolidationType (str): Consolidation Type.  Defaults to '+'
            hierarchy (str): Hierarchy Name. Defaults to 'main'
            root=None (str): root node to use

    Returns:
        None
    """

    # If root is a list, we'll try to sub out every item in the list with dim.ROOT.
    # If root is a string, then we will sub out the string with dim.ROOT and we
    # will sub out '{} Attributes' with dim.ROOT also, to handle PCM attributes properly when
    # a pretty raw PCM PC table is used as input.

    def fix_root(node):
        """Need for user to be able to send in old root node pointer."""
        # allow for > 1 thing to be replaced with root. This will thus automatically
        # enable us to handle things like 'Cost Objects 1 Attributes'
        if node in root:
            return dim.ROOT
        else:
            return node

    def remove_disallowed_characters(node_name):
        # TODO: This should be more comprehensive (and flexible.)
        node_name = node_name.replace(':', '-')
        return node_name

    def load_node(parent, child, consolidation='+', hierarchy='main'):
        """This should not be necessary.  We should be able to use the
        plaidtools add_node function directly.  We can't right now, because
        it doesn't properly deal with edge cases, like children being added to
        parents that don't yet exist."""
        if dim.get_parent(parent, hierarchy=hierarchy) is None and parent != dim.ROOT:  # This is heavy :(
            dim.add_node(parent=dim.ROOT, child=parent, consolidation=consolidation, hierarchy=hierarchy)

        # TODO: this should be done with a check that is not a try/except.
        # try:
        # dim.add_node(parent=parent, child=child, consolidation=consolidation, hierarchy=hierarchy)
        # except:
        # old_parent = dim.get_parent(child, hierarchy=hierarchy)
        # dim.move_node(child, old_parent, parent, hierarchy)

        # TODO: Handle case of updated consolidation types

        old_parent = dim.get_parent(child, hierarchy=hierarchy)
        if old_parent is None:
            dim.add_node(parent=parent, child=child, consolidation=consolidation, hierarchy=hierarchy)
        else:
            dim.move_node(child, old_parent, parent, hierarchy)

    if root:
        if not isinstance(root, list):
            root = [root, '{} Attributes'.format(root)]
    else:
        root = []

    # make 'root' a reserved word.  If parent = 'root', we will use dim.ROOT
    root.append('root')
    root = list(set(root))

    df['ParentName'] = list(map(fix_root, df['ParentName']))

    # Root param is for making sure parents like 'Line Items' and 'Line Item Attributes' can roll
    # up to the root of the dimension, and not an extra dimension member that we don't really need.

    # Special root flagging does NOT need to happen, outside of ingestion of raw PCM parentchild data.
    # Any parent that is not a child can be presumed a node with parent = dim.ROOT
    # orphans = set(df['ParentName']) - set(df['ChildName']) # Probably do not need.

    df = df[df['ParentName'] != df['ChildName']]  # Get rid of 1 extra node when PC comes from PC attributes

    if 'ConsolidationType' not in df.columns:
        df['ConsolidationType'] = '+'

    if 'hierarchy' not in df.columns:
        df['hierarchy'] = 'main'

    df['ChildName'] = list(map(remove_disallowed_characters, df['ChildName']))
    df['ParentName'] = list(map(remove_disallowed_characters, df['ParentName']))

    # Any flavor of 'maIn' needs to be 'main'.
    def lower_main(item):
        if item.lower() == 'main':
            return 'main'
        return item

    df['hierarchy'] = list(map(lower_main, df['hierarchy']))

    # create alt hierarchy if it does not already exist
    hierarchies = list(set(df['hierarchy']))
    for hierarchy in hierarchies:
        if hierarchy != 'main' and hierarchy not in dim.get_alt_hierarchies():
            dim.add_alt_hierarchy(hierarchy=hierarchy)

    # This is just a vectorized way of calling load_node
    df['added'] = list(map(load_node, df['ParentName'], df['ChildName'], df['ConsolidationType'], df['hierarchy']))


def load_alias_frame(dim, df):
    """
    load_alias_frame(dim, df, root)
    Update dimension nodes from an alias dataframe

    Args:
        dim (obj): Dimension
        df (dataframe): Dataframe containing the following required columns:
            AliasValue (str): New node alias name
            AliasName (str): Alias (category). Will be added if it does not already exist
            NodeName (str): Default alias / node name

    Returns:
        None
    """

    # TODO: Should we create nodes at root if they do not already exist?  Possibly, if we want to
    #       be consistent with PCM?

    def remove_disallowed_characters(node_name):
        # TODO: This should be more comprehensive (and flexible.)
        node_name = node_name.replace(':', '-')
        return node_name

    def load_alias(data_alias_name, new_alias, default_alias):
        """Add alias record if it is new and not the same as the default."""

        if new_alias != default_alias:
            try:
                dim.set_node_alias(node=default_alias, alias=data_alias_name, value=new_alias)
                print('{} ::: {}  Loaded Successfully'.format(new_alias, default_alias))
            except:
                print('{} ::: {}  FAILED! Reason unknown'.format(new_alias, default_alias))
        else:
            print('{} ::: {}  skipped.  Default Alias == New Alias'.format(new_alias, default_alias))

    df['AliasValue'] = list(map(remove_disallowed_characters, df['AliasValue']))
    df['AliasName'] = list(map(remove_disallowed_characters, df['AliasName']))
    df['NodeName'] = list(map(remove_disallowed_characters, df['NodeName']))

    # TODO:  Get smarter. Make unbreakable. If user sends in a non-default alias for NodeName, we should find it,
    #        and use it to identify node to which we are adding another Alias Value (like PCM.)

    alias_names = set(df['AliasName'])
    for alias_name in alias_names:
        # TODO: Do it this way once is_alias is fixed.
        # if not dim.is_alias(alias_name):
        # dim.add_alias(alias=alias_name)
        try:
            dim.add_alias(alias=alias_name)
        except:
            pass

    # This is just a vectorized way of calling load_alias
    df['added'] = list(map(load_alias, df['AliasName'], df['AliasValue'], df['NodeName']))


def load_property_frame(dim, df):
    """
    load_alias_frame(dim, df, root)
    Update dimension nodes from an alias dataframe

    Args:
        dim (obj): Dimension
        df (dataframe): Dataframe containing the following required columns:
            PropertyValue (str)
            PropertyName (str)
            NodeName (str)

    Returns:
        None
    """

    # TODO: Should we create nodes at root if they do not already exist?  Possibly, if we want to
    #       be consistent with PCM?

    def remove_disallowed_characters(node_name):
        # TODO: This should be more comprehensive (and flexible.)
        node_name = node_name.replace(':', '-')
        return node_name

    def load_property(property_name, property_value, node_name):
        """Add property"""

        try:
            dim.set_node_property(node=node_name, property=property_name, value=property_value)
            print('{} ::: {} ::: {}  Loaded Successfully'.format(node_name, property_name, property_value))
        except:
            print('{} ::: {} ::: {}  FAILED! Reason unknown'.format(node_name, property_name, property_value))

    df['PropertyValue'] = list(map(remove_disallowed_characters, df['PropertyValue']))
    df['NodeName'] = list(map(remove_disallowed_characters, df['NodeName']))

    # TODO:  Get smarter. Make unbreakable. If user sends in a non-default alias for NodeName, we should find it,
    #        and use it to identify node to which we are adding another Property Value (like PCM.)

    property_names = set(df['PropertyName'])
    for property_name in property_names:
        # TODO: Do it this way once is_alias is fixed.
        # if not dim.is_alias(alias_name):
        # dim.add_alias(alias=alias_name)
        try:
            dim.add_property(property=property_name)
        except:
            pass

    # This is just a vectorized way of calling load_property
    df['added'] = list(map(load_property, df['PropertyName'], df['PropertyValue'], df['NodeName']))


def load_value_frame(dim, df):
    """
    load_alias_frame(dim, df, root)
    Update dimension nodes from an alias dataframe

    Args:
        dim (obj): Dimension
        df (dataframe): Dataframe containing the following required columns:
            Value (float)
            ValueName (str)
            NodeName (str)

    Returns:
        None
    """

    # TODO: Should we create nodes at root if they do not already exist?  Possibly, if we want to
    #       be consistent with PCM?

    def remove_disallowed_characters(node_name):
        # TODO: This should be more comprehensive (and flexible.)
        node_name = node_name.replace(':', '-')
        return node_name

    def load_value(value_name, value, node_name):
        """Add value"""

        try:
            dim.set_node_value(node=node_name, number=value, value=value_name)
            print('{} ::: {} ::: {}  Loaded Successfully'.format(node_name, value_name, value))
        except:
            print('{} ::: {} ::: {} FAILED! Reason unknown'.format(node_name, value_name, value))

    df['ValueName'] = list(map(remove_disallowed_characters, df['ValueName']))
    df['NodeName'] = list(map(remove_disallowed_characters, df['NodeName']))

    # TODO:  Get smarter. Make unbreakable. If user sends in a non-default alias for NodeName, we should find it,
    #        and use it to identify node to which we are adding another Property Value (like PCM.)

    value_names = set(df['ValueName'])
    for value_name in value_names:
        # TODO: Do it this way once is_alias is fixed.
        # if not dim.is_alias(alias_name):
        # dim.add_alias(alias=alias_name)
        try:
            dim.add_value(value=value_name)
        except:
            pass

    # This is just a vectorized way of calling load_property
    df['added'] = list(map(load_value, df['ValueName'], df['Value'], df['NodeName']))
