#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import
import itertools

"""Function to convert a json style dict tree to a generational lod."""

__author__ = 'Adams Tower'
__copyright__ = '© Copyright 2010-2013, Tartan Solutions, Inc'
__credits__ = ['Adams Tower']
__license__ = 'Proprietary'
__maintainer__ = 'Adams Tower'
__email__ = 'adams.tower@tartansolutions.com'


def dict_to_generation(dicttree, topgeneration=0, childrenfield='children', parentname=None, parentkey=None):
    """Converts a dict to a LOD

    Args:
        dicttree (dict): The dict to make a tree from
        topgeneration (int, optional): Which "level" of the dict should be the root of the tree
            (In other words, how many levels to skip)
        childrenfield (str, optional): The key that will contain the children on any given node. Defaults to `children`
        parentname (str, optional): The name of the parent node, if any. Defaults to `None`
        parentkey (str, optional): Which field, if any contains a given node's parent node. Defaults to `None`

    Returns:
        list: A list representation of the LOD

    Examples:
        >>> tree = {
        ...         'name': 'Roich',
        ...         'children': [
        ...             {
        ...                 'name': 'Sualtam',
        ...                 'children': [
        ...                     {
        ...                         'name': 'Monstro',
        ...                         'children': [],
        ...                     }
        ...                 ],
        ...             },
        ...             {
        ...                 'name': 'Fergus',
        ...                 'children': [
        ...                     {
        ...                         'name': 'Fiachra',
        ...                         'children': [],
        ...                     },
        ...                     {
        ...                         'name': 'Buinne',
        ...                         'children': [],
        ...                     },
        ...                     {
        ...                         'name': 'Illan',
        ...                         'children': [],
        ...                     },
        ...                 ],
        ...             },
        ...         ]
        ...     }
        >>> dict_to_generation(tree, parentname='', parentkey='name') == [
        ...     {'generation': 0, 'parentname': '', 'name': 'Roich'},
        ...     {'generation': 1, 'parentname': 'Roich', 'name': 'Sualtam'},
        ...     {'generation': 2, 'parentname': 'Sualtam', 'name': 'Monstro'},
        ...     {'generation': 1, 'parentname': 'Roich', 'name': 'Fergus'},
        ...     {'generation': 2, 'parentname': 'Fergus', 'name': 'Fiachra'},
        ...     {'generation': 2, 'parentname': 'Fergus', 'name': 'Buinne'},
        ...     {'generation': 2, 'parentname': 'Fergus', 'name': 'Illan'}]
        True
    """
    if len(dicttree.get(childrenfield, [])) > 0:
        parentlod = [add_generation(dicttree, topgeneration, parentname, childrenfield)]
        childrenlod = list(itertools.chain.from_iterable([dict_to_generation(c, topgeneration+1, childrenfield, dicttree[parentkey] if parentkey is not None else None, parentkey) for c in dicttree[childrenfield]]))
        return parentlod + childrenlod
    else:
        return [add_generation(dicttree, topgeneration, parentname, childrenfield)]

#def dict_to_record(dct, columnlist, generation, parentname):
    #if parentname is None:
        #rec = [generation]
    #else:
        #rec = [generation, parentname]
    #for column in columnlist:
        #rec.append(dct.get(column, None))
    #return rec


def add_generation(dct, generation, parentname, childrenfield=None):
    """Adds a single generation to a LOD

    Args:
        dct (dict-like object): The dict containing the information about this generation
        generation (int): Which generation this is
        parentname (str): The name of this generation's parent, if any.
        childrenfield (str, optional): The dict key to this generation's children, if any.
            Defaults to `None`

    Returns:
        dict: A dict representation of this generation
    """
    d = dict(dct)
    d['generation'] = generation
    if parentname is not None:
        d['parentname'] = parentname
    if childrenfield is not None:
        del d[childrenfield]
    return d
