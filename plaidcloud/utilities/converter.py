#!/usr/bin/env python
# coding=utf-8

import re

from plaidcloud.rpc.functions import deepmerge

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2009-2018, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"

first_cap_re = re.compile('(.)([A-Z][a-z]+)')
all_cap_re = re.compile('([a-z0-9])([A-Z])')


def j2p(input_string):
    """
    Short version of camelToUnderscore

    Args:
        input_string (str): The json-style string to convert

    Returns:
        str: The converted python-style string

    Examples:
        >>> j2p('valueOne')
        'value_one'
        >>> j2p('valueTwoAndThree')
        'value_two_and_three'
    """
    result = convert(input_string, "json_to_python")
    return result


def p2j(input_string):
    """
    Short version of underscoreToCamel

    Args:
        input_string (str): The python-style string to convert

    Returns:
        str: The converted json-style string

    Examples:
        >>> p2j('value_one')
        'valueOne'
        >>> p2j('value_two_and_three')
        'valueTwoAndThree'
    """
    result = convert(input_string, "python_to_json")
    return result


def underscoreToCamel(input_string):
    """Converts Python-style variable names into Json-style attributes

    Args:
        input_string (str): The string to convert

    Returns:
        str: A Json-style version of `input_string`

    Examples:
        >>> underscoreToCamel('value_one')
        'valueOne'
        >>> underscoreToCamel('value_two_and_three')
        'valueTwoAndThree'
    """
    result = convert(input_string, "python_to_json")
    return result


def camelToUnderscore(input_string):
    """Converts Json-style attributes into Python-style variable names

    Args:
        input_string (str): The string to convert

    Returns:
        str: A Python-style version of `input_string`

    Examples:
        >>> camelToUnderscore('valueOne')
        'value_one'
        >>> camelToUnderscore('valueTwoAndThree')
        'value_two_and_three'
    """
    result = convert(input_string, "json_to_python")
    return result


def convert(input_string, direction="json_to_python"):
    """Does the actual conversion for the other methods in this module

    Args:
        input_string (str): The string to convert
        direction (str, optional): Which way to convert the string, either `json_to_python`
            or `python_to_json`. Defaults to `json_to_python`

    Returns:
        str: The converted string

    Examples:
        >>> convert('valueOne', 'json_to_python')
        'value_one'
        >>> convert('valueTwoAndThree', 'json_to_python')
        'value_two_and_three'
        >>> convert('value_one', 'python_to_json')
        'valueOne'
        >>> convert('value_two_and_three', 'python_to_json')
        'valueTwoAndThree'
    """
    if direction == "json_to_python":
        # MWR 11 Oct 2010: replaced commented code with more efficient (non-looping code)
        # http://stackoverflow.com/questions/1175208/does-the-python-standard-library-have-function-to-convert-camelcase-to-camel-case
        # prog = re.compile("([A-Z])")
        # matches = prog.finditer(input_string)
        # for match in matches:
        #     input_string = input_string.replace(match.group(0), "_" + match.group(0).lower())

        s1 = first_cap_re.sub(r'\1_\2', input_string)
        return all_cap_re.sub(r'\1_\2', s1).lower()

    elif direction == "python_to_json":
        prog = re.compile("(_)([a-z])")
        matches = prog.finditer(input_string)
        for match in matches:
            input_string = input_string.replace(match.group(0), match.group(2).upper())
    return input_string


def setattrs(target_object, input_dict):
    """Sets the attributes of `target_object` based on `input_dict`

    Args:
        target_object (object): The object to set attributes for
        input_dict (dict): A dict of attribute: value pairs to set

    Returns:
        object: `target_object` with its attributes set.

    Examples:
        >>> class TestObject(object):
        ...     def __init__(self):
        ...         pass
        >>> foo = TestObject()
        >>> foo = setattrs(foo, {"a": "A", "b": "B"})
        >>> foo.a
        'A'
        >>> foo.b
        'B'
    """
    while len(input_dict) > 0:
        an_item = input_dict.popitem()
        my_attribute_name = j2p(an_item[0])
        my_attribute_value = an_item[1]
        setattr(target_object, my_attribute_name, my_attribute_value)
    return target_object


def setSettings(target_object, input_dict, default_dict):
    """
    Add custom-defined attributes to an object, based on key-value pairs indicted by 'input_dict'
    using 'default_dict' as a backup.

    Args:
        target_object (object): The object to set the attributes for
        input_dict (dict): A dict of attributes: values to set on `target_object`
        default_dict (dict): A dict of attributes: values to default to

    Returns:
        object: `target_object` with the attributes set

    Examples:
        >>> class TestObject(object):
        ...     def __init__(self):
        ...         pass
        >>> foo = TestObject()
        >>> foo = setSettings(foo, {'a': 'A'}, {'a': 'B', 'b': 'B'})
        >>> foo.a
        'A'
        >>> foo.b
        'B'
    """
    input_dict = deepmerge(default_dict, input_dict)
    target_object = setattrs(target_object, input_dict)
    return target_object
