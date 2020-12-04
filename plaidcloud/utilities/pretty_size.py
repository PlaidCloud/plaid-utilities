#!/usr/bin/env python
# coding=utf-8

"""Utility function for pretty display of file sizes"""

__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel'
__copyright__ = '© Copyright 2014 Tartan Solutions, Inc.'
__license__ = 'Apache 2.0'


def pretty_size(num, divisor=1024.0):
    """Given a number of bytes, determines the correct suffix and formats it

    Args:
        num (float): The number of bytes
        divisor (int, optional): The cutoff before the next suffix is used

    Returns:
        str: `num` formatted with a suffix
    Examples:
        >>> pretty_size(1023)
        '1023.0bytes'
        >>> pretty_size(1024)
        '1.0KB'
        >>> pretty_size(5368709120)
        '5.0GB'
        >>> pretty_size(5368709120, 100.0)
        '53.7TB'
        >>> pretty_size(-5368709120, 100.0)
        '-53.7TB'
    """
    for x in ('bytes', 'KB', 'MB', 'GB', 'TB'):
        if divisor > num > -divisor:
            return "%3.1f%s" % (num, x)
        num /= divisor
    return "%3.1f%s" % (num, 'PB')


def pretty_size_disk(num):
    """Uses a divisor of 1000 Bytes instead of 1024 to align with disk manufacturers

    Args:
        num (float): The number of bytes

    Returns:
        str: `num` formatted with a suffix
    Examples:
        >>> pretty_size_disk(999)
        '999.0bytes'
        >>> pretty_size_disk(1000)
        '1.0KB'
        >>> pretty_size_disk(1024)
        '1.0KB'
    """

    return pretty_size(num, 1000.0)
