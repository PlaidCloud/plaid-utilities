#!/usr/bin/env python
# coding=utf-8

from __future__ import absolute_import
import re

__author__ = "Michael Rea"
__copyright__ = "© Copyright 2018, Tartan Solutions, Inc"
__credits__ = ["Michael Rea", "Silvio Santos"]
__license__ = "Proprietary"
__maintainer__ = "Silvio Santos"
__email__ = "silvio.santos@tartansolutions.com"


def multiply(thingOne, thingTwo):
    """
    This is just an example of a function with plaid's sanctioned docstring format and extra
    commentary for illustrative purposes.

    Args:
        thingOne (float): Just a number
        thingTwo (float): Just another number

    Returns:
        answer (float): Product of thingOne * thingTwo

    Examples:
        >>> multiply(5,3)
        15
        >>> multiply(5,6)
        30
    """

    answer = thingOne * thingTwo
    return answer
