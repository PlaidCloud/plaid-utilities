#!/usr/bin/env python
# coding=utf-8
"""
Creates a unique SHA1 hash
"""

import hashlib
import random
import string
import time

__author__ = "Paul Morel"
__copyright__ = "© Copyright 2010-2011, Tartan Solutions, Inc"
__credits__ = ["Paul Morel"]
__license__ = "Apache 2.0"
__maintainer__ = "Paul Morel"
__email__ = "paul.morel@tartansolutions.com"


def get_random(prefix="", length=15, sequence=string.printable[:62], not_used=None):
    """Generates a completely random string using a sequence.

    The default sequence comprises the numbers and letters

    Args:
        prefix (str, optional): An optional string to prefix the generated string with
        length (int, optional): The length of the random string to generate. Default 15
        sequence (str, optional): What characters to use for the random string. Defaults to `string.printable[:62]`

    Returns:
        str: A randomly generated string

    Examples:
        >>> from minimock import mock
        >>> mock('random.SystemRandom.choice', tracker=None, returns='x')
        >>> get_random('test', 15, 'somestring') == 'testxxxxxxxxxxxxxxx'
        True
    """
    rng = random.SystemRandom()
    random_hash = ''.join(rng.choice(sequence) for _ in range(length))
    return str(''.join((prefix, random_hash)))


class Hasher(object):
    """SHA1 Hash Creation Class"""

    def __init__(self):
        self.__string_padding = '@@!jasoibvh853jvnmp23inbbw043'

    def _prepare_data(self, data):
        """Prepares data so that it is consistently a list of strings suitable for the .join()

        Args:
            data (str, dict, or None): The data to clean

        Returns:
            :type:`list` of :type:`str`: The data as a list of strings

        Examples:
            >>> h = Hasher()
            >>> h._prepare_data('test')
            ['test', 't', 'e', 's', 't']
            >>> sorted(h._prepare_data({"thing": "one", "another_thing": None, "yet_another": 4}))
            ['another_thing', 'thing', 'yet_another']
        """
        final_data = []
        if isinstance(data, str):
            final_data.append(data)

        for d in data:
            final_data.append(str(d))

        return final_data

    def get(self, data=[]):
        """Creates a new hash based on the input data, string padding, and a timestamp.

        Args:
            data (str, dict, list, or None, optional): The data to create a hash of. Defaults to an empty list

        Returns:
            str: The sha1 hash of the data

        Examples:
            >>> h = Hasher()
            >>> isinstance(h.get('blah'), str)
            True
        """

        final_data = self._prepare_data(data)
        final_data.append(str(time.time()))
        final_data.append(self.__string_padding)

        string_to_hash = "".join(final_data)
        return hashlib.sha1(string_to_hash.encode("latin-1")).hexdigest()

    def get_consistent(self, data=[]):
        """Creates a new hash based on the input data and some string padding.

        Args:
            data: (str, dict, list, or None, optional): The data to generate a hash for. Defaults to an empty list.

        Returns:
            str: The generated hash

        Examples:
            >>> h = Hasher()
            >>> h.get_consistent('blah')
            '3ff98ae89914f78eed6da40b27a9b1875cf282e0'
        """
        final_data = self._prepare_data(data)
        final_data.append(self.__string_padding)
        string_to_hash = "".join(final_data)
        return hashlib.sha1(string_to_hash.encode("latin-1")).hexdigest()

    def get_clean(self, data=[]):
        """Creates a new hash based on the input data only.

        Args:
            data: (str, dict, list, or None, optional): The data to generate a hash for. Defaults to an empty list.

        Returns:
            str: The generated hash

        Examples:
            >>> h = Hasher()
            >>> h.get_clean('blah')
            'd3395867d05cc4c27f013d6e6f48d644e96d8241'
        """
        final_data = self._prepare_data(data)
        string_to_hash = "".join(final_data)
        return hashlib.sha1(string_to_hash.encode("latin-1")).hexdigest()
