# coding=utf-8

import unittest
import time

__author__ = "Kellen Kapper"
__copyright__ = "© Copyright 2018-2021, Tartan Solutions, Inc"
__credits__ = ["Kellen Kapper"]
__license__ = "Apache 2.0"
__maintainer__ = "Kellen Kapper"
__email__ = "kellen.kapperl@tartansolutions.com"

from plaidcloud.utilities.hasher import Hasher


class TestHasher(unittest.TestCase):

    def setUp(self):
        """Constructs a test environment if necessary"""
        pass

    def test_object_instantiation(self):
        obj = Hasher()
        self.assertTrue(isinstance(obj, Hasher))

    def test_prepare_data_retains_all_elements(self):
        obj = Hasher()
        test_dictionary = {"Alpha": "1", "Bravo": "2", "Charlie": "3"}
        key_list = obj._prepare_data(test_dictionary)
        self.assertTrue(all(string in key_list for string in ["Alpha", "Bravo", "Charlie"]))

    def test_prepare_data_sorts_elements_correctly(self):
        obj = Hasher()
        test_dictionary = {"Alpha": "1" , "Charlie": "2", "Bravo": "3"}
        key_list = obj._prepare_data(test_dictionary)
        self.assertEqual(sorted(key_list) , ["Alpha", "Bravo", "Charlie"])

    # This should fail, this is passing the "string_types" check for some reason.
    def test_prepare_data_only_works_on_strings(self):
        obj = Hasher()
        test_dictionary = {1: 1, 2: 2, 3: 3}
        clean_data = obj._prepare_data(test_dictionary)
        self.assertEqual(clean_data, ['1', '2', '3'])

    def test_getting_dynamic_hash(self):
        # We use time for dynamic hashing, this ensures two requests in rapid succession still have different hashes.
        # Currently this uses only the most recent second, and this test will fail; leaving it for now.
        obj = Hasher()
        test_dictionary = {"Alpha": "1" , "Charlie": "2", "Bravo": "3"}
        start = str(time.time())
        first_hash = obj.get(test_dictionary)
        while start == str(time.time()):
            time.sleep(0.1)
        second_hash = obj.get(test_dictionary)

        self.assertTrue(first_hash != second_hash)

    def test_getting_consistent_hash(self):
        obj = Hasher()
        test_dictionary = {"Alpha": "1", "Charlie": "2", "Bravo": "3"}
        first_hash = obj.get_consistent(test_dictionary)
        second_hash = obj.get_consistent(test_dictionary)

        self.assertTrue(first_hash == second_hash)


if __name__ == '__main__':
    unittest.TestProgram()
