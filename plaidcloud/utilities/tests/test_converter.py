# coding=utf-8
"""Tests for plaidcloud.utilities.converter."""

import unittest

from plaidcloud.utilities import converter


class TestCamelUnderscore(unittest.TestCase):

    def test_camel_to_underscore(self):
        self.assertEqual(converter.camelToUnderscore('valueOne'), 'value_one')
        self.assertEqual(
            converter.camelToUnderscore('valueTwoAndThree'),
            'value_two_and_three',
        )

    def test_underscore_to_camel(self):
        self.assertEqual(converter.underscoreToCamel('value_one'), 'valueOne')
        self.assertEqual(
            converter.underscoreToCamel('value_two_and_three'),
            'valueTwoAndThree',
        )

    def test_short_aliases(self):
        self.assertEqual(converter.j2p('valueOne'), 'value_one')
        self.assertEqual(converter.p2j('value_one'), 'valueOne')

    def test_roundtrip(self):
        original = 'value_two_and_three'
        roundtrip = converter.j2p(converter.p2j(original))
        self.assertEqual(roundtrip, original)

    def test_convert_unknown_direction_passthrough(self):
        # When the direction is unrecognized, the input string is returned unchanged.
        self.assertEqual(
            converter.convert('something', 'bogus_direction'),
            'something',
        )


class _Target:
    pass


class TestSetattrs(unittest.TestCase):

    def test_attributes_get_set_and_renamed_to_python_style(self):
        target = _Target()
        converter.setattrs(target, {'someKey': 'v', 'otherKey': 42})
        self.assertEqual(target.some_key, 'v')
        self.assertEqual(target.other_key, 42)


class TestSetSettings(unittest.TestCase):

    def test_override_wins_over_default(self):
        target = _Target()
        converter.setSettings(target, {'a': 'A'}, {'a': 'B', 'b': 'B'})
        self.assertEqual(target.a, 'A')
        self.assertEqual(target.b, 'B')

    def test_default_fills_missing(self):
        target = _Target()
        converter.setSettings(target, {}, {'a': 'default_a'})
        self.assertEqual(target.a, 'default_a')


if __name__ == '__main__':
    unittest.main()
