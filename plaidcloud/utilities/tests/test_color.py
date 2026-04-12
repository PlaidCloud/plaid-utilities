# coding=utf-8
"""Tests for plaidcloud.utilities.color."""

import unittest

from plaidcloud.utilities import color


class TestColorToHex(unittest.TestCase):

    def test_named_color_returns_hex(self):
        self.assertEqual(color.colorToHex('yellow'), '#ffff00')
        self.assertEqual(color.colorToHex('red'), '#ff0000')

    def test_hex_input_is_lowercased(self):
        self.assertEqual(color.colorToHex('#0000AA'), '#0000aa')

    def test_unknown_color_returns_default(self):
        self.assertEqual(color.colorToHex('not a valid color'), '#330000')

    def test_non_string_returns_empty(self):
        self.assertEqual(color.colorToHex(None), '')
        self.assertEqual(color.colorToHex(123), '')


class TestHTMLColorToRGB(unittest.TestCase):

    def test_valid_hex_returns_tuple(self):
        self.assertEqual(color.HTMLColorToRGB('#ffff00'), (255, 255, 0))
        # Works without leading #
        self.assertEqual(color.HTMLColorToRGB('ffff00'), (255, 255, 0))

    def test_malformed_raises_valueerror(self):
        with self.assertRaises(ValueError):
            color.HTMLColorToRGB('ffff00xxx')


class TestRGBToFloat(unittest.TestCase):

    def test_black(self):
        self.assertEqual(color.RGBToFloat((0, 0, 0)), (0.0, 0.0, 0.0))

    def test_white(self):
        self.assertEqual(color.RGBToFloat((255, 255, 255)), (1.0, 1.0, 1.0))

    def test_midpoint(self):
        r, g, b = color.RGBToFloat((128, 128, 128))
        self.assertAlmostEqual(r, 128 / 255.0)
        self.assertAlmostEqual(g, 128 / 255.0)
        self.assertAlmostEqual(b, 128 / 255.0)


class TestProcessColor(unittest.TestCase):

    def test_named_color_returns_float_tuple(self):
        self.assertEqual(color.processColor('yellow'), (1.0, 1.0, 0.0))

    def test_named_color_is_case_insensitive(self):
        self.assertEqual(color.processColor('Yellow'), (1.0, 1.0, 0.0))

    def test_hex_input_returns_float_tuple(self):
        self.assertEqual(color.processColor('#ffff00'), (1.0, 1.0, 0.0))

    def test_int_tuple_returns_float_tuple(self):
        self.assertEqual(color.processColor((255, 255, 0)), (1.0, 1.0, 0.0))

    def test_float_tuple_passthrough(self):
        self.assertEqual(color.processColor((1.0, 1.0, 0.0)), (1.0, 1.0, 0.0))


class TestShorthand(unittest.TestCase):

    def test_rgb_shorthand_matches_html_color_to_float(self):
        self.assertEqual(color.rgb('#ffff00'), color.HTMLColorToFloat('#ffff00'))


if __name__ == '__main__':
    unittest.main()
