# coding=utf-8
"""Tests for plaidcloud.utilities.pretty_size."""

import unittest

from plaidcloud.utilities.pretty_size import pretty_size, pretty_size_disk


class TestPrettySize(unittest.TestCase):

    def test_bytes_under_kilobyte(self):
        self.assertEqual(pretty_size(0), '0.0bytes')
        self.assertEqual(pretty_size(512), '512.0bytes')
        self.assertEqual(pretty_size(1023), '1023.0bytes')

    def test_kilobytes(self):
        self.assertEqual(pretty_size(1024), '1.0KB')
        self.assertEqual(pretty_size(2048), '2.0KB')

    def test_megabytes(self):
        self.assertEqual(pretty_size(1024 * 1024), '1.0MB')

    def test_gigabytes(self):
        self.assertEqual(pretty_size(5368709120), '5.0GB')

    def test_petabytes_fallback(self):
        # Beyond terabytes falls through to PB.
        huge = 1024 ** 5
        self.assertEqual(pretty_size(huge), '1.0PB')

    def test_custom_divisor(self):
        self.assertEqual(pretty_size(5368709120, 100.0), '53.7TB')

    def test_negative_values(self):
        self.assertEqual(pretty_size(-1023), '-1023.0bytes')
        self.assertEqual(pretty_size(-5368709120, 100.0), '-53.7TB')


class TestPrettySizeDisk(unittest.TestCase):

    def test_uses_1000_divisor(self):
        self.assertEqual(pretty_size_disk(999), '999.0bytes')
        self.assertEqual(pretty_size_disk(1000), '1.0KB')
        self.assertEqual(pretty_size_disk(1024), '1.0KB')
        self.assertEqual(pretty_size_disk(1_000_000), '1.0MB')


if __name__ == '__main__':
    unittest.main()
