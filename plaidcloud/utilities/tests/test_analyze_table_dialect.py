# coding=utf-8
"""sc-23700: `compiled` must refuse to guess a datastore dialect.

It used to fall back to 'greenplum' — an engine no tenant runs — so a caller
that lost the dialect got SQL for an engine that cannot exist instead of an
error.
"""

import unittest

import sqlalchemy

from plaidcloud.utilities.analyze_table import compiled

__author__ = 'Paul Morel <paul@plaidcloud.com>'
__copyright__ = '© Copyright 2026, PlaidCloud, Inc.'
__license__ = 'Apache 2.0'


class TestCompiledRequiresDialect(unittest.TestCase):

    def setUp(self):
        self.query = sqlalchemy.select(sqlalchemy.literal(1))

    def test_missing_dialect_raises(self):
        for missing in (None, ''):
            with self.subTest(dialect=missing), self.assertRaisesRegex(ValueError, 'dialect'):
                compiled(self.query, missing)

    def test_omitted_dialect_raises(self):
        with self.assertRaisesRegex(ValueError, 'dialect'):
            compiled(self.query)
