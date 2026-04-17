# coding=utf-8
"""Tests for plaidcloud.utilities.stringtransforms."""

import unittest

from plaidcloud.utilities import stringtransforms


class TestReplaceTags(unittest.TestCase):

    def test_single_tag_replacement(self):
        result = stringtransforms.replaceTags('hello [name]', {'name': 'world'})
        self.assertEqual(result, 'hello world')

    def test_multiple_tags_replacement(self):
        data = {'a': 'AAA', 'b': 'BBB'}
        result = stringtransforms.replaceTags('aye [a] and bee [b]', data)
        self.assertEqual(result, 'aye AAA and bee BBB')

    def test_missing_tag_is_unchanged(self):
        result = stringtransforms.replaceTags('hi [missing]', {'other': 'x'})
        self.assertEqual(result, 'hi [missing]')

    def test_no_tags(self):
        self.assertEqual(stringtransforms.replaceTags('plain', {'a': 'A'}), 'plain')


class TestApplyVariables(unittest.TestCase):

    def test_none_message_returns_none(self):
        self.assertIsNone(stringtransforms.apply_variables(None))

    def test_empty_string_returns_empty(self):
        self.assertEqual(stringtransforms.apply_variables(''), '')

    def test_positional_tokens_stripped(self):
        # {} (positional) must be removed before formatting
        self.assertEqual(
            stringtransforms.apply_variables('foo{}bar'),
            'foobar',
        )

    def test_basic_substitution(self):
        out = stringtransforms.apply_variables(
            'hello {name}',
            variables={'name': 'plaid'},
        )
        self.assertEqual(out, 'hello plaid')

    def test_strict_raises_on_missing_key(self):
        with self.assertRaises(Exception):
            stringtransforms.apply_variables(
                'hello {missing}',
                variables={},
                strict=True,
            )

    def test_non_strict_replaces_missing_with_empty_string(self):
        out = stringtransforms.apply_variables(
            'hello {missing}!',
            variables={},
            strict=False,
        )
        self.assertEqual(out, 'hello !')

    def test_non_strict_calls_error_handler(self):
        errors = []
        stringtransforms.apply_variables(
            'hi {x}',
            variables={},
            strict=False,
            nonstrict_error_handler=errors.append,
        )
        self.assertEqual(len(errors), 1)
        self.assertIn('x', errors[0])


class TestApplyVariablesParseError(unittest.TestCase):

    def test_malformed_format_string_raises_wrapped_exception(self):
        # A lone '{' without a closing '}' raises in Formatter.parse().
        with self.assertRaisesRegex(Exception, 'Error trying to apply variables'):
            stringtransforms.apply_variables('hello {world')


if __name__ == '__main__':
    unittest.main()
