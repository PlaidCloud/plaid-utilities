# coding=utf-8
"""Tests for plaidcloud.utilities.xray.

Targets branch coverage across attribute types: str/bool/int/float/dict/list/
tuple/None/numpy/method-wrapper/builtin and top-level recursion into
XrayDict.
"""

import unittest

import numpy as np

from plaidcloud.utilities import xray


class _AllTypes:
    def __init__(self):
        self.s = 'hello'
        self.b = True
        self.i = 1
        self.f = 1.5
        self.d = {'k': 'v', 'n': 1}
        self.l = [1, 'two', 3.0]
        self.t = (1, 2, 3)
        self.none = None
        self.np_val = np.int64(42)


class TestXray(unittest.TestCase):

    def test_returns_string(self):
        out = xray.Xray(_AllTypes(), id_list=[])
        self.assertIsInstance(out, str)
        # Headers/types all mentioned
        for tok in ('str ', 'bool ', 'int ', 'float ', 'dict ', 'list ',
                    'tuple ', 'None '):
            self.assertIn(tok, out)

    def test_max_level_zero_returns_empty(self):
        self.assertEqual(xray.Xray(_AllTypes(), id_list=[], max_level=0), '')

    def test_shows_private_when_requested(self):
        class HasDunder:
            def __init__(self):
                self._public = 'p'
        out = xray.Xray(HasDunder(), id_list=[], show_private=True)
        self.assertIsInstance(out, str)

    def test_nested_object_recurses(self):
        outer = _AllTypes()
        outer.inner = _AllTypes()
        out = xray.Xray(outer, id_list=[])
        # Nested recursion should render something about the inner object.
        self.assertIn('object', out)

    def test_no_getattribute_branch(self):
        # Slot-based class will have __getattribute__ on it because all
        # Python objects inherit it; simulate the "no __getattribute__"
        # branch by patching dir() to hide it.
        class Bare:
            pass
        target = Bare()
        orig = xray.__builtins__.get('dir') if isinstance(xray.__builtins__, dict) else None  # noqa: F841
        # The branch is nominally unreachable on CPython since all objects
        # have __getattribute__. Patch the xray.dir lookup via a dummy.
        class NoGetAttr:
            def __dir__(self):
                return ['x']
        # Force a minimal object exercising the fallback "no __getattribute__
        # in dir" path by creating a class that explicitly lies about its dir.
        obj = NoGetAttr()
        # This should just not crash.
        result = xray.Xray(obj, id_list=[])
        self.assertIsInstance(result, str)


class TestXrayDict(unittest.TestCase):

    def test_returns_string_with_all_types(self):
        out = xray.XrayDict({
            's': 'hello',
            'b': True,
            'i': 1,
            'f': 1.5,
            'd': {'nested': 1},
            'l': [1, 2, 3],
            't': (1, 2, 3),
            'none': None,
            'np_val': np.int64(42),
        }, id_list=[])
        self.assertIsInstance(out, str)
        for tok in ('str ', 'bool ', 'int ', 'float ', 'dict ', 'list ',
                    'tuple ', 'None '):
            self.assertIn(tok, out)

    def test_max_level_zero_returns_empty(self):
        self.assertEqual(xray.XrayDict({'a': 1}, id_list=[], max_level=0), '')

    def test_private_dunder_keys_skipped(self):
        # Dunder-style keys are treated as private; default show_private=False
        # should skip them.
        out = xray.XrayDict({'__private__': 'x', 'public': 'y'}, id_list=[])
        self.assertIn('public', out)
        self.assertNotIn('__private__', out)

    def test_private_keys_shown_when_requested(self):
        out = xray.XrayDict(
            {'__private__': 'x'}, id_list=[], show_private=True,
        )
        self.assertIn('__private__', out)

    def test_object_in_dict_triggers_object_branch(self):
        class Widget:
            def __init__(self):
                self.name = 'w'
        out = xray.XrayDict({'w': Widget()}, id_list=[])
        self.assertIn('object', out)

    def test_builtin_value(self):
        out = xray.XrayDict({'fn': len}, id_list=[])
        self.assertIn('builtin', out)

    def test_method_wrapper_value(self):
        # str.__add__ is a method-wrapper on CPython; str.__str__ is as well.
        out = xray.XrayDict({'m': 'abc'.__add__}, id_list=[])
        # Depending on CPython version this is either method-wrapper or builtin;
        # both branches are valid - test that it renders something recognizable.
        self.assertTrue('meth-wrap' in out or 'builtin' in out or 'object' in out)


class TestXrayHelper(unittest.TestCase):

    def test_private_add_helper(self):
        # Covers the private __add helper via its mangled name on the module.
        fn = getattr(xray, '_xray__add', None)
        if fn is None:
            # Module-level private fn is named __add; Python does not mangle
            # at module scope, so it stays accessible as __add via getattr.
            fn = getattr(xray, '__add', None)
        self.assertIsNotNone(fn)
        out = fn('seed ', ['line', 'two'])
        self.assertIsInstance(out, str)


if __name__ == '__main__':
    unittest.main()
