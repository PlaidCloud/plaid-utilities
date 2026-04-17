# coding=utf-8
"""Tests for plaidcloud.utilities.udf_utility_loader."""

import sys
import unittest

from plaidcloud.utilities import udf_utility_loader
from plaidcloud.utilities.udf_utility_loader import (
    DEFAULT_NAMESPACE,
    UtilityScriptValidationError,
    load_utility_scripts,
    validate_utility_script,
)


class TestValidateUtilityScript(unittest.TestCase):

    def test_accepts_function_definition(self):
        validate_utility_script('def foo():\n    return 1\n')

    def test_accepts_class_definition(self):
        validate_utility_script('class Foo:\n    pass\n')

    def test_accepts_imports(self):
        validate_utility_script('import os\nfrom collections import OrderedDict\n')

    def test_accepts_module_docstring(self):
        validate_utility_script('"""Hello, module."""\ndef foo():\n    pass\n')

    def test_accepts_constant_assignment(self):
        validate_utility_script("CONST = 'value'\n")
        validate_utility_script("NUMS = [1, 2, 3]\n")
        validate_utility_script("MAPPING = {'a': 1}\n")

    def test_accepts_constant_annotated_assignment(self):
        validate_utility_script("CONST: str = 'value'\n")

    def test_rejects_top_level_expression(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('print("hi")\n')

    def test_rejects_decorators(self):
        code = '@cache\ndef foo():\n    return 1\n'
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script(code)

    def test_rejects_non_constant_assignment(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('X = some_func()\n')

    def test_rejects_non_constant_annotated_assignment(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('X: int = some_func()\n')

    def test_rejects_tuple_unpacking(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('a, b = 1, 2\n')

    def test_rejects_syntax_error(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('def bad(:\n')

    def test_rejects_while_loop(self):
        with self.assertRaises(UtilityScriptValidationError):
            validate_utility_script('while True:\n    pass\n')


class TestLoadUtilityScripts(unittest.TestCase):

    def setUp(self):
        # Ensure the namespace exists (it does, but guard against side effects
        # left by earlier tests).
        __import__(DEFAULT_NAMESPACE)
        self._to_remove = []

    def tearDown(self):
        for mod in self._to_remove:
            sys.modules.pop(mod, None)

    def test_loads_module_and_exposes_members(self):
        name = 'test_loader_module_basic'
        self._to_remove.append(f'{DEFAULT_NAMESPACE}.{name}')
        load_utility_scripts({name: 'def add(a, b):\n    return a + b\n'})

        import plaidcloud.utilities.udf_helpers as helpers
        mod = getattr(helpers, name)
        self.assertEqual(mod.add(2, 3), 5)

    def test_reload_overwrites_previous_definition(self):
        name = 'test_loader_module_reload'
        self._to_remove.append(f'{DEFAULT_NAMESPACE}.{name}')
        load_utility_scripts({name: 'VALUE = 1\n'})
        load_utility_scripts({name: 'VALUE = 2\n'})

        import plaidcloud.utilities.udf_helpers as helpers
        self.assertEqual(getattr(helpers, name).VALUE, 2)

    def test_no_reload_keeps_previous_definition(self):
        name = 'test_loader_module_noreload'
        self._to_remove.append(f'{DEFAULT_NAMESPACE}.{name}')
        load_utility_scripts({name: 'VALUE = 1\n'})
        load_utility_scripts(
            {name: 'VALUE = 99\n'},
            reload=False,
        )

        import plaidcloud.utilities.udf_helpers as helpers
        self.assertEqual(getattr(helpers, name).VALUE, 1)

    def test_validation_errors_include_module_name(self):
        with self.assertRaises(Exception) as ctx:
            load_utility_scripts({'bad_module': 'print("hi")\n'}, validate=True)
        self.assertIn('bad_module', str(ctx.exception))

    def test_syntax_error_includes_module_name(self):
        # No validate needed - exec itself will raise, and the wrapper should
        # surface the module name in the error message.
        with self.assertRaises(Exception) as ctx:
            load_utility_scripts({'syntax_bad': 'def x(:\n'})
        self.assertIn('syntax_bad', str(ctx.exception))

    def test_missing_namespace_raises_import_error(self):
        # Temporarily hide the namespace from sys.modules.
        saved = sys.modules.pop(DEFAULT_NAMESPACE, None)
        try:
            with self.assertRaises(ImportError):
                load_utility_scripts({'x': 'def y(): pass\n'})
        finally:
            if saved is not None:
                sys.modules[DEFAULT_NAMESPACE] = saved


if __name__ == '__main__':
    unittest.main()
