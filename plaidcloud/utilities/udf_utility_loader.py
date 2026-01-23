import ast
import sys
import importlib.util

DEFAULT_NAMESPACE: str = "plaidcloud.utilities.udf_helpers"

ALLOWED_TOP_LEVEL_NODES = (
    ast.FunctionDef,
    ast.AsyncFunctionDef,
    ast.ClassDef,
    ast.Import,
    ast.ImportFrom,
    ast.Assign,
    ast.AnnAssign,
)

class UtilityScriptValidationError(ValueError):
    pass

def load_utility_scripts(
    scripts: dict[str, str],
    *,
    validate: bool = False,
    reload: bool = True,
):
    """
    Load utility scripts into plaidcloud.utilities.udf.{script_name}

    Args:
        scripts: dict of {module_name: code}
        reload: whether to reload modules if already present
    """
    namespace = DEFAULT_NAMESPACE  # Just for now, I don't think it makes sense to allow others at this time
    base_module = sys.modules.get(namespace)

    if base_module is None:
        raise ImportError(f"Namespace {namespace} is not available")

    for module_name, code in scripts.items():
        if validate:
            validate_utility_script(code)

        full_name = f"{namespace}.{module_name}"

        if full_name in sys.modules and not reload:
            continue

        spec = importlib.util.spec_from_loader(full_name, loader=None)
        module = importlib.util.module_from_spec(spec)

        # module.__file__ = url
        module.__package__ = namespace

        exec(code, module.__dict__)

        sys.modules[full_name] = module
        setattr(base_module, module_name, module)


def _is_constant_expr(node: ast.AST) -> bool:
    """Return True if the AST node is a compile-time constant."""
    if isinstance(node, ast.Constant):
        return True

    if isinstance(node, (ast.Tuple, ast.List, ast.Set)):
        return all(_is_constant_expr(elt) for elt in node.elts)

    if isinstance(node, ast.Dict):
        return all(
            _is_constant_expr(k) and _is_constant_expr(v)
            for k, v in zip(node.keys, node.values)
        )

    return False


def validate_utility_script(code: str) -> None:
    """
    Validate that a utility script contains no executable top-level code.

    Raises UtilityScriptValidationError on failure.
    """
    try:
        tree = ast.parse(code)
    except SyntaxError as e:
        raise UtilityScriptValidationError(f"Syntax error: {e}") from e

    for i, node in enumerate(tree.body):
        # Allow module docstring ONLY
        if (
            i == 0
            and isinstance(node, ast.Expr)
            and isinstance(node.value, ast.Constant)
            and isinstance(node.value.value, str)
        ):
            continue


        # ❌ Disallow all other expressions (blocks function calls, etc.)
        if isinstance(node, ast.Expr):
            raise UtilityScriptValidationError(
                f"Executable expression not allowed at top level "
                f"(line {node.lineno})"
            )

        # ❌ Disallow decorators (execute at import time)
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            if node.decorator_list:
                raise UtilityScriptValidationError(
                    f"Decorators are not allowed "
                    f"(line {node.lineno})"
                )

        # ✅ Function / class definitions
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue

        # # ✅ Imports (optional allowlist)
        # if isinstance(node, (ast.Import, ast.ImportFrom)):
        #     if allowed_imports is not None:
        #         for alias in node.names:
        #             root = alias.name.split(".")[0]
        #             if root not in allowed_imports:
        #                 raise UtilityScriptValidationError(
        #                     f"Import '{alias.name}' is not allowed "
        #                     f"(line {node.lineno})"
        #                 )
        #     continue

        # ✅ Constant-only assignments
        if isinstance(node, ast.Assign):
            for target in node.targets:
                if not isinstance(target, ast.Name):
                    raise UtilityScriptValidationError(
                        f"Only simple name assignments allowed "
                        f"(line {node.lineno})"
                    )

            if not _is_constant_expr(node.value):
                raise UtilityScriptValidationError(
                    f"Top-level assignments must be constants "
                    f"(line {node.lineno})"
                )
            continue

        if isinstance(node, ast.AnnAssign):
            if node.value and not _is_constant_expr(node.value):
                raise UtilityScriptValidationError(
                    f"Annotated assignment must be constant "
                    f"(line {node.lineno})"
                )
            continue

        # ❌ Everything else is disallowed
        raise UtilityScriptValidationError(
            f"Disallowed top-level statement: {type(node).__name__} "
            f"(line {node.lineno})"
        )
