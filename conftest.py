
import platform

collect_ignore = ["setup.py"]
collect_ignore.append('plaidcloud/utilities/tests/test_remote_dimension.py')
# --doctest-modules imports every module for collection, and because plaidcloud/
# is a namespace package with no __init__.py, pytest imports this one a SECOND
# time under the rootdir-relative name 'utilities.sqlalchemy_functions'. The
# duplicate re-registers the GenericFunctions, so sqlalchemy.func.import_col()
# builds the DUPLICATE's class whose @compiles handler reads the duplicate
# module's typed_staging ContextVar while callers set the canonical one — the
# flag silently no-ops (split-brain). The module has zero doctests, so skipping
# doctest collection loses nothing.
collect_ignore.append('plaidcloud/utilities/sqlalchemy_functions.py')

if platform.system() == "Linux":
    collect_ignore.append("plaidcloud/utilities/xlwings_utility.py")
