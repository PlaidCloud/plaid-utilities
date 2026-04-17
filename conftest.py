
import importlib.util
import platform

collect_ignore = ["setup.py"]
collect_ignore.append('plaidcloud/utilities/tests/test_remote_dimension.py')

# xlwings is only listed as a Windows dependency in requirements.txt. Skip the
# doctest-module collection for xlwings_utility anywhere it cannot be imported
# so CI (Linux) and local dev (macOS) don't fail on collection.
if platform.system() != "Windows" or importlib.util.find_spec("xlwings") is None:
    collect_ignore.append("plaidcloud/utilities/xlwings_utility.py")
