
import platform

collect_ignore = ["setup.py"]
collect_ignore.append('plaidcloud/tests/test_remote_dimension.py')

if platform.system() == "Linux":
    collect_ignore.append("plaidcloud/utilities/xlwings_utility.py")
