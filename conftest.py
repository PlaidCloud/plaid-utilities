
import platform

collect_ignore = ["setup.py"]

if platform.system() == "Linux":
    collect_ignore.append("plaidcloud/utilities/xlwings_utility.py")
