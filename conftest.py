
import platform

collect_ignore = ["setup.py"]

if platform.system() == "Linux":
    collect_ignore.append("plaidtools/xlwings_utility.py")
