
from plaidcloud.utilities.udf_utility_loader import load_utility_scripts, validate_utility_script
from plaidcloud.utilities import udf_helpers

# Imported for its side effect: `dialects` points the `starrocks` name at the
# subclass that keeps `CAST(… AS FLOAT)` in the compiled SQL. It is armed here, at
# the package root, so that *any* use of this library has it — `query.Connection`
# builds its dialect from the registry, and a consumer that had to remember to
# import something first is precisely the defect this replaces. `dialects` imports
# only sqlalchemy, already a hard dependency, and the registration itself is lazy.
from plaidcloud.utilities import dialects  # noqa: F401

__all__ = ["load_utility_scripts", "validate_utility_script", "udf_helpers"]
