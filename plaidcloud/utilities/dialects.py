"""The SQL dialect drivers every image that builds a `query.Connection` must carry.

`Connection.__init__` calls `registry.load()` on the project's datastore dialect and
raises when that driver is absent, rather than compiling Postgres SQL against a
Databend or StarRocks warehouse (sc-23700). That is the right answer, but it fires at
query time, on a customer's step, in whichever image happens to be short a driver.

The requirement is declared here, next to the code that enforces it, and every
consuming image runs it as a build step:

    python -m plaidcloud.utilities.dialects <image-name> <requirements-file>

so a mis-provisioned image fails its own build. Because the check ships in the same
wheel as the requirement, bumping the plaidcloud-utilities pin is self-verifying: an
image lacking a driver this version needs cannot be built, in whatever order the pin
bump and the driver addition happened.

Greenplum is deliberately absent. It is no longer a lakehouse engine, and the dead
default to it is where this whole defect family started (sc-23700).
"""

import sys

from sqlalchemy.dialects import registry
from sqlalchemy.exc import NoSuchModuleError

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2026, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

# Dialect name as `analyze.query.dialect()` returns it -> the pip distribution that
# registers it. Static, not derived from what happens to be installed: the failure
# mode this guards is *absence*, and a list read off the environment can never be
# violated by it. Adding an engine here is the one edit that should require review.
REQUIRED_DIALECTS = {
    'databend': 'databend-sqlalchemy',
    'databricks': 'databricks-sqlalchemy',
    'snowflake': 'snowflake-sqlalchemy',
    'starrocks': 'starrocks',
}


def unloadable_dialects():
    """Yield (dialect, pip package, error) for every required dialect this image cannot load."""
    for dialect, package in REQUIRED_DIALECTS.items():
        try:
            registry.load(dialect)
        except (NoSuchModuleError, ImportError) as exc:
            yield dialect, package, exc


def assert_dialects_available(image, requirements_file):
    """Raise unless every dialect in REQUIRED_DIALECTS loads in this interpreter.

    Args:
        image (str): the image being built, named as the error should name it.
        requirements_file (str): the requirements file that image installs — the file
            a missing driver has to be added to.
    """
    failures = list(unloadable_dialects())
    if not failures:
        return
    raise RuntimeError('\n'.join([
        f'{image}: missing {len(failures)} of {len(REQUIRED_DIALECTS)} SQL dialect drivers.',
        'This image cannot compile SQL for every warehouse PlaidCloud supports, so any',
        'project on one of the engines below dies at plaidcloud.utilities.query.Connection()',
        '— at query time, on the customer, with the image deployed green (sc-23700).',
        '',
        f'FIX: add the pip package(s) below to {requirements_file}, then rebuild {image}.',
        '',
        *(f'  dialect {dialect!r}  ->  pip package {package!r}   [{exc}]'
          for dialect, package, exc in failures),
        '',
        'The required set is plaidcloud.utilities.dialects.REQUIRED_DIALECTS — it travels',
        'with the plaidcloud-utilities pin, so this is a driver gap in the image, never a',
        'reason to relax the check.',
    ]))


if __name__ == '__main__':
    try:
        assert_dialects_available(sys.argv[1], sys.argv[2])
    except RuntimeError as exc:
        # SystemExit, not a propagated RuntimeError: a traceback in a Docker build log
        # buries the message, and the message is the whole point of this check.
        raise SystemExit(str(exc)) from None
    print(f'{sys.argv[1]}: all SQL dialect drivers load ({", ".join(REQUIRED_DIALECTS)})')
