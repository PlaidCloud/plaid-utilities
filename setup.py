from setuptools import setup

test_deps = [
    'pytest',
    'mock',
    'minimock',
    'pytest-cov',
    'pytest-runner',
    'hdbcli',
    'pyhdb',
]

extras = {
    'test': test_deps
}

try:
    import pygit2
    import os
    import datetime
    repo = pygit2.Repository(os.getcwd())
    commit_hash = repo.head.target
    commit = repo[commit_hash]
    print('commit {}'.format(commit_hash))
    print(datetime.date.fromtimestamp(commit.commit_time))
    print(commit.message)
except ImportError:
    print('pygit2 is not available. Cannot detect current commit.')
except:
    print('This is probably not a repo, a copy of the code.')

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

setup(
    name='plaidcloud-utilities',
    version="1.0.1",
    author='Michael Rea',
    author_email='michael.rea@tartansolutions.com',
    packages=['plaidcloud.utilities', 'plaidcloud.utilities.remote'],
    install_requires=[
        'argparse',
        'chardet',
        'ipython;python_version>"2.7"',
        'messytables==0.15.2',
        'minimock',
        'numpy;python_version>="3.5"',
        'numpy<=1.16.5;python_version<"3.5"',
        'pandas',
        'plaidcloud-rpc',
        'requests',
        'orjson',
        'six',
        'sqlalchemy',
        'sqlalchemy-hana',
        'sqlalchemy-greenplum',
        'tables',
        'texttable',
        'toolz==0.10.0',
        'unicodecsv',
        'xlrd',
        'xlwings;platform_system=="Windows"',
        'pyyaml',
    ],
    tests_require=test_deps,
    setup_requires=['pytest-runner'],
    extras_require=extras,
    dependency_links=[
        'git+git://github.com/PlaidCloud/plaid-rpc.git@master#egg=plaidcloud-rpc'
    ],
    long_description=long_description,
    long_description_content_type='text/markdown',
)
