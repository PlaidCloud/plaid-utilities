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


setup(
    name='plaidcloud-utilities',
    author='Michael Rea',
    author_email='michael.rea@tartansolutions.com',
    packages=['plaidcloud.utilities', 'plaidcloud.utilities.remote'],
    install_requires=[
        'argparse',
        'chardet',
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
        'texttable',
        'toolz==0.10.0',
        'unicodecsv',
        'xlrd',
        'xlwings;platform_system=="Windows"',
    ],
    tests_require=test_deps,
    setup_requires=['pytest-runner'],
    extras_require=extras,
    dependency_links=[
        # 'https://github.com/PlaidCloud/sqlalchemy-greenplum/tarball/master#egg=sqlalchemy-greenplum-0.0.1',
        'file:///usr/sap/hdbclient/hdbcli-2.2.36.tar.gz#egg=hdbcli'
    ],
)
