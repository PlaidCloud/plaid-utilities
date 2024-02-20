from setuptools import setup

test_deps = [
    'pytest',
    'mock',
    'minimock',
    'pytest-cov',
    'pytest-runner',
    'starrocks'
]

extras = {
    'test': test_deps
}

# Commenting out, seems unnecessary these days
# try:
#     import pygit2
#     import os
#     import datetime
#     repo = pygit2.Repository(os.getcwd())
#     commit_hash = repo.head.target
#     commit = repo[commit_hash]
#     print('commit {}'.format(commit_hash))
#     print(datetime.date.fromtimestamp(commit.commit_time))
#     print(commit.message)
# except ImportError:
#     print('pygit2 is not available. Cannot detect current commit.')
# except:
#     print('This is probably not a repo, a copy of the code.')

from os import path
this_directory = path.abspath(path.dirname(__file__))
with open(path.join(this_directory, 'README.md'), encoding='utf-8') as f:
    long_description = f.read()

with open('requirements.txt') as f:
    required = f.read().splitlines()

setup(
    name='plaidcloud-utilities',
    version="1.0.5",
    author='Michael Rea',
    author_email='michael.rea@tartansolutions.com',
    packages=['plaidcloud.utilities', 'plaidcloud.utilities.remote'],
    install_requires=required,
    tests_require=test_deps,
    setup_requires=['pytest-runner'],
    extras_require=extras,
    long_description=long_description,
    long_description_content_type='text/markdown',
)
