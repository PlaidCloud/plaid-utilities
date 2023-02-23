#!/usr/bin/env python
# coding=utf-8
"""
Clean Files
Scrub input files to fix issues where some come with unexpected encoding/characters.
"""
import argparse
import csv
import errno
import glob
import logging
import os

import chardet
import pandas as pd
import unicodedata

from plaidcloud.rpc import config
from plaidcloud.utilities import utility

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

logger = logging.getLogger(__name__)
conf = config.get_dict()


def should_clean(dirty_path, clean_path):
    """Determines if the file at `dirty_path` should be cleaned.

    The clean file path is required, as its timestamp is used in the logic.

    Args:
        dirty_path (str): The path of the potentially-dirty file
        clean_path (str): The path where a clean version of the file (if it exists) would be located

    Returns:
        bool: `True` if dirty_path has been modified more recently than clean_path, or if clean_path does
            not exist. Otherwise returns `False`
    """

    if os.path.exists(clean_path):
        if os.stat(dirty_path).st_mtime > os.stat(clean_path).st_mtime:
            print(clean_path)
            should = True
        else:
            should = False
    else:
        should = True

    return should


def clean_email_address_str(input_string):
    """Remove unwanted characters from an email address.

    This only handles characters, not encoding.

    Args:
        input_string (str): The email address to clean

    Returns:
        str: The cleaned version of `input_string`

    Examples:
        >>> clean_email_address_str('test_name@email.com')
        'test_name@email.com'
        >>> clean_email_address_str('\\rtest??  _name\\t@email.com\\n')
        'test_name@email.com'
    """
    # result = str(input_string).translate(None, ',? \t\n\r') # old, doesn't work in Py3
    result = input_string
    chars = ',? \t\n\r'
    for c in chars:
        result = result.replace(c, '')
    return result


def find_clean_path(model_period, original_path, config=conf):
    """Find the clean path for the given `original_path` using the config.

    Args:
        model_period (str): The period of the model, for use in formatting
            the `original_path`.
        original_path (str): The path where the model used to reside
        config (dict, optional): A config dict. Should be provided by default

    Returns:
        str: path of the clean version of `original_path`"""
    try:
        input_to_clean = config['paths']['input_to_clean_dirs']
    except KeyError:
        logger.error('"input_to_clean_dirs" must be defined in paths.json.')
        raise

    paths_model = config['options']['PATHS_MODEL']

    abspath_map = {os.path.abspath(in_dir.format(period=model_period, PATHS_MODEL=paths_model)): os.path.abspath(clean_dir.format(period=model_period, PATHS_MODEL=paths_model))
                   for in_dir, clean_dir in input_to_clean.items()}
    abspath = os.path.abspath(original_path)
    head, tail = os.path.split(abspath)

    try:
        clean_dir = abspath_map[head]
    except KeyError:
        logger.error("Clean directory for '%s' not found in configuration.", head)
        raise

    if not os.path.isdir(clean_dir):
        try:
            os.mkdir(clean_dir)
        except:
            logger.error("Could not create directory '%s'", clean_dir)
            raise
    clean_path = os.path.normpath(os.path.sep.join((clean_dir, tail)))

    return clean_path


def normalize_line(old, new):
    """Normalize whitespace/encoding of each line in `old` and place in `new``.

    `old` and `new` are both file-like objects.

    Args:
        old (:obj:`file`): The file object to normalize
        new (:obj:`file`): The file to write out to"""

    # purge all non-ascii characters.
    # This is the most pain-alleviating piece of code Mike has ever written:
    for line in old:
        line = line.replace('\n"', '"')
        try:
            new.write(unicodedata.normalize('NFKD', line.decode('utf8')).encode('ascii', 'ignore').replace('\n"\n', '"\n'))
        except:
            encoding = chardet.detect(line)['encoding']
            new.write(unicodedata.normalize('NFKD', line.decode(encoding)).encode('ascii', 'ignore').replace('\n"\n', '"\n'))


def normalize_files_at_paths(model_period, conf_d=conf):
    """Normalize whitespace/encoding in files given by the configuration.

    Args:
        model_period (str): The period to normalize
        conf_d (dict, optional): The model configuration

    Returns:
        :obj:`list` of :obj:`str`: The cleaned file paths"""

    if not conf_d:
        conf_d = config.get_dict()

    input_dirs = list(conf_d['paths']['input_to_clean_dirs'].keys())
    input_dirs = [x.format(period=model_period, PATHS_MODEL=conf_d['options']['PATHS_MODEL']) for x in input_dirs]
    patterns = ('*.txt', '*.tsv', '*.csv', '*.psv', '*.dsv')
    path_set = set()
    for input_dir in input_dirs:
        path_set.update(utility.get_path_set(patterns, prefix=input_dir))
    cleaned = []
    for path in path_set:
        raw_path = path
        clean_path = find_clean_path(model_period, raw_path, conf_d)
        if should_clean(raw_path, clean_path):
            logger.debug("Determined that '%s' must be cleaned.", raw_path)
            # Read 'dirty' data, sanitize, and write to a temporary file.
            temp_clean_path = clean_path + '.tmp'
            with open(raw_path, 'r') as old, open(temp_clean_path, 'w') as new:
                normalize_line(old, new)
            try:
                os.remove(clean_path)
            except OSError as e:
                if e.errno != errno.ENOENT:
                    raise
            os.rename(temp_clean_path, clean_path)
            logger.debug("Cleaned data from '%s' written to '%s'", raw_path, clean_path)
            cleaned.append(clean_path)
        else:
            logger.debug("Skipping cleaning of '%s' because it is older than '%s'", raw_path, clean_path)
    
    return cleaned


def clean_email_address_columns(path, column_names, **read_csv_kwargs):
    """Clean the email address `column_names` in in the file at `path`.

    A temporary path will be used, as to not overwrite the contents of the
    given one. The way in which data will be cleaned is determined by the
    function `clean_email_address_str`.

    Args:
        path (str): The path to the flie to clean
        column_names (list of str): A list of the names of the columns to clean
        read_csv_kwargs (kwargs): A dict of kwargs to pass to `pandas.read_csv`"""

    temp_path = path + '.tmp'
    df = pd.read_csv(path, **read_csv_kwargs)
    for col in column_names:
        df[col] = list(map(clean_email_address_str, df[col]))
    df.to_csv(temp_path, sep='|', index=False)

    try:
        os.remove(path)
    except OSError as e:
        if e.errno != errno.ENOENT:
            logger.exception("Couldn't move '%s' to '%s'", temp_path, path)
            raise
    os.rename(temp_path, path)


def clean_email_addresses_in_files(normalized_file_paths, manual_cols=None):
    """Clean the email addresses in files that are known to have them.

    `normalized_file_paths` should be a list of paths that were cleaned by the
    function `normalize_files_at_paths`, which records the files that it
    cleans. If it skipped a file, it will be skipped here.

    Args:
        normalized_file_paths (list of str): List of file paths that
            have been cleaned by `normalize_files_at_paths`
        manual_cols (list of str): A manual list of columns with email
            addresses to clean. Defaults to `None`"""

    paths = conf['paths']
    opts = conf['options']
    PATHS_MODEL = opts['PATHS_MODEL']

    SAMPLE_1_FILE = os.path.normpath(paths['RAW_SAMPLE_1'].format(PATHS_MODEL=PATHS_MODEL))
    CONTACT_POINT_EMAIL_FILE = os.path.normpath(paths['CONTACT_POINT_EMAIL'].format(PATHS_MODEL=PATHS_MODEL))
    IN_EXCHANGE_EMAIL = paths['EXCHANGE_EMAIL_DIR'].format(PATHS_MODEL=PATHS_MODEL)
    email_file_paths = list(map(os.path.normpath, glob.glob('/'.join((IN_EXCHANGE_EMAIL, '*')))))

    if manual_cols:
        files_with_email_addresses = {path: manual_cols for path in normalized_file_paths}
    else:
        files_with_email_addresses = {
            SAMPLE_1_FILE: ['EMAIL'],
            CONTACT_POINT_EMAIL_FILE: ['EMAIL_ADDRESS'],
        }
        files_with_email_addresses.update({
            path: ['ORIGINATOR_EMAIL', 'RECIPIENT_EMAIL']
            for path in email_file_paths
        })

    for path, columns in files_with_email_addresses.items():
        if path in normalized_file_paths:
            clean_email_address_columns(path, columns, sep='|', quoting=csv.QUOTE_ALL)
        else:
            logger.debug("Skipping '%s' email address cleaning because it was skipped before.", path)


def main(model_period=None, columns=None, email_file=None):
    """Character encoding issues are evil, and keeping proper funky master data
    columns is pointless for what we need to do. Clean them up and move on.

    >:(
    -Mike, the "proper funky master"
    """
    if model_period is None:
        model_period = conf['options']['MODEL_PERIOD']
    if not columns or not email_file:
        # "Normal" mode - run both
        cleaned = normalize_files_at_paths(model_period)
        clean_email_addresses_in_files(cleaned)
    else:
        # Email-only mode - use options
        paths = [email_file]
        manual_cols = columns.split(',')
        clean_email_addresses_in_files(paths, manual_cols)


if __name__ == '__main__':
    ap = argparse.ArgumentParser()
    ap.add_argument('--period', '-p',
                    help='Period to run, YYYY_QQ. \
                          If none specified, options.json MODEL_PERIOD will be used')
    opts = conf['options']
    ap.set_defaults(period=opts['MODEL_PERIOD'].upper().strip())

    ap.add_argument('-e', '--clean-email-addresses', action='store',
                    dest='email_file',
                    help='File with email addresses to clean manually')
    ap.add_argument('-c', '--columns', action='store', dest='columns',
                    metavar='COLNAME[,COLNAME_2,COLNAME_3,...]',
                    help='Comma-separated list of column names to clean')
    args = ap.parse_args()
    utility.do_timed_run(main, func_args=(args.period, args.columns, args.email_file))

