"""
Model Utility
Utility functions common repository for use by multiple model jobs.
"""
import datetime
import errno
import glob
import logging
import logging.config
import os
import shutil
import time
import uuid

from numpy import nan
import pandas as pd

from plaidcloud.utilities import data_helpers as dh
from plaidcloud.rpc import config

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

conf = config.get_dict()
logger = logging.getLogger(__name__)


def configure_logging():
    """Configures logging based on Plaid's config"""
    # Application logging is defined in a/the configuration file.
    try:
        logging_config_dict = config.get_dict()['logging']
    except KeyError:
        valid_config_dict = (False, 'Could not find logging configuration.')
    except Exception as e:
        logging_config_dict = (False, "Error: {}".format(str(e)))
    else:
        valid_config_dict = (True, None)

    if valid_config_dict[0]:
        logging.config.dictConfig(logging_config_dict)
    else:
        # Allow the program to continue without a good configuration file.
        log_filename = 'C:/MY_FOLDER/Dev/models/application.log'
        logging.basicConfig(level=logging.INFO, filename=log_filename)
        logger.addHandler(logging.StreamHandler())
        logger.error("%r Logging to %s. The program will continue to run.",
                     valid_config_dict[1], log_filename)


def create_results_directories():
    """Creates all needed output directories"""
    MODEL_PERIOD = conf['options']['MODEL_PERIOD']
    PATHS_MODEL  = conf['options']['PATHS_MODEL']
    run_name = "{}__{}".format(conf['run_timestamp'], conf['options']['run_name'].format(period=MODEL_PERIOD))
    results_parent_dir = conf['paths']['RESULTS_PARENT_DIR'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)
    period_dir = conf['paths']['PERIOD_DIR'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)
    for d in [period_dir, results_parent_dir]:
        try:
            os.mkdir(d)
        except OSError as e:
            if e.errno == errno.EEXIST:
                pass
            else:
                raise
    results_dir = '/'.join((results_parent_dir, run_name))
    conf['paths']['RESULTS_DIR'] = results_dir
    os.mkdir(results_dir)
    logger.debug("Specified results will be placed in '%s\\'", os.path.abspath(results_dir))


def copy_output_to_timestamped_directory():
    """Copy all output files to the timestamped results directory.

    Warning: This uses the OUTPUT_DIR path as specified in the config, which
    isn't necessarily the directory that each path is configured to use."""

    # Heavily deliberate proper path construction; glob requires it.
    output_dir = os.path.normpath(conf['paths']['OUTPUT_DIR'].format(PATHS_MODEL=conf['options']['PATHS_MODEL']))  # Source
    results_dir = os.path.normpath(conf['paths']['RESULTS_DIR'])  # Destination
    results_dir = "{}{}".format(results_dir.rstrip(os.sep), os.sep)
    output_paths = glob.glob("{}{}*".format(output_dir.rstrip(os.sep), os.sep))

    for path in output_paths:
        shutil.copy(path, results_dir)
        logger.debug("Copied '%s' to '%s'", path, results_dir)


def copy_results_to_timestamped_directory():
    """Copy all result files to the timestamped results directory."""

    paths = conf['paths']
    results_keys = paths['results_keys']

    # Heavily deliberate proper path construction; glob requires it.
    results_dir = os.path.normpath(conf['paths']['RESULTS_DIR'])  # Destination
    results_dir = "{}{}".format(results_dir.rstrip(os.sep), os.sep)

    for result_key in results_keys:
        path = os.path.normpath(paths[result_key])
        try:
            shutil.copy(path, results_dir)
        except IOError as e:
            logger.warning("Could not copy '%s' into '%s': %s",
                           path, results_dir, e.strerror)
        else:
            logger.debug("Copied '%s' to '%s'", path, results_dir)


def copy_results_output_to_results_directory():
    """Copy all result files to the non-timestamped results directory."""

    paths = conf['paths']
    results_keys = paths['results_keys']

    # Careful - strptime drops timezone data completely, but we are requiring
    # the run_timestamp to be in UTC.
    run_timestamp = datetime.datetime.strptime(conf['run_timestamp'], '%Y-%m-%dT%H_%M_%SZ')
    results_dir = os.path.normpath(paths['RESULTS_PARENT_DIR'].format(period=conf['options']['MODEL_PERIOD'], PATHS_MODEL=conf['options']['PATH_MODEL']))

    for key in results_keys:
        path = os.path.normpath(paths[key])
        try:
            # st_mtime on Windows is the modification time of the file, and
            # it's provided to us by the os module as a Unix timestamp. If an
            # existing file opened in 'w' mode, truncating the existing data,
            # it is still considered as 'modified', not 'created'.
            mtime = datetime.datetime.fromtimestamp(os.stat(path).st_mtime, datetime.UTC)
        except:
            logger.error("'%s' does not exist to copy to '%s'. Should it be "
                         "removed from results_keys?", path, results_dir)
        else:
            # Any output files older than run_timestamp are from another run
            # and should not be copied.
            if mtime > run_timestamp:
                shutil.copy(path, results_dir)
                logger.debug("Copied '%s' into '%s'", path, results_dir)
            else:
                logger.debug("Did not copy '%s' into '%s' because it was "
                             "produced by an earlier run.", path, results_dir)


def clean_empty_results_directory():
    """Delete any empty results directories"""
    results_dir = os.path.normpath(conf['paths']['RESULTS_DIR'])
    try:
        # This fails if the directory is not empty:
        os.removedirs(results_dir)
    except OSError as e:
        if e.errno == errno.ENOENT:
            # Doesn't exist to even delete
            pass
        elif e.errno == errno.ENOTEMPTY:
            # Not empty, so ignore the error
            pass
        else:
            # Unexpected exception
            logger.debug("Couldn't clean '%s': %s", results_dir, str(e))
            raise
    else:
        logger.debug("Unused results directory '%s\\' removed", results_dir)


def do_timed_run(func, func_args=(), func_kwargs={}):
    """Run func, time it, and configure application logging for it.

    This is a convenience function for use in `if name == '__main__'` code
    blocks. run_all.py performs similar functionality on its own.

    Args:
        func (function): The function to time
        func_args (tuple): Args to provide to the function
        func_kwargs (dict): Keyword args to provide to the function
    """

    opts = conf['options']
    logging.getLogger('').handlers = []  # In case plogging is being used
    configure_logging()
    if opts['timestamped_results']:
        create_results_directories()
    start_time = time.time()
    try:
        func(*func_args, **func_kwargs)
    except:
        logger.error("Could not complete run")
        raise
    else:
        end_time = time.time()
        run_time_str, run_secs = get_run_time(start_time, end_time)
        logger.debug("Completed run in %s (%.2f seconds)", run_time_str, run_secs)
    finally:
        if opts['timestamped_output']:
            copy_output_to_timestamped_directory()
        if opts['timestamped_results']:
            copy_results_output_to_results_directory()

            # Remove the timestamped directory if nothing was put in.
            clean_empty_results_directory()


def get_run_time(start_time, end_time):
    """Calculate and return the run time (string and seconds float).

    start_time and end_time are both floats of seconds since the epoch.

    Args:
        start_time (float): When the run started
        end_time (float): When the run ended

    Returns:
        str: How long the run took, in HH:MM:SS format
        float: The unrounded number of seconds the run took
    """

    run_secs_unrounded = end_time - start_time
    run_secs = round(run_secs_unrounded)
    minutes, seconds = divmod(run_secs, 60)
    hours, minutes = divmod(minutes, 60)
    run_time = "{}:{:02}:{:02}".format(int(hours),
                                       int(minutes), int(seconds))

    return run_time, run_secs_unrounded


def get_path_set(patterns, prefix=None):
    """Return a set of all files matching one of the given patterns.

    The parameter `patterns` must be sequence of pattern strings that work with
    the standard library glob, e.g. ('*.htm', '*.html').

    Args:
        patterns (:type:`list` of :type:`str`): The pattern to use to match files
        prefix (str, optional): A prefix apply to all searches

    Returns:
        :type:`list` of :type:`str`: A list of paths that match the pattern
    """

    paths = set()
    for pattern in patterns:
        if prefix:
            path_pattern = os.path.normpath('/'.join((prefix, pattern)))
        else:
            path_pattern = pattern
        paths_found = glob.glob(path_pattern)
        logger.debug("Found files with pattern '%s': %s",
                     path_pattern, paths_found)
        paths.update(paths_found)

    return paths


def _iter_column_mapping(column_mapping):
    for old_column, value in column_mapping.items():
        if isinstance(value, str):
            weight = 1
            rollup = value
        else:
            weight, rollup = value
        yield (old_column, weight, rollup)


def rollup_sum(df, column_mapping):
    """Sum old column values into a new one, and then delete the old ones.

    e.g. If a row has a value of 1 for column 'blue' and a value of 1 for a
    column 'red', and the `column_mapping` dict is
    {'blue': (1, 'color'), 'red': (1, 'color')}, then the DataFrame will have a
    new column, 'color', with a value of 2 for that row (the sum of the values
    found by multiplying the old columns by their given coefficients), and the
    columns 'blue' and 'red' will have been removed from the DataFrame.

    Each column can have a coefficient, though it is not required.
    e.g. {'old': 'new'} and {'old': (2, 'new')} are both okay.

    This function returns a tuple: the first element is the new DataFrame, a
    shallow copy with the changes, and the second element is a list of the new
    column names.

    Args:
        df (`pandas.DataFrame`): The dataframe to rollup
        column_mapping (dict): The column mapping to use in the rollup

    Returns:
        `pandas.DataFrame`: The rolled up version of `df`
        :type:`list` of :type:`str`: A list of the new column names
    """

    df = df.copy(deep=False)

    # Temporary renaming *needs* to occur first.
    rollups = set()
    old_columns = set()
    for old_column, weight, rollup in _iter_column_mapping(column_mapping):
        rollups.add(rollup)
        old_columns.add(old_column)
    collisions = rollups.intersection(old_columns)
    renames = {name: "{}_{}".format(name, uuid.uuid4()) for name in collisions}
    df.rename(columns=renames, inplace=True)

    for old_column_, weight, rollup in _iter_column_mapping(column_mapping):
        if rollup not in df.columns:
            df[rollup] = 0
        weight_col = old_column_ + '_weight'
        old_column = renames.get(old_column_, old_column_)
        df[rollup] += (df[old_column] * df[weight_col])
        del df[weight_col]
        del df[old_column]
        logger.debug("Rolled up %s (weight: %.2f) into %s",
                     old_column_, weight, rollup)

    rollups = list(rollups)
    logger.debug("Created rollup columns: %s", rollups)

    return df, rollups


def get_patterns_for_sync(paths_dict=None):
    """Return a list file short names/patterns for pushing and pulling.

    This will skip the nested JSON objects (dicts) but will expand the nested
    JSON arrays (lists).

    Args:
        paths_dict (dict, optional): A dict of paths to use. By default this is
            specified by the config

    Returns:
        list: List of patterns
    """

    if paths_dict is None:
        paths_dict = conf['paths']
    paths = []

    def get_short_name(path):
        return os.path.normpath(path).rsplit(os.sep, 1)[-1] if path else ''

    for key, value in paths_dict.items():
        if isinstance(value, str):
            paths.append(get_short_name(value))
        elif isinstance(value, list):
            paths.extend(get_short_name(path) for path in value)
    paths.extend((r'.*\.xls[xm]?$'))  # Excel files aren't in paths.json

    return paths


def month_to_quarter(month_num):
    """Convert the month, provided as an int, to a yearly quarter, also an int.

    Args:
        month_num (int): The month number to convert (1-12)

    Returns:
        int: The quarter this month is in

    Examples:
        >>> month_to_quarter(1)
        1
        >>> month_to_quarter(9)
        3
    """

    try:
        month_num = int(month_num)
        if month_num < 1 or month_num > 12:
            raise Exception
    except:
        raise ValueError("month_num must be an int between 1 and 12, not {!r}"
                         .format(month_num))
    quarter = ((month_num - 1) // 3) + 1

    return quarter


def fill_missing_ITEMs(df_to_fill, df_email_ITEM_map, to_fill_email_col_name,
                      to_fill_ITEM_col_name, map_ITEM_col_name):
    """Fills missing emails in one DataFrame using a second as a resource.
    `df_email_ITEM_map` must be indexed by email address. This function
    takes two DataFrames and the relevant column names as parameters."""

    suffixes = ('_to_fill', '_map')
    merged = pd.merge(left=df_to_fill, right=df_email_ITEM_map, how='left',
                      left_on=to_fill_email_col_name, right_index=True,
                      suffixes=suffixes)

    # Determine the correct column names for reference in the rearrangement
    # code that follows. The suffixes will be used by Pandas only if the ITEM
    # columns have the same name.
    if to_fill_ITEM_col_name == map_ITEM_col_name:
        ITEM_tf_cn = "{}{}".format(to_fill_ITEM_col_name, suffixes[0])
        ITEM_map_cn = "{}{}".format(map_ITEM_col_name, suffixes[1])
    else:
        ITEM_tf_cn = to_fill_ITEM_col_name
        ITEM_map_cn = map_ITEM_col_name

    # If the DataFrame to fill is missing a ITEM, prefer the mapping's value.
    merged[ITEM_tf_cn] = merged[ITEM_tf_cn].fillna(merged[ITEM_map_cn])

    # Rearrange the columns so that the returned DataFrame has the same format
    # as the provided `df_to_fill`.
    filled = merged.rename(columns={ITEM_tf_cn: to_fill_ITEM_col_name})
    del filled[ITEM_map_cn]

    return filled


def prefix_ITEM(raw_ITEM):
    """
    Given a string, this will ensure that all ITEM are length 8.
    Ex: ITEM_123456 is WRONG.  It should be ITEM_00123456

    Args:
        raw_ITEM (str): The item to validate

    Returns:
        str: `raw_ITEM` padded to 8 digits
    """
    raw_ITEM = dh.cast_as_int(raw_ITEM)
    if raw_ITEM != 0:
        ITEM = 'ITEM_' + str(raw_ITEM).strip('ITEM_').zfill(8)
        return ITEM
    else:
        return nan


def iter_dataframes(data_source, pd_read_csv_kwargs={'sep': '|'}):
    """Generator which yields DataFrames from `data_source`.
    `data_source` is either an an iterable of file-like objects, an iterable of
    paths, or a glob string. For example glob strings, see the official
    documentation page at https://docs.python.org/2/library/glob.html.
    `pd_read_csv_kwargs` is a dict containing keyword arguments to the function
    `pandas.read_csv`, which is used to generate the DataFrames.

    DataFrame iteration via the `chunksize` or `iterator` parameters is fully
    supported. When used, each file is iterated through in one or more
    chunks, so there may be more than one DataFrame yield per file if this
    option is provided to `pd_read_csv_kwargs`.

    Args:
        data_source (iterable of :type:`File` or :type:`str`): A list of files (or paths to
            files) which contain the data to use to create the DataFrames
        pd_read_csv_kwargs (dict, optional): kwargs to pass to Pandas

    Yields:
        iterable of :type:`pandas.DataFrame`: DataFrames generated by
            the data in `data_source`
    """

    if isinstance(data_source, str):
        paths_or_buffers = glob.glob(data_source)
    else:
        paths_or_buffers = data_source
    of_total = len(paths_or_buffers)

    for iter_no, path_or_buffer in enumerate(paths_or_buffers, start=1):
        data = pd.read_csv(path_or_buffer, **pd_read_csv_kwargs)
        if isinstance(data, pd.io.parsers.TextFileReader):
            dfs = data
        else:
            dfs = [data]

        for df_i, df in enumerate(dfs, 1):
            logger.debug("File #%s of %s, chunk #%s: read %s rows from '%s'",
                        "{:,}".format(iter_no), "{:,}".format(of_total),
                        "{:,}".format(df_i), "{:,}".format(len(df)),
                        path_or_buffer)
            if len(df) > 0:
                yield df
