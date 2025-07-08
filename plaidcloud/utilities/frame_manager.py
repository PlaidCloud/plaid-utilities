#!/usr/bin/env python
# coding=utf-8
# vim: set filetype=python:

import os
import posixpath
import sys
import math
import datetime
import csv
from functools import wraps
from io import StringIO, BytesIO
import traceback

import fastavro
import pandas as pd
from pandas.api.types import is_string_dtype
import numpy as np
import orjson as json

from plaidcloud.rpc import utc
from plaidcloud.rpc.type_conversion import analyze_type, postgres_to_python_date_format
from plaidcloud.rpc.connection.jsonrpc import SimpleRPC
from plaidcloud.rpc.rpc_connect import Connect
from plaidcloud.utilities.query import Connection, Table
from plaidcloud.utilities import data_helpers as dh


__author__ = 'Paul Morel'
__maintainer__ = 'Paul Morel <paul.morel@tartansolutions.com>'
__copyright__ = '© Copyright 2013-2021, Tartan Solutions, Inc'
__license__ = 'Apache 2.0'


CSV_TYPE_DELIMITER = '::'


class ContainerLogger(object):

    def info(self, msg):
        print(msg, file=sys.stderr)

    def debug(self, msg):
        self.info(msg)

    def exception(self, msg=None):
        print(traceback.format_exc(), file=sys.stderr)
        if msg is not None:
            print(msg, file=sys.stderr)


logger = ContainerLogger()


def sql_from_dtype(dtype):
    """Returns a sql datatype given a pandas datatype

    Args:
        dtype (str): The pandas datatype to convert

    Returns:
        str: the equivalent SQL datatype

    Examples:
        >>> sql_from_dtype('bool')
        'boolean'
        >>> sql_from_dtype('float64')
        'numeric'
        >>> sql_from_dtype('number')
        'numeric'
        >>> sql_from_dtype('varchar(123)')
        'text'
        >>> sql_from_dtype('char(3)')
        'text'
        >>> sql_from_dtype('xml')
        'text'
        >>> sql_from_dtype('bytea')
        'largebinary'
    """
    mapping = {
        'bool': 'boolean',
        'boolean': 'boolean',
        's8': 'text',
        's16': 'text',
        's32': 'text',
        's64': 'text',
        's128': 'text',
        's256': 'text',
        'object': 'text',
        's512': 'text',
        's1024': 'text',
        'text': 'text',
        'string': 'text',
        'int8': 'smallint',  # 2 bytes
        'int16': 'integer',
        'smallint': 'smallint',
        'int32': 'integer',  # 4 bytes
        'integer': 'integer',
        'int64': 'bigint',  # 8 bytes
        'bigint': 'bigint',
        'float8': 'numeric',
        'float16': 'numeric',  # variable but ensures precision
        'float32': 'numeric',  # variable but ensures precision
        'float64': 'numeric',  # variable but ensures precision
        'numeric': 'numeric',
        'serial': 'serial',
        'bigserial': 'bigserial',
        'datetime64[s]': 'timestamp',  # This may have to cover all datettimes
        'datetime64[d]': 'timestamp',
        'datetime64[ns]': 'timestamp',
        'timestamp': 'timestamp',
        'timestamp without time zone': 'timestamp',
        'timedelta64[s]': 'interval',  # This may have to cover all timedeltas
        'timedelta64[d]': 'interval',
        'timedelta64[ns]': 'interval',
        'interval': 'interval',
        'date': 'date',
        'time': 'time',
        'binary': 'largebinary',
        'bytea': 'largebinary',
        'largebinary': 'largebinary',
        'xml': 'text',
        'uuid': 'uuid',
        'money': 'numeric',
        'real': 'numeric',
        'json': 'json',
        'cidr': 'text',
        'inet': 'text',
        'macaddr': 'text',
    }

    dtype = str(dtype).lower()
    if dtype.startswith('num'):
        dtype = 'numeric'
    elif 'char' in dtype:
        dtype = 'text'

    return mapping[dtype]


def save_typed_psv(df, outfile, sep='|', **kwargs):
    """Saves a typed psv, from a pandas dataframe. Types are analyze compatible
    sql types, written in the header, like {column_name}::{column_type}, ...

    Args:
        df (`pandas.DataFrame`): The dataframe to create the psv from
        outfile (file object or str): The path to save the output file to
        sep (str, optional): The separator to use in the output file
    """

    # ADT2017: _write_copy_from did something special with datetimes, but I'm
    # not sure it's necessary, so I'm leaving it out.

    #TODO: for now we just ignore extra kwargs - we accept them to make it a
    #little easier to convert old to_csvs and read_csvs. In the future, we
    #should probably pass as many of them as possible on to to_csv/read_ccsv -
    #the only issue is we have to make sure the header stays consistent.

    def cleaned(name):
        return str(name).replace(CSV_TYPE_DELIMITER, '')

    column_names = [cleaned(n) for n in list(df)]
    column_types = [sql_from_dtype(d) for d in df.dtypes]
    header = [
        CSV_TYPE_DELIMITER.join((name, sqltype))
        for name, sqltype in zip(column_names, column_types)
    ]

    df.to_csv(outfile, header=header, index=False, sep=sep)


def list_of_dicts_to_typed_psv(lod, outfile, types, fieldnames=None, sep='|'):
    """ Saves a list of dicts as a typed psv. Needs a dict of sql types. If
    provided, fieldnames will specify the column order.

    Args:
        lod (:type:`list` of :type:`dict`): The list of dicts containing the data
            to use to create the psv
        outfile (str): The path to save the output file to, including file name
        types (dict): a dict with column names as the keys and column datatypes as
            the values
        fieldnames (:type:`list` of :type:`str`, optional): A list of the field names.
            If none is provided, defaults to the keys in `types`
        sep (str): The separator to use in the output file
    """

    def cleaned(name):
        return str(name).replace(CSV_TYPE_DELIMITER, '')

    header = {
        name: CSV_TYPE_DELIMITER.join((cleaned(name), sqltype))
        for name, sqltype in types.items()
    }

    if fieldnames is None:
        # Caller doesn't care about the order
        fieldnames = list(types.keys())

    def write(buf):
        writer = csv.DictWriter(buf, fieldnames=fieldnames, delimiter=sep)
        writer.writerow(header)  # It's not just the keys, so we're not using writeheader
        for row in lod:
            writer.writerow(row)

    if isinstance(outfile, str):
        with open(outfile, 'w') as buf:
            write(buf)
    else:
        write(outfile)


def get_project_variables(token, uri, project_id):
    """It opens a connection to Analyze and then
    gets vars for a given project

    Args:
        token (str): oAuth token to pass into
        uri (str): Typically https://ci.plaidcloud.com/json-rpc/, https://plaidcloud.com/json-rpc/
            or local dev machine equiv.  Would typically originate from a local config.
        project_id (str): Id of the Project for which to grab the variables

    Returns:
        dict: Variables as key/values

    """

    rpc = SimpleRPC(token, uri, verify_ssl=True)
    try:
        project_vars = rpc.analyze.project.variables(project_id=project_id)
    except:
        project_vars = rpc.analyze.project.variables(project=project_id)

    return {pv['id']: pv['value'] for pv in project_vars}


def download(tables, configuration=None, retries=5, conn=None, clean=False, **kwargs):
    """This replaces the old get_tables() that was client-specific.
    It opens a connection to Analyze and then
    accepts a set of tables and saves them off to a local location.
    For now, tables are understood to be typed psv's, but that can expand to
    suit the need of the application (for instance, Excel.)
    Args:
        tables (set or list): table paths to retrieve (for backwards compatibility, you can leave off the initial '/')
        token (str): token to pass into
        uri (str): Typically https://ci.plaidcloud.com/json-rpc/, https://plaidcloud.com/json-rpc/
        or local dev machine equiv.  Would typically originate from a local config.
        local_storage_path (str): local path where files should be saved.  Would typically originate
        from a local config.
        **kwargs:
            config (dict) contains a dict of config settings
            token (str) simpleRFC authorization token
            uri (str): uri e.g. 'https://ci.plaidcloud.com/json-rpc/'
            local_storage_path (str) Target for files being saved
    Returns:
        The return value of function. If retries are exhausted, raises the
        final Exception.
    Examples:
    """

    # TODO: if configuration is None, revert to **kwargs for the params we need.
    if not conn:
        try:
            rpc = Connect()
        except:
            logger.exception('Could not connect via RPC')
            return False
        conn = Connection(project=rpc.project_id)

    try:
        return_df = configuration['return_df']
    except:
        return_df = True

    try:
        project_id = configuration['project_id']
    except:
        project_id = conn.project_id

    dfs = []

    for table in tables:
        table_path = table.get('table_name')
        query = table.get('query')
        table_obj = table.get('table_object')

        df = None  # Initial value
        # wipe this out each time through
        clean_df = pd.DataFrame()

        logger.debug("Attempting to download {0}...".format(table_path))

        tries = 1
        if table_obj is not None:
            # RPC table object exists; proceed to use it to fetch data
            while tries <= retries:
                if query is None:
                    # no query passed. fetch whole table
                    df = conn.get_dataframe(table_obj, clean=clean)
                    if isinstance(df, pd.DataFrame):
                        logger.debug("Downloaded {0}...".format(table_path))
                        break
                elif isinstance(query, str):
                    # query object passed in.  execute it
                    try:
                        df = conn.get_dataframe_by_querystring(query)
                    except Exception as e:
                        logger.exception("Attempt {0}: Failed to download {1}: {2}".format(tries, table_path, e))
                    else:
                        if isinstance(df, pd.DataFrame):
                            logger.debug("Downloaded {0}...".format(table_path))
                            break
                else:
                    # query object passed in.  execute it
                    try:
                        df = conn.get_dataframe_by_query(query)
                    except Exception as e:
                        logger.exception("Attempt {0}: Failed to download {1}: {2}".format(tries, table_path, e))
                    else:
                        if isinstance(df, pd.DataFrame):
                            logger.debug("Downloaded {0}...".format(table_path))
                            break
                tries += 1

            columns = table_obj.cols()
            if columns:
                if isinstance(df, pd.DataFrame):
                    cols = [c['id'] for c in columns if c['id'] in df.columns.tolist()]
                    df = df[cols]  # this ensures that the column order is as expected
                else:
                    cols = [c['id'] for c in columns]
                    df = pd.DataFrame(columns=cols)  # create empty dataframe with expected metadata/shape
        else:
            if not table_path.startswith('/'):
                table_path = '/{}'.format(table_path)
            table_result = None
            while not table_result and tries <= retries:
                tries += 1
                try:
                    table_result = conn.analyze.table.table(project_id=project_id, table_path=table_path)
                    logger.debug("Downloaded {0}...".format(table_path))
                    break
                except Exception as e:
                    logger.exception("Attempt {0}: Failed to download {1}: {2}".format(tries, table_path, e))

            df = table_result_to_df(table_result or pd.DataFrame())

        if not isinstance(df, pd.DataFrame):
            logger.exception('Table {0} failed to download!'.format(table_path))
        elif len(df.columns) == 0:
            logger.exception('Table {0} downloaded 0 records!'.format(table_path))
        else:
            if clean and query:
                # Use the old cleaning process for things other than the full query.
                clean_df = dh.clean_frame(df)
            else:
                clean_df = df

        dfs.append({'df': clean_df, 'name': table_path})

    return dfs



def load(source_tables, fetch=True, cache_locally=False, configuration=None, conn=None, clean=False):
    """Load frame(s) from requested source, returning a list of dicts

    If local, will load from the typed_psv.  If Analyze, then will load the analyze table.
    """
    return_type = None
    if type(source_tables) == list:
        return_type = 'list'
    elif type(source_tables) == str:
        # single table (as string) passed... expecting to return full table
        source_tables = [source_tables]
        return_type = 'dataframe'
    elif type(source_tables) == dict:
        # single table (as dict) passed... likely with subsetting query, but not req'd
        source_tables = [source_tables]
        return_type = 'dataframe'

    source_tables_proper = []
    reassign = False
    for s in source_tables:
        if type(s) == str:
            # convert list of strings into a list of dicts
            reassign = True
            d = {}
            d['table_name'] = s
            source_tables_proper.append(d)

    if reassign:
        # replace source_tables with reformatted version
        source_tables = source_tables_proper

    dfs = []
    if fetch is True:
        if not conn:
            # create connection object
            try:
                rpc = Connect()
            except:
                logger.exception('Could not connect via RPC')
                return False
            conn = Connection(project=rpc.project_id)

        for s in source_tables:
            # create table objects if they don't exist
            if s.get('table_object') == None:
                s['table_object'] = Table(conn, s.get('table_name'))

        downloads = download(source_tables, configuration=configuration, conn=conn, clean=clean)

        for d in downloads:
            df = d.get('df')
            name_of_df = '{0}.psv'.format(d.get('name'))
            if name_of_df.startswith('/'):
                name_of_df = name_of_df[1:]
            if cache_locally is True:
                with open(os.path.join(configuration['LOCAL_STORAGE'], name_of_df), 'w') as f:
                    save_typed_psv(df, f)
            dfs.append(df)
    else:
        for s in source_tables:
            source_table = '{0}.psv'.format(s.get('table_name'))
            source_path = os.path.join(configuration['LOCAL_STORAGE'], source_table)
            df = load_typed_psv(source_path)
            dfs.append(df)

    if return_type == 'dataframe':
        return dfs[0]
    else:
        return dfs


def load_new(source_tables, sep='|', fetch=True, cache_locally=False, configuration=None, connection=None):
    """Load frame(s) from requested source

    If local, will load from the typed_psv.  If Analyze, then will load the analyze table.

    TODO: Make it fetch from analyze table....really this should be assimilated with dwim once dwim works again.
    TODO: Make it go to analyze and cache locally, if requested to do so.
    """

    if connection:
        configuration['project_id'] = connection.project_id

    if fetch is True:
        download(source_tables, configuration)

    dfs = []
    for source_table in source_tables:
        _, table_name = posixpath.split(source_table)
        source_path = '{}/{}.psv'.format(configuration['LOCAL_STORAGE'], source_table)
        df = load_typed_psv(source_path)
        dfs.append(df)
    return dfs


def avro_from_sql(sql, default=None):
    """Gets an avro dtype from a SQL data type
    Args:
        sql (str): SQL data type
        
    Returns:
        str: the Avro data type equivalent"""
    
    mapping = {
        'null': 'null',
        'text': 'string',
        'boolean': 'boolean',
        'smallint': 'int',
        'bigint': 'int',
        'integer': 'int',
        'numeric': 'double',
        'timestamp': 'int',
        'interval': 'float',
        'date': 'float',
        'time': 'float'
    }

    return mapping.get(sql, default)



def dtype_from_sql(sql):
    """Gets a pandas dtype from a SQL data type

    Args:
        sql (str): The SQL data type

    Returns:
        str: the pandas dtype equivalent of `sql`
    """
    mapping = {
        'boolean': 'bool',
        'text': 'object',
        'smallint': 'int16',
        'integer': 'int32',
        'bigint': 'int64',
        'numeric': 'float64',
        'timestamp': 'datetime64[s]',
        'interval': 'timedelta64[s]',
        'date': 'datetime64[s]',
        'time': 'datetime64[s]',
    }

    return mapping.get(str(sql).lower(), None)


def sturdy_cast_as_float(input_val):
    """
    Force a value to be of type 'float'.  Sturdy and unbreakeable.
    Works like data_helpers.cast_as_float except it returns NaN and None
    in cases where such seems appropriate, whereas the former forces to 0.0.
    """

    if input_val is None:
        return 0.0
    try:
        if np.isnan(input_val):
            float('nan')
        else:
            try:
                return float(input_val)
            except ValueError:
                return None
    except:
        try:
            return float(input_val)
        except ValueError:
            return None


def converter_from_sql(sql):
    """Gets a pandas converter from a SQL data type

    Args:
        sql (str): The SQL data type

    Returns:
        str: the pandas converter
    """
    mapping = {
        'boolean': bool,
        'text': str,
        'smallint': int,
        'integer': int,
        'bigint': int,
        #'numeric': float, #dh.cast_as_float,
        #'numeric': dh.cast_as_float,
        'numeric': sturdy_cast_as_float,
        'timestamp': pd.datetime,
        'interval': pd.datetime,
        'date': pd.datetime,
        'time': pd.datetime,
    }

    return mapping.get(str(sql).lower(), str(sql).lower())



def load_typed_psv(infile, sep='|', **kwargs):
    """ Loads a typed psv into a pandas dataframe. If the psv isn't typed,
    loads it anyway.

    Args:
        infile (str): The path to the input file
        sep (str, optional): The separator used in the input file
    """

    #TODO: for now we just ignore extra kwargs - we accept them to make it a
    #little easier to convert old to_csvs and read_csvs. In the future, we
    #should probably pass as many of them as possible on to to_csv/read_ccsv -
    #the only issue is we have to make sure the header stays consistent.

    if isinstance(infile, str):
        if os.path.exists(infile):
            buf = open(infile, 'r')
        else:
            logger.exception('File does not exist: {0}'.format(infile))
            return False
    else:
        buf = infile

    try:
        headerIO = StringIO(buf.readline())  # The first line needs to be in a separate iterator, so that we don't mix read and iter.
        header = next(csv.reader(headerIO, delimiter=sep))  # Just parse that first line as a csv row
        names_and_types = [h.split(CSV_TYPE_DELIMITER) for h in header]
        column_names = [n[0] for n in names_and_types]
        try:
            dtypes = {
                name: dtype_from_sql(sqltype)
                for name, sqltype in names_and_types
            }
        except ValueError:
            # Missing sqltype - looks like this is a regular, untyped csv.
            # Let's hope that first line was its header.
            dtypes = None

        converters={}
        #for name, sqltype in names_and_types:
            #converter = converter_from_sql(sqltype)
            #if converter:
                #converters[name] = converter

        try:
            converters = {
                name: converter_from_sql(sqltype)
                for name, sqltype in names_and_types
            }
        except ValueError:
            # Missing sqltype - looks like this is a regular, untyped csv.
            # Let's hope that first line was its header.
            converters = None

        # This will start on the second line, since we already read the first line.
        #return pd.read_csv(buf, header=None, names=column_names, dtype=dtypes, sep=sep)
        na_values = [
            #'',  # This was here, and then commented out, and I'm putting it back in 20180824. ***
            #     # If it isn't here, we fail when attempting to import a delimited file of type 'numeric'
            #     # it is coming in as null/empty (e.g. the last record in the following set:)
            #     # LE::text|PERIOD::text|RC::text|MGCOA::text|VT::text|TP::text|FRB::text|FUNCTION::text|DCOV::numeric|LOCAL_CURRENCY::text|CURRENCY_RATE::numeric|DCOV_LC::numeric
            #     # LE_0585|2018_01|6019999|6120_NA|VT_0585|TP_NA|FRB_AP74358|OM|0.00031|EUR|0.8198|0.000254138
            #     # LE_0003|2018_07|CA10991|5380_EBITX|VT_9988|TP_NA|FRB_APKRA15|OM|-0.00115|INR|68.7297|-0.079039155
            #     # LE_2380|2017_08|AP92099|Q_5010_EBITX|VT_0585|TP_NA|FRB_AP92099|RE|99|||
            '#N/A',
            '#N/A N/A',
            '#NA',
            '-1.#IND',
            '-1.#QNAN',
            '-NaN',
            '-nan',
            '1.#IND',
            '1.#QNAN',
            'N/A',
            'NA',
            'NULL',
            'NaN',
            'n/a',
            'nan',
            'null'
        ]
        parse_dates = []

        if dtypes is not None:
            for k, v in dtypes.items():
                dtypes[k] = v.lower()
                #Handle inbound dates
                #https://stackoverflow.com/questions/21269399/datetime-dtypes-in-pandas-read-csv
                if 'datetime' in dtypes[k]:
                    dtypes[k] = 'object'
                    parse_dates.append(k)

        try:
            df = pd.read_csv(buf, header=None, names=column_names, dtype=dtypes, sep=sep, na_values=na_values, keep_default_na=False, parse_dates=parse_dates, encoding='utf-8')
        except ValueError:
            #remove dtypes if we have converters instead:
            for k in converters.keys():
                if k in list(dtypes.keys()):
                    dtypes.pop(k, None)
            na_values.append('')
            buf = open(infile, 'rb')
            headerIO = BytesIO(buf.readline())  # The first line needs to be in a separate iterator, so that we don't mix read and iter.
            header = next(csv.reader(headerIO, delimiter=sep))  # Just parse that first line as a csv row
            df = pd.read_csv(buf, header=None, names=column_names, dtype=dtypes, sep=sep, na_values=na_values, keep_default_na=False, parse_dates=parse_dates, converters=converters, encoding='utf-8')
        finally:
            # A final note:
            #    SURELY there's a more efficient and native pandas way of doing this, but I'll be damnded if I could figure it out.
            #    Pandas used to have an error='coerce' method to force data type.  It's no longer an option, it seems.
            #    Forcing data type is NOT easy, when incoming text data is sequential delimiters with no values or whitespace.
            #    What We're doing now is still not w/o risk.  There are use cases for setting empty to zero, which is what we're doing, and use cases to set
            #    empty to null, which is probably what we SHOULD do, but for now, we do it this way because we already have a battle hardened dh.cast_as_float that
            #    works this way.  We should probably just call a different homegrown float that returns a NaN or None (None being preferred) rather than 0.0 on exception.
            #    Mercy.  This has been a pain.
            #    I guess if it was easy, Pandas wouldn't support the ability to send in your own converters.
            pass
        return df


    finally:
        if isinstance(infile, str):
            buf.close()
            #Otherwise leave it open, since this function didn't open it.


def table_result_to_df(result):
    """Converts a SQL result to a pandas dataframe

    Args:
        result (dict): The result of a database query

    Returns:
        `pandas.DataFrame`: A  dataframe representation of `result`
    """
    meta = result['meta']
    data = result['data']

    columns = [m['id'] for m in meta]
    dtypes = {
        m['id']: dtype_from_sql(m['dtype'].lower())
        for m in meta
    }

    df = pd.DataFrame.from_records(data, columns=columns)

    try:
        typed_df = df.astype(dtype=dtypes)
    except:
        """
        This is heavy-handed, but it had to be.
        Something was tripping up the standard behavior, presumably relating to
        handling of nulls in floats.  We're forcing them to 0.0 for now, which is possibly
        sketchy, depending on the use case, but usually preferred behavior.
        Buyer beware.
        """
        typed_df = df
        for col in typed_df.columns:
            if dtypes[col] == 'object':
                typed_df[col] = list(map(dh.cast_as_str, typed_df[col]))
            elif dtypes[col].startswith('float'):
                typed_df[col] = list(map(dh.cast_as_float, typed_df[col]))
            elif dtypes[col].startswith('int'): #detect any flavor of int and cast it as int.
                typed_df[col] = list(map(dh.cast_as_int, typed_df[col]))

    return typed_df


def dwim_save(df, name, localdir='/tmp', lvl='model', extension='txt', sep='|', **kwargs):
    """If we're on an app server, saves a dataframe as an analyze table.
    Otherwise saves it as a typed psv in localdir.

    Args:
        df (`pandas.DataFrame`): The dataframe to save
        name (str): The name to save this dataframe as
        localdir (str, optional): The local path to save the typed psv
        lvl (str, optional): What level (project/model) the table should be
        extension (str, optional): What file extension to give the output file
        sep (str, optional): The separator to use in the output file
    """


    #TODO: for now we just ignore extra kwargs - we accept them to make it a
    #little easier to convert old to_csvs and read_csvs. In the future, we
    #should probably pass as many of them as possible on to to_csv/read_ccsv -
    #the only issue is we have to make sure the header stays consistent.

    try:
        from plaid.app.analyze.sandboxed_code.user.iplaid.frame import save, save_project
        # We must be on the app server.
        # TODO: change this when we change how iplaid works

        save_fn = {
            'model': save,
            'project': save_project,
        }[lvl]

        save_fn(df, name)

    except ImportError:
        # We must not be on an app server, so save as typed_psv
        fname = '.'.join((name, extension))
        if lvl == 'model':
            path = os.path.join(localdir, fname)
        else:
            path = os.path.join(localdir, lvl, fname)

        dirname = os.path.dirname(path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        save_typed_psv(df, path, sep)


def dwim_load(name, localdir='/tmp', lvl='model', extension='txt', sep='|', **kwargs):
    """If we're on an app server, loads an analyze table.
    Otherwise loads a typed psv from localdir.

    Args:
        name (str): The name of the table or file to load
        localdir (str, optional): The path to the directory where the local file is stored
        lvl (str, optional): The level (model/project) of the table to load
        extension (str, optional): The flie extension of the local file
        sep (str, optional): The separator used in the local file
    """

    #TODO: for now we just ignore extra kwargs - we accept them to make it a
    #little easier to convert old to_csvs and read_csvs. In the future, we
    #should probably pass as many of them as possible on to to_csv/read_ccsv -
    #the only issue is we have to make sure the header stays consistent.

    try:
        from plaid.app.analyze.sandboxed_code.user.iplaid.frame import load, load_project
        # We must be on the app server.
        # TODO: change this when we change how iplaid works

        load_fn = {
            'model': load,
            'project': load_project,
        }[lvl]

        return load_fn(name)

    except ImportError:
        # We must not be on an app server, so load from typed_psv
        fname = '.'.join((name, extension))
        if lvl == 'model':
            path = os.path.join(localdir, fname)
        else:
            path = os.path.join(localdir, lvl, fname)

        return load_typed_psv(path, sep)


def clean_uuid(id):
    """Removes any invalid characters from a UUID and ensures it is 32 or 36 characters

    Args:
        id (str): The ID to clean

    Returns:
        str: `id` with any invalid characters removed
    """
    # !! WARNING: If you're calling this in new code, make sure it's really what you
    # !! want. It used to remove dashes. That turned out to be a bad idea. Now
    # !! it leaves dashes in.
    #
    # !! If you've found a bug related to dashes being left in, and this is
    # !! being called on lookup, you should probably just remove the call to
    # !! clean_uuid. Going forward, we don't remove dashes.

    if id is None:
        return None

    name = str(id).lower()
    valid_chars = '0123456789abcdef-'
    cleaned_id = u''.join(n for n in name if n in valid_chars)
    if '-' in cleaned_id:
        if len(cleaned_id) != 36:
            raise Exception("Could not clean id {}. Not 36 characters long.".format(id))
    else:
        if len(cleaned_id) != 32:
            raise Exception("Could not clean id {}. Not 32 characters long.".format(id))

    return cleaned_id


def clean_name(name):
    """
    DEPRECATED: does nothing

    Removes any invalid characters from a name and limits it to 63 characters

    Args:
        name (str): The name to clean

    Returns:
        str: The cleaned version of `name`
    """
    return name


def clean_filename(name):
    """Remove '/' from a name

    Args:
        name (str): the filename to clean

    Returns:
        str: the cleaned version of `name`
    """
    if name is None:
        return None

    # everything's fine except /
    return str(name).translate({'/': None})


def describe(df):
    """Shorthand for df.describe()

    Args:
        df (`pandas.DataFrame`): The dataframe to describe

    Returns:
        summary: Series/DataFrame of summary statistics
    """
    return df.describe()


def unique_values(df, column):
    """Returns unique values in the provided column

    Args:
        df (`pandas.DataFrame`): The DataFrame containing data
        column (str): The column to find unique values in

    Returns:
        list: The unique values in the column
    """
    return df[column].unique()


def count_unique(group_by, count_column, df):
    """Returns a count of unique items in a dataframe

    Args:
        group_by (str): The group by statement to apply to the dataframe
        count_column (str): The column to count unique records in
        df (`pandas.DataFrame`): The DataFrame containing the data

    Returns:
        int: The count of unique items in the specified column after grouping
    """
    return df.groupby(group_by)[count_column].apply(lambda x: len(x.unique()))


def sum(group_by, df):
    return df.groupby(group_by).sum()


def std(group_by, df):
    return df.groupby(group_by).std()


def mean(group_by, df):
    return df.groupby(group_by).mean()


def count(group_by, df):
    return df.groupby(group_by).count()


def inner_join(left_frame, right_frame, left_on, right_on=None, keep_columns=None):
    """Keeps only matches

    Args:
        left_frame (`pandas.DataFrame`): The first frame to join
        right_frame (`pandas.DataFrame`): The second frame to join on
        left_on (str): Which column in the left frame to join on
        right_on (str, optional): Which column to join on in the right frame, if different from `left_on`
        keep_columns (:type:`list` of :type:`str`, optional): A list of columns to keep in the result

    Returns:
       `pandas.DataFrame`: A frame containing the results of the join
    """

    if right_on is None:
        right_on = left_on

    if type(left_on) == str:
        left_on = [left_on]

    if type(right_on) == str:
        right_on = [right_on]

    right_cols = right_frame.columns

    # optional arg to specify which columns in right table to keep
    if keep_columns is not None:
        drop_columns = set()
        for col in right_cols:
            if (col in keep_columns) or (col in right_on):
                pass
            else:
                # column not being kept
                drop_columns.add(col)

        # exclude columns from right table
        right_cols = right_cols.difference(drop_columns)

    return pd.merge(left_frame, right_frame[right_cols], left_on=left_on, right_on=right_on, how='inner')


def outer_join(left_frame, right_frame, left_on, right_on=None, keep_columns=None):
    """Keeps data from both frames and matches up using the on_columns

    Args:
        left_frame (`pandas.DataFrame`): The first frame to join
        right_frame (`pandas.DataFrame`): The second frame to join on
        left_on (str): Which column in the left frame to join on
        right_on (str, optional): Which column to join on in the right frame, if different from `left_on`
        keep_columns (:type:`list` of :type:`str`, optional): A list of columns to keep in the result

    Returns:
       `pandas.DataFrame`: A frame containing the results of the join
    """

    if right_on is None:
        right_on = left_on

    if type(left_on) == str:
        left_on = [left_on]

    if type(right_on) == str:
        right_on = [right_on]

    right_cols = right_frame.columns

    # optional arg to specify which columns in right table to keep
    if keep_columns is not None:
        drop_columns = set()
        for col in right_cols:
            if (col in keep_columns) or (col in right_on):
                pass
            else:
                # column not being kept
                drop_columns.add(col)

        # exclude columns from right table
        right_cols = right_cols.difference(drop_columns)

    return pd.merge(left_frame, right_frame[right_cols], left_on=left_on, right_on=right_on, how='outer')


def left_join(left_frame, right_frame, left_on, right_on=None, keep_columns=None):
    """Keeps all data from left frame and any matches in right using the on_columns

    Args:
        left_frame (`pandas.DataFrame`): The first frame to join
        right_frame (`pandas.DataFrame`): The second frame to join on
        left_on (str): Which column in the left frame to join on
        right_on (str, optional): Which column to join on in the right frame, if different from `left_on`
        keep_columns (:type:`list` of :type:`str`, optional): A list of columns to keep in the result

    Returns:
       `pandas.DataFrame`: A frame containing the results of the join
    """

    if right_on is None:
        right_on = left_on

    if type(left_on) == str:
        left_on = [left_on]

    if type(right_on) == str:
        right_on = [right_on]

    right_cols = right_frame.columns

    # optional arg to specify which columns in right table to keep
    if keep_columns is not None:
        drop_columns = set()
        for col in right_cols:
            if (col in keep_columns) or (col in right_on):
                pass
            else:
                # column not being kept
                drop_columns.add(col)

        # exclude columns from right table
        right_cols = right_cols.difference(drop_columns)

    return pd.merge(left_frame, right_frame[right_cols], left_on=left_on, right_on=right_on, how='left')


def right_join(left_frame, right_frame, left_on, right_on=None, keep_columns=None):
    """Keeps all data from right frame and any matches in left using the on_columns

    Args:
        left_frame (`pandas.DataFrame`): The first frame to join
        right_frame (`pandas.DataFrame`): The second frame to join on
        left_on (str): Which column in the left frame to join on
        right_on (str, optional): Which column to join on in the right frame, if different from `left_on`
        keep_columns (:type:`list` of :type:`str`, optional): A list of columns to keep in the result

    Returns:
       `pandas.DataFrame`: A frame containing the results of the join
    """

    if right_on is None:
        right_on = left_on

    if type(left_on) == str:
        left_on = [left_on]

    if type(right_on) == str:
        right_on = [right_on]

    right_cols = right_frame.columns

    # optional arg to specify which columns in right table to keep
    if keep_columns is not None:
        drop_columns = set()
        for col in right_cols:
            if (col in keep_columns) or (col in right_on):
                pass
            else:
                # column not being kept
                drop_columns.add(col)

        # exclude columns from right table
        right_cols = right_cols.difference(drop_columns)

    return pd.merge(left_frame, right_frame[right_cols], left_on=left_on, right_on=right_on, how='right')



def anti_join(left_frame, right_frame, left_on, right_on=None):
    """Keeps all data from left frame that is not found in right frame

    Args:
        left_frame (`pandas.DataFrame`): The first frame to join
        right_frame (`pandas.DataFrame`): The second frame to join on
        left_on (str): Which column in the left frame to join on
        right_on (str, optional): Which column to join on in the right frame, if different from `left_on`

    Returns:
       `pandas.DataFrame`: A frame containing the results of the join
    """

    if right_on is None:
        right_on = left_on

    if type(left_on) == str:
        left_on = [left_on]

    if type(right_on) == str:
        right_on = [right_on]

    indicator_status = False
    indicator_name = '_merge'

    left_cols = left_frame.columns

    # avoid collision with pd generated indicator name
    while not indicator_status:
        if indicator_name in left_cols:
            indicator_name = '_' + indicator_name
        else:
            indicator_status = True

    df = pd.merge(left_frame, right_frame[right_on], how='left', left_on=left_on, right_on=right_on, indicator=indicator_name)
    df = df[df[indicator_name] == 'left_only']
    del df[indicator_name]

    return df


def compare(left_frame, right_frame, left_on, right_on=None):
    """Keeps all data from right frame and any matches in left using the on_columns"""

    #20180420 PBB Is "compare" a good name for this, it's basically a right-join in SQL terms?
    #20180420 MWR It's quite old legacy.  Not sure this one has ever been used for anything.  Perhaps
    #             we can just do away with it.

    if right_on is None:
        right_on = left_on

    return pd.merge(left_frame, right_frame, left_on=left_on, right_on=right_on, how='outer')


def apply_rule(df, rules, target_columns=None, include_once=True, show_rules=False):
    """
    If include_once is True, then condition n+1 only applied to records left after condition n.
    Adding target column(s), plural, because we'd want to only run this operation once, even
    if we needed to set multiple columns.

    Args:
        df (pandas.DataFrame): The DataFrame to apply rules on
        rules (list): A list of rules to apply
        target_columns (list of str, optional): The target columns to apply rules on.
        include_once (bool, optional): Should records that match multiple rules
            be included ONLY once? Defaults to `True`
        show_rules (bool, optional): Display the rules in the result data? Defaults to `False`

    Returns:
        pandas.DataFrame: The results of applying rules to the input `df`
    """
    target_columns = target_columns or ['value']
    df_final = pd.DataFrame()

    df['temp_index'] = df.index
    df['include'] = True
    df['log'] = ''

    if show_rules is True:
        df['rule_number'] = ''
        df['rule'] = ''

    # Establish new column(s) as blank columns.
    for column in target_columns:
        df[column] = ''

    def exclude_matched(include, match):
        """
        Exclude if matched, or if previously excluded
        Please do not change the 'if match is True:' line to 'if match:'.  It matters here.
        """
        return False if match is True else include

    rule_num = 0

    for rule in rules:
        rule_num = rule_num + 1
        rule_condition = rule.get('condition')

        # Find subset based on condition
        if rule_condition is not None and rule_condition != '' and str(rule_condition) != 'nan':
            try:
                df_subset = df[df['include'] == True].query(rule_condition, engine='python')
                print('subset length: {}'.format(len(df[df['include'] == True])))
                if show_rules:
                    df_subset['rule_number'] = str(rule_num)
                    df_subset['rule'] = str(rule_condition)
            except Exception as e:
                # TODO update this. We should capture all exceptions in an exception table.
                df_subset = pd.DataFrame()

                def add_message(log):
                    return '<{} ::: {}>'.format(e, log)  # removed redundant rule_condition format param from here
                if show_rules:
                    df['log'] = list(map(add_message, df['log']))
                error_msg = ' (rule_num {0}) {1} error: {2}'.format(rule_num, rule_condition, e)
                logger.exception('EXCEPTION {}'.format(error_msg))
        else:
            df_subset = df[df['include'] == True]

        # Populate target columns as specified in split
        for column in target_columns:
            df_subset[column] = rule[column]

        # need to find a way to flip the flag once data has been selected

        if include_once:
            # Exclude the records of the current split from exposure to
            # subsequent filters.

            #if statement handles edge case where df is empty and has no columns.
            if 'temp_index' in df_subset.columns:
                #refactor to be m*1 not m*n.
                df_subset['match'] = True
                df = lookup(
                    df,
                    df_subset,
                    left_on=['temp_index'],
                    right_on=['temp_index'],
                    keep_columns=['match']
                )

                df['include'] = list(map(exclude_matched, df['include'], df['match']))

                del df['match']

        # The way we're doing this allows multiple matches
        # if include_once is false.
        # Future: MAY be a reason to allow first-in wins or last-in wins, or ALL win.
        df_final = pd.concat([df_final, df_subset])
        print('length:{}'.format(len(df_subset)))

    df_final.drop(columns=['temp_index', 'include'], inplace=True, errors='ignore')

    return df_final


def apply_rules(df, df_rules, target_columns=None, include_once=True, show_rules=False,
                verbose=True, unmatched_rule='UNMATCHED', condition_column='condition', iteration_column='iteration',
                rule_id_column=None, logger=logger, raise_exceptions=False):
    """
    If include_once is True, then condition n+1 only applied to records left after condition n.
    Adding target column(s), plural, because we'd want to only run this operation once, even
    if we needed to set multiple columns.

    Args:
        df (pandas.DataFrame): The DataFrame to apply rules on
        df_rules (pandas.DataFrame): A list of rules to apply
        target_columns (list of str, optional): The target columns to apply rules on.
        include_once (bool, optional): Should records that match multiple rules
            be included ONLY once? Defaults to `True`
        show_rules (bool, optional): Display the rules in the result data? Defaults to `False`
        verbose (bool, optional): Display the rules in the log messages? Defaults
            to `True`.  This is not overly heavier than leaving it off, so we probably should
            always leave it on unless logging is off altogether.
        unmatched_rule (str, optional): Default rule to write in cases of records not matching any rule
        condition_column (str, optional): Column name containing the rule condition, defaults to 'condition'
        rule_id_column (str, optional): Column name containing the rule id, just set to index if not provided
        logger (object, optional): Logger to record any output
        raise_exceptions (bool, optional): Whether to raise exceptions. Defaults to `False` (backwards compatible)

    Returns:
        list of pandas.DataFrame: The results of applying rules to the input `df`
    """
    target_columns = target_columns or ['value']
    df_rules = df_rules.reset_index(drop=True)
    if iteration_column not in df_rules.columns:
        df_rules[iteration_column] = 1

    df['include'] = True
    df['log'] = ''
    if show_rules is True:
        df['rule_number'] = ''
        df['rule'] = ''
        df['rule_id'] = '' if rule_id_column else df.index

    # Establish new column(s) as blank columns <i>if they do not already exist.</i>
    for column in target_columns:
        if column not in df.columns:
            df[column] = ''

    summary = []
    row_num = 0
    iterations = list(set(df_rules[iteration_column]))
    iterations.sort()
    for iteration in iterations:
        df['include'] = True

        def write_rule_numbers(rule_num):
            """Need to allow for fact that there will be > 1 sometimes if we have > iteration."""
            if rule_num == '':
                return str(index)
            else:
                return '{}, {}'.format(rule_num, str(index))

        def write_rule_conditions(condition):
            """Need to allow for fact that there will be > 1 sometimes if we have > iteration."""
            if condition == '':
                return str(rule[condition_column])
            else:
                return '{}, {}'.format(condition, str(rule[condition_column]))

        def write_rule_id(rule_id):
            """Need to allow for fact that there will be > 1 sometimes if we have > iteration."""
            if rule_id == '':
                return str(rule[rule_id_column])
            else:
                return '{}, {}'.format(rule_id, str(rule[rule_id_column]))

        matches = []  # for use when include_once is False
        for index, rule in df_rules[df_rules[iteration_column] == iteration].iterrows():
            # Find subset based on condition
            df_subset = df[df['include'] == True]
            input_length = len(df_subset)
            if include_once is True and input_length == 0:
                break
            if verbose:
                logger.debug('')
                logger.debug('iteration:{} - rule:{} - {}'.format(iteration, index, rule[condition_column]))
                logger.debug('{} - input length'.format(input_length))

            if rule[condition_column] is not None and rule[condition_column] != '' and str(rule[condition_column]) != 'nan':
                try:
                    df_subset = df_subset.query(rule[condition_column])#, engine='python')
                    if show_rules is True:
                        if include_once is True:
                            df.loc[list(df_subset.index), 'rule_number'] = list(map(write_rule_numbers, df.loc[list(df_subset.index), 'rule_number']))
                            # df.loc[list(df_subset.index), 'rule'] = list(map(write_rule_conditions, df.loc[list(df_subset.index), 'rule']))
                            # Set this to empty string.  Waaaaaay to much data being generated.
                            df.loc[list(df_subset.index), 'rule'] = ''
                            if rule_id_column:
                                df.loc[list(df_subset.index), 'rule_id'] = list(map(write_rule_id, df.loc[list(df_subset.index), 'rule_id']))
                        else:
                            df_subset.loc[list(df_subset.index), 'rule_number'] = df_subset.index
                            # df_subset.loc[list(df_subset.index), 'rule'] = rule[condition_column]
                            # Set this to empty string.  Waaaaaay to much data being generated.
                            df_subset.loc[list(df_subset.index), 'rule'] = ''

                            if rule_id_column:
                                df_subset.loc[list(df_subset.index), 'rule_id'] = rule[rule_id_column]
                except Exception as e:
                    if raise_exceptions:
                        raise
                    df_subset = pd.DataFrame()

                    def add_message(log):
                        return '<{} ::: {}>'.format(e, log)  # removed redundant rule[condition_column] param from format string

                    if show_rules is True:
                        df['log'] = list(map(add_message, df['log']))
                    error_msg = ' (rule_num {0}) {1} error: {2}'.format(index, rule[condition_column], e)
                    logger.exception('EXCEPTION {}'.format(error_msg))

            # Populate target columns as specified in split
            for column in target_columns:
                if rule[column] not in ['nan', '', 'None', None]:
                    if include_once is True:
                        df.loc[list(df_subset.index), column] = rule[column]
                    else:
                        df_subset.loc[list(df_subset.index), column] = rule[column]
            # The way we're doing this allows multiple matches if include_once is False.
            # Future: MAY be a reason to allow first-in wins or last-in wins, or ALL win.
            # MIKE look here.
            # df_final = pd.concat([df_final, df_subset])
            matched_length = len(df_subset)
            if verbose:
                logger.debug('{} - matched length'.format(matched_length))

            if include_once:
                # Exclude the records of the current split from exposure to subsequent filters.
                df.loc[list(df_subset.index), 'include'] = False
            else:
                if matched_length > 0:
                    matches.append(df_subset)

            summary_record = {
                'row_num': row_num,
                iteration_column: iteration,
                'input_records': input_length,
                'matched_records': matched_length,
            }
            summary_record.update(rule)
            summary.append(summary_record)
            row_num += 1

        if include_once is False:
            if len(matches) > 0:
                df = pd.concat(matches)
            else:
                df = pd.DataFrame().reindex(columns=df.columns)
        # unmatched record:
        unmatched_length = len(df[df['include'] == True])
        summary.append({
            'row_num': row_num,
            iteration_column: iteration,
            'input_records': unmatched_length,
            'matched_records': unmatched_length,
            'rule': unmatched_rule
        })
        row_num += 1

    df_summary = pd.DataFrame.from_records(summary)
    if show_rules is True:
        df.drop(columns=['include'], inplace=True, errors='ignore')
    else:
        df.drop(columns=['include', 'rule_number', 'rule', 'rule_id'], inplace=True, errors='ignore')
    return [df, df_summary]


def memoize(fn):
    cache = fn.cache = {}

    @wraps(fn)
    def memoizer(*args, **kwargs):
        key = str(args) + str(kwargs)
        if key not in cache:
            cache[key] = fn(*args, **kwargs)
        return cache[key]
    return memoizer


def trailing_negative(value, default=0):
    """Attempts to handle the trailing negative issue in a more performant way

    Args:
        value (str, int, or float): The value to clean
        default (float or int, optional): A default value to return if `value`
            cannot be cleaned

    Returns:
        float: The cleaned version of `value`, or `default`
    """
    try:
        return float(value)
    except:
        try:
            return -float(value[:-1])
        except:
            return default


def now(offset=0):
    """Returns the current date/time, with an optional offset

    Args:
        offset (int, optional): The offset to apply to the current time

    Returns:
        `utc.timestamp`: The current date/time, with `offset` applied
    """
    dt = utc.timestamp()

    if offset != 0:
        off = datetime.timedelta(hours=offset)
        dt = dt + off

    return dt


def concat(left_frame, right_frame):
    """Concatenate two DataFrames

    Args:
        left_frame (`pandas.DataFrame`): The first frome to concat
        right_frame (`pandas.DataFrame`): The second frame to concat

    Returns:
        `pandas.DataFrame`: The concatenation of `left_frame` and `right_frame`
    """
    return pd.concat(left_frame, right_frame)


def covariance(df, columns, min_observations=0):
    """Compute pairwise covariances among the series in the DataFrame, also excluding NA/null values

    Args:
        df (`pd.DataFrame`): The dataframe to compute on
        columns (None): DEPRECATED - Columns are now determined from `df`
        min_observations (int, optional): Minimum observations, defaults to `0`
    """
    df = df.columns

    if min_observations is not None and min_observations > 0:
        return df.cov(min_periods=min_observations)
    else:
        return df.cov()


def correlation(first_series, second_series, method='pearson'):
    return first_series.corr(second_series, method=method)


def apply_agg(df, group_by, column_operations):
    """Pass in a dict of key values for columns and operations

    {'A': 'sum', 'B': 'std', 'C': 'mean'}

    Args:
        df (`pandas.DataFrame`): The dataframe to apply aggregation to
        group_by (str): The group by operation to apply to `df`
        column_operations (dict): The operations to apply and on which columns

    Returns:
        `pandas.DataFrame`: `df` with aggregation applied
    """

    # Taken from operations that can be performed on columns as described here:
    # http://pandas.pydata.org/pandas-docs/dev/generated/pandas.DataFrame.html
    valid_operations = (
        'mean', 'median', 'mode',
        'sum', 'std', 'var', 'size', 'first', 'last', 'prod', 'product',
        'min', 'max', 'abs', 'quantile',
        'skew',  # Return unbiased skew over requested axis
        'kurtosis',  # Return unbiased kurtosis over requested axis
        'mad',  # Return the mean absolute deviation of the values for the requested axis
        'cumsum',  # Return cumulative sum over requested axis
        'cummin',  # Return cumulative min over requested axis.
        'cummax',  # Return cumulative max over requested axis.
        'cumprod'  # Return cumulative prod over requested axis.
    )

    final = {}
    for co in column_operations.keys():
        if column_operations[co] in valid_operations:
            final[co] = column_operations[co]

    return df.groupby(group_by).agg(final).reset_index()


def distinct(df, columns=None, keep='first', inplace=False):
    """Removes duplicate items from columns

    Args:
        df (`pandas.DataFrame`): The DataFrame to operate on
        columns (list, optional): Specific columns to operate on
        keep (str, optional): Which row containing duplicate values to keep.
            defaults to `'first'`
        inplace (bool, optional): Should duplicate values be removed from the
            source `df`? Defaults to `False`

    Returns:
        `pandas.DataFrame`: The `df` with duplicate values removed
    """
    return df.drop_duplicates(subset=columns,
                              keep=keep,
                              inplace=inplace)


def find_duplicates(df, columns=None, take_last=False):
    """Locates duplicate values in a dataframe

    Args:
        df (`pd.DataFrame`): The DataFrame to find duplicates in
        columns (`list`, optional): Specific columns to find duplicates in
        take_last (bool, optional): Should the last duplicate not be marked as a duplicate?
            Defaults to `False`

    Returns:
        `pd.DataFrame`: A frame containing duplicates
    """
    mask = df.duplicated(
        subset=columns,
        keep='last' if take_last else 'first',
    )
    return df.loc[mask]


def sort(df, sort_columns, sort_directions=True):
    """Sorts a dataframe

    wraps `pandas.DataFrame.sort_values()`

    Args:
        df (`pandas.DataFrame`): The dataframe to sort
        sort_columns (list): A list of the columns to sort on
        sort_directions (bool, optional): `True` to sort ascending, `False`
            to sort descending

    Returns:
        `pandas.DataFrame`: `df` sorted by the provided columns
    """
    return df.sort_values(sort_columns, ascending=sort_directions)


def replace_column(df, column, replace_dict):
    """cdystonia2.treat.replace({'a':{'Placebo': 0, '5000U': 1, '10000U': 2}})

    Args:
        df (`pandas.DataFrame`): The dataframe to replace in
        column (str): Which column to replace values in
        replace_dict (dict): What values to replace, and what to replace them with

    Returns:
        `pandas.dataframe`: `df` with the values replaced
    """

    return df.replace({column: replace_dict})


def replace(df, replace_dict):
    """Replaces values in columns based on a dict

    Args:
        df (`pandas.DataFrame`): The dataframe to replace values in
        replace_dict (dict): A dict containing columns, the values to replace, and what to replace them with.
            Should be formatted like:
            {
                'column_a': {'$': '', ',':''},
                'column_b': {'bought': 1, 'sold': -1}
            }
    """
    return df.replace(replace_dict)


def reindex(df, columns):
    """Reindexes a dataframe

    wraps `pandas.DataFrame.reindex()`

    Args:
        df (`pandas.DataFrame`): The dataframe to reindex
        columns (:type:`list`): A list of the columns to reindex

    Returns:
        `pandas.DataFrame`: A reindexed version of `df`
    """

    return df.reindex(columns)


def rename_columns(df, rename_dict):
    """Renames columns based on `rename_dict`

    Args:
        df (`pandas.DataFrame`): The dataframe to rename columns in
        rename_dict (:type:`dict`): A dict in the format `{'old_name': 'new_name'}`
            to use to rename the columns

    Returns:
        `pandas.DataFrame`: `df` with renamed columns
    """

    return df.rename(columns=rename_dict)


def column_info(df):
    """Returns a list of columns and their datatypes

    Args:
        df (`pandas.DataFrame`): The dataframe to get info for

    Returns:
        :type:`list` of :type:`dict`: A list of dicts containing column ids and their
            Dtypes
    """

    return [
        {'id': column_name, 'dtype': str(data_type)}
        for column_name, data_type in df.dtypes.items()
    ]


def set_column_types(df, type_dict):
    """Sets the datatypes of columns in a DataFrame

    df3.astype('float32').dtypes

    Here are the date units:
    Code    Meaning Time span (relative)    Time span (absolute)
    Y   year    +/- 9.2e18 years    [9.2e18 BC, 9.2e18 AD]
    M   month   +/- 7.6e17 years    [7.6e17 BC, 7.6e17 AD]
    W   week    +/- 1.7e17 years    [1.7e17 BC, 1.7e17 AD]
    D   day +/- 2.5e16 years    [2.5e16 BC, 2.5e16 AD]
    And here are the time units:

    Code    Meaning Time span (relative)    Time span (absolute)
    h   hour    +/- 1.0e15 years    [1.0e15 BC, 1.0e15 AD]
    m   minute  +/- 1.7e13 years    [1.7e13 BC, 1.7e13 AD]
    s   second  +/- 2.9e12 years    [ 2.9e9 BC, 2.9e9 AD]
    ms  millisecond +/- 2.9e9 years [ 2.9e6 BC, 2.9e6 AD]
    us  microsecond +/- 2.9e6 years [290301 BC, 294241 AD]
    ns  nanosecond  +/- 292 years   [ 1678 AD, 2262 AD]
    ps  picosecond  +/- 106 days    [ 1969 AD, 1970 AD]
    fs  femtosecond +/- 2.6 hours   [ 1969 AD, 1970 AD]
    as  attosecond  +/- 9.2 seconds [ 1969 AD, 1970 AD]

    Args:
        df (`pandas.DataFrame`): The dataframe to set column types on
        type_dict (dict): A dict to set columns based on. In the format
            {'column': 'dtype'}

    Returns:
        `pandas.DataFrame`: `df` with the column types set
    """

    float_column_types = (
        'float16', 'float32', 'float64', 'numeric',
    )

    #Can't call this int_column_types, because there's a few other possibilities things listed
    #that we'll just send through to get the default Pandas.to_numeric() treatment for now.
    numeric_non_float_column_types = (
        'int8', 'int16', 'int32', 'int64',
        'uint8', 'uint16', 'uint32', 'uint64',
        'complex64', 'complex128',
        'bigint', 'smallint',
    )

    date_column_types = (
        'datetime64[as]', 'datetime64[fs]', 'datetime64[ps]', 'datetime64[ns]',
        'datetime64[us]', 'datetime64[ms]', 'datetime64[s]', 'datetime64[m]',
        'datetime64[h]', 'datetime64[D]', 'datetime64[W]',
        'datetime64[M]', 'datetime64[Y]',
        'timestamp', 'date', 'time'
    )

    timedelta_column_types = (
        'timedelta64[as]', 'timedelta64[fs]', 'timedelta64[ps]', 'timedelta64[ns]',
        'timedelta64[us]', 'timedelta64[ms]', 'timedelta64[s]', 'timedelta64[m]',
        'timedelta64[h]', 'timedelta64[D]', 'timedelta64[W]',
        'timedelta64[M]', 'timedelta64[Y]'
        'interval',
    )

    bool_column_types = (
        'bool',
        'Boolean',
    )

    string_column_types = {
        'object': 256,
        's8': 8,
        's16': 16,
        's32': 32,
        's64': 64,
        's128': 128,
        's256': 256,
        's512': 512,
        's1024': 1024,
        'text': 256,
    }

    no_convert_types = [
        'uuid',
        'json',
    ]

    category_column_types = (
        'category',
    )

    for td in type_dict.keys():
        dtype = dtype_from_sql(type_dict[td])
        try:
            if dtype in float_column_types:
                df[td] = pd.to_numeric(df[td], downcast='float')

            elif dtype in numeric_non_float_column_types:
                df[td] = pd.to_numeric(df[td])

            elif dtype in date_column_types:
                df[td] = pd.to_datetime(df[td])

            elif dtype in timedelta_column_types:
                df[td] = pd.to_timedelta(df[td])

            elif dtype in no_convert_types:
                # Do not convert.  Keep as is.
                pass

            elif dtype in list(string_column_types.keys()):
                # Keep whatever text is there
                pass

            elif dtype in bool_column_types:
                df[td] = df[td].astype('bool')
            elif dtype in category_column_types:
                df[td] = df[td].astype('category')
            else:
                raise Exception('Unknown dtype specified')
        except:
            logger.exception('EXCEPTION')
            err_msg = 'dtype conversion of {0} to {1} FAILED'.format(td, dtype)

            raise Exception(err_msg)

    return df


def drop_column(df, columns_to_drop):
    """ Removes columns from a DataFrame

    del df[name]

    Args:
        df (`pandas.DataFrame`): The dataframe to drop columns on
        columns_to_drop (:type:`list` of :type:`str`): A list of the columns
            to remove

    Returns:
        `pandas.DataFrame`: `df` with the provided columns removed
    """

    for ctd in columns_to_drop:
        del df[ctd]

    return df


def has_data(df):
    """Determines if a DataFrame has any data

    Args:
        df (`pandas.DataFrame`): A DataFrame to test

    Returns:
        bool: If `df` has any data
    """
    try:
        counts = [int(v) for v in df.count().values]
        if not counts:
            return False
        return max(counts) > 0
    except:
        return False


def convert_currency(
    df_data,
    df_rates,
    source_amount_column=None,
    target_amount_column=None,
    rate_field='RATE',
    source_frame_source_currency='CURRENCY_SOURCE',
    source_frame_target_currency='CURRENCY_TARGET',
    rates_frame_source_currency='CURRENCY_SOURCE',
    rates_frame_target_currency='CURRENCY_TARGET',
    intermediate_currency='USD',
):
    """
    Convert currency from unconverted amount to amount of target currency

    This function requires 4 parameters: rates_frame, source_amount_column, and target_amount_column.
    source_frame and rates_frame are pandas.df.
    Remaining parameters are all strings representing required or optional column names

    We are going to try to do a point-to-point lookup first.  If that fails, we will attempt to lookup
    from source to USD and then from USD to target, applying correct math along the way.

    TODO 1: We should optionally consider period, but for now we'll assume records of both tables pertain to the same period.
    TODO 2: Build test cases
    TODO 3: Build appropriate warnings for things like:
        Sending in source tables with columns that do not line up with expected source columns

    Args:
        df_data (`pandas.DataFrame`): The frame containing source data
        df_rates (`pandas.DataFrame`): A frame containing currency rates
        source_amount_column (str): Column of the `source_frame` containing pre-converted amount
        target_amount_column (str): Column name of the final converted amount
        rate_field (str, optional): Column name of column of the `rates_frame` containing the amount to convert
        source_frame_source_currency (str, optional): Column of source frame indicating currency of pre-converted amount
        source_frame_target_currency (str, optional): Column of source frame indicating currency of intended post-converted amount
        rates_frame_source_currency (str, optional): Column of source frame indicating currency of pre-converted amount
        rates_frame_target_currency (str, optional): Column of source frame indicating currency of intended post-converted amount
        intermediate_currency (str, optional):

    Returns:
        `pandas.DataFrame`: The results of the lookup
    """

    periods_in_use = set(df_data['PERIOD'])

    # Set intermediate currency for 2-step currency conversion
    df_data['CURRENCY_INTERMEDIATE'] = intermediate_currency

    if source_frame_source_currency != 'CURRENCY_SOURCE':
        df_data['CURRENCY_SOURCE'] = df_data[source_frame_source_currency]
        delete_curr_src_col = True #Clean up after ourselves so we don't send extra columns back that wouldn't be expected.
    else:
        delete_curr_src_col = False

    if source_frame_target_currency != 'CURRENCY_TARGET':
        df_data['CURRENCY_TARGET'] = df_data[source_frame_target_currency]
        delete_curr_tgt_col = True #Clean up after ourselves so we don't send extra columns back that wouldn't be expected.
    else:
        delete_curr_tgt_col = False

    df_rates = df_rates[df_rates['PERIOD'].isin(periods_in_use)]

    df_rates['CURRENCY_SOURCE'] = df_rates[rates_frame_source_currency]
    df_rates['CURRENCY_TARGET'] = df_rates[rates_frame_target_currency]
    df_rates[rate_field] = df_rates[rate_field]
    df_rates['RATE_TYPE'] = '1' #Loaded rates are preferred

    df_rates = df_rates[[
        'PERIOD', #object
        rate_field, #float64
        'CURRENCY_SOURCE', #object
        'CURRENCY_TARGET', #object
    ]]

    df_rates_flipped = df_rates.copy(deep=True)
    df_rates_flipped['CURRENCY_SOURCE_old'] = df_rates_flipped['CURRENCY_SOURCE']
    df_rates_flipped['CURRENCY_TARGET_old'] = df_rates_flipped['CURRENCY_TARGET']
    df_rates_flipped['CURRENCY_SOURCE']     = df_rates_flipped['CURRENCY_TARGET_old']
    df_rates_flipped['CURRENCY_TARGET']     = df_rates_flipped['CURRENCY_SOURCE_old']
    df_rates_flipped[rate_field] = 1.0 / df_rates_flipped[rate_field]
    del df_rates_flipped['CURRENCY_SOURCE_old']
    del df_rates_flipped['CURRENCY_TARGET_old']
    df_rates_flipped['RATE_TYPE'] = '2' #Flipped rates are to be used only if loaded rates do not exist

    currencies_in_use = list(set(list(set(df_rates['CURRENCY_SOURCE'])) +  list(set(df_rates['CURRENCY_TARGET']))))
    currencies_in_use.sort()

    df_rates_passthrough_wip = pd.DataFrame({'CURRENCY_SOURCE' : currencies_in_use})
    df_rates_passthrough_wip['CURRENCY_TARGET'] = df_rates_passthrough_wip['CURRENCY_SOURCE']
    df_rates_passthrough_wip[rate_field]=1.0
    df_rates_passthrough_wip['RATE_TYPE'] = '3'

    df_rates_passthrough = pd.DataFrame()

    #Create passthrough rates for each period.
    for period in periods_in_use:
        df_rates_passthrough_chunk = df_rates_passthrough_wip.copy(deep=True)
        df_rates_passthrough_chunk['PERIOD'] = period
        df_rates_passthrough = pd.concat([df_rates_passthrough, df_rates_passthrough_chunk])


    df_rates = pd.concat([df_rates, df_rates_flipped, df_rates_passthrough])

    #Keep loaded rates rather than flipped rates if both exist for a given currency
    df_rates.sort_values(['RATE_TYPE'], ascending=[True], inplace=True)
    df_rates = distinct(df_rates, columns=['CURRENCY_SOURCE', 'CURRENCY_TARGET'], keep='first')

    #If multiple rates exist, keep only the one from the most-recent period.
    #df_rates.sort_values(['PERIOD'], ascending=[False], inplace=True)
    #df_rates = distinct(df_rates, columns=['CURRENCY_SOURCE', 'CURRENCY_TARGET'])

    df_data = lookup(
        df_data,
        df_rates,
        ['CURRENCY_SOURCE','CURRENCY_TARGET'],
        ['CURRENCY_SOURCE', 'CURRENCY_TARGET'],
        keep_columns=rate_field,
        exclude_duplicate_columns=False,
        keep="first",
        inplace=False
    )

    df_data.rename(
        columns={
            rate_field   : 'RATE_1', #for 1-step conversion
        }, inplace = True
    )


    df_data = lookup(
        df_data,
        df_rates,
        ['CURRENCY_SOURCE','CURRENCY_INTERMEDIATE'],
        ['CURRENCY_SOURCE', 'CURRENCY_TARGET'],
        keep_columns=rate_field,
        exclude_duplicate_columns=False,
        keep="first",
        inplace=False
    )

    df_data.rename(
        columns={
            rate_field   : 'RATE_2a', #for 2-step conversion
        }, inplace = True
    )

    df_data = lookup(
        df_data,
        df_rates,
        ['CURRENCY_INTERMEDIATE', 'CURRENCY_TARGET'],
        ['CURRENCY_SOURCE', 'CURRENCY_TARGET'],
        keep_columns=rate_field,
        exclude_duplicate_columns=False,
        keep="first",
        inplace=False
    )

    df_data.rename(
        columns={
            rate_field   : 'RATE_2b', #for 2-step conversion
        }, inplace = True
    )

    def get_effective_rate(rate_1, rate_2a, rate_2b):
        rate_1 = dh.cast_as_float(rate_1)
        rate_2a = dh.cast_as_float(rate_2a)
        rate_2b = dh.cast_as_float(rate_2b)

        if rate_1 != 0.0:
            #We have a direct point-to-point rate between source and target
            return rate_1
        else:
            #We do not have a direct point-to-point rate between source and target, so we try to pass through intermediate currency.
            return rate_2a * rate_2b

    df_data[rate_field] = list(map(get_effective_rate, df_data['RATE_1'], df_data['RATE_2a'], df_data['RATE_2b']))

    del df_data['CURRENCY_INTERMEDIATE']
    del df_data['RATE_1']
    del df_data['RATE_2a']
    del df_data['RATE_2b']

    if source_amount_column and target_amount_column:
        df_data[target_amount_column] = df_data[source_amount_column] * df_data[rate_field]

    if delete_curr_src_col:
        del df_data['CURRENCY_SOURCE']

    if delete_curr_tgt_col:
        del df_data['CURRENCY_TARGET']

    #Note, We return the effective rate that was used to do the conversion.
    #We will also return a converted amount if a source amount was passed & target converted amount field name were passed in.

    #TODO: We could send back a diagnostic field that shows HOW we arrived at the currency rate
    #TODO: This possibly could be converted to 2 methods, one to find the rate and another to apply it, with the 2nd optionally calling the first.
    return df_data


def lookup(source_frame, lookup_frame, left_on, right_on=None, keep_columns=None, exclude_duplicate_columns=False, keep="first", inplace=False):
    """Keeps all data from left frame and any matches in right using the on_columns.

    This function requires 3 parameters: source_frame, lookup_frame, and left_on.
    source_frame and lookup_frame are pandas.df.
    left_on, right_on, keep_columns are all lists.
    Remaining optional parameters are all bool.

    Args:
        source_frame (`pandas.DataFrame`): The frame containing source data
        lookup_frame (`pandas.DataFrame`): A frame containing data to look up
        left_on (str): The on clause to apply to the `source_frame`
        right_on (str, optional): The on clause to apply to the `lookup_frame`
        keep_columns (:type:`list` of :type:`str`): A list of columns to keep in the result
        exclude_duplicate_columns (bool, optional): if duplicate columns should be excluded. Defaults
            to `False`
        keep (str, optional): Which duplicate to use, defaults to `'first'`
        inplace (bool, optional): Should the input frame be modified? Defaults ot `False`

    Returns:
        `pandas.DataFrame`: The results of the lookup
    """

    if right_on is None:
        right_on = left_on

    if not isinstance(left_on, list):
        left_on = [left_on]

    if not isinstance(right_on, list):
        right_on = [right_on]

    drop_columns = []

    # optional arg to remove duplicate columns from lookup table
    if exclude_duplicate_columns is True:
        for col in source_frame:
            if col in lookup_frame:
                drop_columns.append(col)

    # optional arg to specify which columns in lookup table to keep
    if keep_columns is not None:
        for i in list(lookup_frame):
            if i in keep_columns:
                pass
            else:
                # column not being kept... append to list to drop cols
                if i not in drop_columns:
                    drop_columns.append(i)

    # have to keep right_on match keys for lookup operation to work
    for i in right_on:
        if i in drop_columns:
            drop_columns.remove(i)

    # cannot modify the actual lookup frame or it will blow up the original.
    # make a copy first.
    trimmed_lookup_frame = lookup_frame.copy()

    # drop columns from lookup table
    if len(drop_columns) > 0:
        trimmed_lookup_frame = drop_column(trimmed_lookup_frame, drop_columns)

    # default for drop_duplicates. Effectively returns the 1st result and drops others
    # moved to param default
    # take_last = False

    # default for drop_duplicates. drop them in place, do not return a copy
    # moved to param default
    # inplace = False

    # ensure that lookup table is distinct based on lookup values
    distinct_lookup_frame = distinct(trimmed_lookup_frame, right_on, keep, inplace)

    df = left_join(source_frame, distinct_lookup_frame, left_on, right_on)

    # for cases in which left_on != right_on, delete the right_on cols which vary

    if set(left_on) != set(right_on):
        limit = len(left_on)
        iter = 0
        while iter < limit:
            if left_on[iter] != right_on[iter]:
                try:
                    del df[right_on[iter]]
                except:
                    try:
                        # Pandas automatically adds _x and _y suffixes to data when a join causes a duplicate column.
                        # This behavior is not desirable in the case of the plaidtools lookup function.  When this edge
                        # case occurs, the first table's matching column and column name should be preserved and the second
                        # table's matching column name should be dropped.
                        col_x = right_on[iter] + '_x'
                        col_y = right_on[iter] + '_y'
                        del df[col_y]
                        df.rename(
                            columns={
                                col_x: right_on[iter]
                            }, inplace=True
                        )
                    except:
                        pass
            iter += 1

    return df


def coalesce(first_col, *subsequent_cols, **kwargs):

    """Return the first non-null value for each row, from each column (Series).
    This emulates the common SQL function COALESCE efficiently in Pandas.

    The Series are evaluated for null values in the order provided to the
    function, i.e. coalesce(a, b, c) will first check `a` for non-nulls, then
    `b`, then `c`. There is no limit on the number of Series supported.

    The returned Series will have an index matching that of `first_col`.
    Subsequent Series provided must match the length of `first_col`.

    The only keyword argument currently accepted is `consider_null`, an
    iterable of values to consider null in addition to NaN for numeric arrays
    and NaN/None for object arrays. Because numpy doesn't support cross-type
    comparison, neither does this function, so each value in `consider_null`
    must have the same type as the columns provided, lest numpy raises a
    TypeError. If no addition null values are provided (the default), the
    columns can be different types.

    Args:
        first_col (Series): An index to maintain in the result
        subsequent_cols (*args): Variable list of additional columns

    Returns:
        `pandas.Series`: The results of a COALESCE
    """

    if len(subsequent_cols) == 0:
        return first_col[:]

    # First things first: check that all of the Series are of the same length.
    for arg_num, col in enumerate(subsequent_cols, 1):
        try:
            assert len(col) == len(first_col)
        except AssertionError:
            raise Exception("Argument column %d not as long as first_col",
                            arg_num)

    # Initialize a Series of NaNs matching the index of the first column.
    # This Series will be filled in with non-nulls as they are found.
    result = pd.Series(index=first_col.index)

    consider_null = kwargs.get('consider_null', [])

    coalesce_cols = [first_col] + list(subsequent_cols)
    for col in coalesce_cols:
        # Make all indices match for .loc use later. They will be changed back.
        # The lengths were already compared to be equal. If the indexes are
        # equal, this is essentially a no-op.
        old_col_index = col.index
        col.index = first_col.index

        # Find the indices of the NaNs in the remaining result Series.
        to_fill = result.isnull()
        if not to_fill.any():
            # There are no more updates to apply. End the loop early.
            break

        # Find the indices of the non-nulls in the Series being used to fill
        # result, which is anything that isn't NaN or equal to a value in the
        # optional kwarg list consider_null.
        fill_with = to_fill & col.notnull()  # pylint: disable=no-member
        for other_null_val in consider_null:
            try:
                fill_with = fill_with & (col != other_null_val)
            except TypeError:
                raise TypeError(("Cannot compare {!r} with column type {}. "
                                 "Any consider_null values must compare with "
                                 "all column dtypes.")
                                .format(other_null_val, col.dtype))  # pylint: disable=no-member

        fill = to_fill & fill_with
        result.loc[fill] = col.loc[fill]  # pylint: disable=no-member

        # Set the old index back in place as to not modify any args.
        col.index = old_col_index

    return result


def summarize(df, group_by_columns, summarize_columns):
    """Group and aggregate DataFrame based on parameters and data types.

    This is a wrapper function to df.groupby which supports a small subset
    because of its intent to be used in specific summarization operations. It
    can be used to group by columns and aggregated in two different modes based
    on the data types of the aggregation columns: numeric columns are only
    summed, and object columns are only aggregated by count of unique items.

    Args:
        df (`pandas.DataFrame`): The DataFrame to summarize
        group_by_columns (:type:`list` of :type:`str`): Columns to GROUP BY
        summarize_columns(:type:`list` of :type:`str`): Columns to Summarize on

    Returns:
        `pandas.DataFrame`: The results of the summarize
    """

    if isinstance(group_by_columns, str):
        group_by_columns = [group_by_columns]
    if isinstance(summarize_columns, str):
        summarize_columns = [summarize_columns]
    all_cols = list(group_by_columns) + list(summarize_columns)
    try:
        assert len(all_cols) == len(set(all_cols))
    except AssertionError:
        raise Exception('group_by_columns and summarize_columns cannot share '
                        'any columns.')
    df = df[all_cols]

    # Mike *slightly* disagrees with the way that Pandas handles groupby with
    # NaN values, so replace them with '' or 0 depending on the dtype. This
    # includes both the groupby columns and the aggregation columns.
    agg_map = {}
    for col, col_dtype in df.dtypes.items():
        if col_dtype == np.dtype(object):
            df[col] = df[col].fillna('')
            if col in summarize_columns:
                agg_map[col] = pd.Series.nunique
        else:
            if col in group_by_columns:
                # All columns used in the groupby must be converted to string
                # because of how this function is intended to be used.
                df[col] = df[col].astype(str)
            else:
                df[col] = df[col].fillna(0)
                try:
                    # Prefer int if it's possible to convert to it.
                    df[col] = df[col].astype(int)
                except ValueError:
                    # It can't be converted to int. Let it be.
                    pass
                agg_map[col] = np.sum

    return df.groupby(group_by_columns).agg(agg_map).reset_index()


def json_to_csv(json_file_name, csv_file_name, columns=None, writeheader=True):
    """Converts a JSON file to a CSV file

    Args:
        json_file_name (str): The name of the input JSON file
        csv_file_name (str): The name of the output CSV file
        columns (list, optional): A list of columns to keep in the CSV file
        writeheader (bool, optional): Whether or not to write the header
    """
    with open(json_file_name, 'rb') as json_file:
        j = json.loads(json_file.read())
        if columns is None:
            columns = list(get_json_columns(j))
        with open(csv_file_name, 'w') as csv_file:
            wr = csv.DictWriter(
                csv_file,
                columns,
                extrasaction='ignore',
                delimiter='\t',
                quotechar='"',
                escapechar='"'
            )
            if writeheader:
                wr.writeheader()
            wr.writerows(j)


def get_json_columns(json_file_or_dict, check_row_count=1):
    """Determines the names of columns in a JSON file

    Args:
        json_file_or_dict (str or dict): The file name of the JSON file, or a dict
        check_row_count (int, optional): The number of rows to use to get columns

    Returns:
        list of str: A list of the column names
    """
    # Likely to be pretty slow. Hopefully only runs on guess?
    columns = set()
    if isinstance(json_file_or_dict, str):
        with open(json_file_or_dict, 'r') as json_file:
            j = json.loads(json_file.read())
    else:
        j = json_file_or_dict

    for index, record in enumerate(j):
        if index >= check_row_count:
            break
        columns = columns | set(record.keys())

    return columns


def precision_and_scale(x):
    max_digits = 14
    int_part = int(abs(x))
    magnitude = 1 if int_part == 0 else int(math.log10(int_part)) + 1
    if magnitude >= max_digits:
        return (magnitude, 0)
    frac_part = abs(x) - int_part
    multiplier = 10 ** (max_digits - magnitude)
    frac_digits = multiplier + int(multiplier * frac_part + 0.5)
    while frac_digits % 10 == 0:
        frac_digits /= 10
    scale = int(math.log10(frac_digits))
    return magnitude + scale, scale


def get_formatted_number(val):
    """Formats a number with least possible decimal places

    Args:
        val (float): Number to be formatted

    Returns:
        str: The str representation of the number

    Examples:
        >>> get_formatted_number(3.345e-09)
        '0.000000003345'
        >>> get_formatted_number(3.345e09)
        '3345000000'
        >>> get_formatted_number(1234567.89)
        '1234567.89'

    """
    precision, scale = precision_and_scale(val)
    if precision > 14 and int(val) == val:
        return f'{int(val):d}'
    return f'{val:.{scale}f}'


def excel_to_csv(excel_file_name, csv_file_name, sheet_name='sheet1', has_header=True, skip_rows=0):
    """Converts an excel file to a CSV file

    Args:
        excel_file_name (str): The name of the input Excel file
        csv_file_name (str): The name of the output CSV file
        sheet_name (str, optional): The name of the sheet to use. Defaults to `'sheet1'`
        has_header (bool, optional): The file has a header row
        skip_rows (int, optional): The number of rows to skip at the top of the file
    """
    df = pd.read_excel(
        excel_file_name,
        sheet_name=sheet_name,
        header=0 if has_header else None,
        engine="calamine",
        skiprows=skip_rows,
    )
    df.to_csv(
        csv_file_name,
        index=False,
        date_format='%Y-%m-%dT%H:%M:%S',
    )


def fixedwidth_to_csv(fixed_width_file_name, csv_file_name, colspecs):
    """Converts an fixed width file to a CSV file

    Args:
        fixed_width_file_name (str): The name of the input Fixed Width file
        csv_file_name (str): The name of the output CSV file
        colspecs (list): List of the column widths
    """
    df = pd.read_fwf(fixed_width_file_name, colspecs=colspecs)
    df.to_csv(
        csv_file_name,
        index=False,
        sep='\t',
        quotechar='"',
        escapechar='"',
    )


def avro_to_csv(avro_file_name: str, csv_file_name: str, start_row: int = 0, date_format: str = 'YYYY-MM-DD"T"HH:MI:SS'):
    with open(avro_file_name, 'r') as infile:
        with open(csv_file_name, 'w') as outfile:
            reader = fastavro.reader(infile)
            if start_row:
                # Somewhat ugly since fastavro doesn't have a clean way to do this.
                lines = 0
                for _ in reader:
                    lines += 1
                    if lines >= start_row:
                        break
            # write header
            header = [field['name'] for field in json.loads(reader.metadata['avro.schema'])['fields']]
            writer = csv.DictWriter(
                outfile,
                fieldnames=header,
                delimiter='\t',
                quotechar='"',
                escapechar='"',
            )
            writer.writeheader()
            writer.writerows(reader)


def parquet_to_csv(parquet_file_name: str, csv_file_name: str, start_row: int = 0, date_format: str = 'YYYY-MM-DD"T"HH:MI:SS'):
    df = pd.read_parquet(parquet_file_name) # should use pyArrow
    if start_row:
        df = df.iloc[start_row:]
    df.to_csv(
        csv_file_name,
        index=False,
        sep='\t',
        quotechar='"',
        escapechar='"',
        date_format=postgres_to_python_date_format(date_format),
    )


def allocate(
    logging,                     # pass in the logger we want to hit.
    df_input,                     # input data frame
    df_driver,                     # driver data frame
    input_data,                 # Specify col(s) to allocate.
    input_keys,                 # Specify input join columns.
    driver_data,                 # Specify col with driver data.
    driver_numerator_keys,        # Specify numerator join col(s).
    driver_denominator_keys,    # Specify denominator join col(s).
    ):
    """Allocates a given set of input data based on a driver contained in a set of driver data, subject to
    limiting conditions. Expressed as hierarchy filters.

    Args:
        logging (logger): Logger
        df_input (pandas.Dataframe): Input data frame
        df_driver (pandas.Dataframe): Driver data frame
        input_data (str or list): column(s) of input data that should be allocated
        input_keys (str or list): column(s) in input data to be joined with driver data
        driver_data (str): column containing driver values
        driver_numerator_keys (str or list): Driver data numerator join columns
        driver_denominator_keys (str or list): Driver data denominator join columns

    """
    # Remove any unused columns from the driver data
    df_driver.drop(columns=[
        col for col in df_driver.columns if col not in driver_numerator_keys + driver_denominator_keys + [driver_data]
    ], inplace=True)

    #20180422 Make this more user-friendly.  #allocable needs to be added in if it's not already there.  It's *not* required in order to call the function.
    if 'allocable' not in df_input.columns:
        df_input['allocable'] = 1

    def alloc_status(driver_value):
        """
        0: Stranded cost
        1: Allocated cost

        This is used internally in allocate()
        """
        if math.isnan(driver_value) or driver_value == 0:
            return 0
        else:
            return 1

    def shred(coeff, numerator, denominator, allocable):
        """Apply split ratio to coefficient.

        Args:
            coeff (int): The coefficient to apply a split ratio to
            numerator (int): The numerator of the ratio
            denominator (int): The denominator or the ratio
            allocable (int): 1 If this is allocable.

        Returns:
            int: The value after shredding
        """
        # This can be refactored to be smarter & more efficient.  Also, first if clause is new. Need to make
        # sure it does not break something.
        if allocable == 1:
            if numerator < 0:
                pass

            if math.isnan(numerator) or numerator == 0:
                return 0
            if math.isnan(denominator) or denominator == 0:
                return coeff  # This is for passing stranded costs through to result set.
            else:
                try:
                    return coeff * numerator / denominator
                except ValueError:
                    return 0
        else:
            # Not Allocable.  Pass input through to output.
            return coeff

    def _clean_names(frame):
        """
        Delete input columns "foo__in" from frame.
        Clean output columns "foo__out" -> "foo"
        """
        col_list = list(frame.columns)

        for col in col_list:
            if col.endswith('__value'):
                del frame[col]
            elif col.endswith('__split'):
                frame.rename(columns={col: col[:-7]}, inplace=True)

        return frame

    #OUTPUT_PATH_EXCEL = 'C:/UBS/Dev/models/debug/allocate.xlsx'
    #xlsx = pd.ExcelWriter(OUTPUT_PATH_EXCEL)

    #0.) Set nan driver values to zero
    #df_driver[driver_data].fillna(0, inplace=True) #Not this

    original_driver_col = driver_data
    driver_data = '__DRIVER_VALUE__'
    df_driver.rename(columns={
        original_driver_col: driver_data
    }, inplace=True)
    df_driver = df_driver[(np.isfinite(df_driver[driver_data])) & (df_driver[driver_data] != 0)]

    if not isinstance(input_keys, list):
        input_keys = [input_keys]

    if not isinstance(driver_numerator_keys, list):
        driver_numerator_keys = [driver_numerator_keys] #If a single column is specified, convert to 1-item list.

    if not isinstance(driver_denominator_keys, list):
        driver_denominator_keys = [driver_denominator_keys] #If a single column is specified, convert to 1-item list.

    driver_numerator_and_denominator_keys = []
    driver_numerator_and_denominator_keys.extend(driver_numerator_keys)
    driver_numerator_and_denominator_keys.extend(driver_denominator_keys)

    #0.1) Build table w/ numerators...should be same as input table unless
    #      We are summarizing to get to numerator level.

    agg_items = {}
    #agg_items[driver_data] = 'sum' # 20150324 comment out MWR

    lookup_passthrough = []

    for item in df_driver.columns:
        if item not in driver_numerator_and_denominator_keys:
            if np.issubdtype(df_driver[item], np.number):
                agg_items[item] = 'sum'
            else:
                lookup_passthrough.append(item)

        if len(df_driver) > 0 and item in driver_numerator_and_denominator_keys and is_string_dtype(df_driver[item]):
            # cast null string values to string so that subsequent groupby statement doesn't drop records
            df_driver[item] = list(map(dh.cast_as_str, df_driver[item]))

    #This is for safely (without any chance of dupes or buggering up our primary group-by) passing through extra masterdata string columns.
    lookup_passthrough.extend(driver_numerator_and_denominator_keys)
    df_lookup_passthrough_masterdata = df_driver.groupby(lookup_passthrough).agg({driver_data: 'sum'}).reset_index()

    #20180422 https://stackoverflow.com/questions/44123874/dataframe-object-has-no-attribute-sort
    df_driver = df_driver.groupby(
        driver_numerator_and_denominator_keys
        ).agg(
            agg_items
        ).sort_values(
            [
                driver_data,
            ],
            ascending=[0]
        ).reset_index()

    # Knock out records with driver_data = 0. It happens. We don't want it. It fouls things up.
    # 20150324 MWR We sure about this? Would make results set bigger, which we might want for completeness. (although I still generally agree)
    # 20150325 Agreeing w/ my comment from 0324 here.  Maybe we keep this around for comparison vs. alt passthrough (driver) data.
    # Turning off for now.  If this blows up our data set too much, we can turn it back on.
    # df_driver = df_driver[df_driver[driver_data]!=0]

    df_driver = lookup(
        df_driver,
        df_lookup_passthrough_masterdata,
        driver_numerator_and_denominator_keys,
        driver_numerator_and_denominator_keys,
        exclude_duplicate_columns=True
    )

    # TODO: WE REALLY NEED TO SPLIT AND HANDLE POSITIVES AND NEGATIVES SEPARATELY OR IT WILL BITE US HARD SOME DAY!
    # One split = 1,000,000. Another = -999,999. Driver Value = 1,000,000 + -999,999. Allocate a dollar and
    # get 1,000,000 to cost object A and -999,999 to cost object B.  This is bad.
    # This split would possibly start here in the code. Has to be throught through.


    #1.) Build denominators by creating summary table by join columns
    df_denominators = df_driver.groupby(
        driver_denominator_keys
    ).agg(
        {
            driver_data: 'sum',
        }
    ).sort_values(
        [
            driver_data,
        ],
        ascending=[0]
    ).reset_index()

    # 2.) Check driver sum
    #pre_sum = df_driver[driver_data].sum()
    #post_sum = df_denominators[driver_data].sum()
    #logging.debug('\nControl total: driver pre:post sum ' + num(pre_sum) + ' : ' + num(post_sum))
    #logging.debug(get_text_table(df_denominators, 5, 'df_denominators'))

    # 3.) Cost control total
    if not isinstance(input_data, list):
        input_data = [input_data]

    #for col in input_data:
    #    logging.debug('\nControl total: input to allocate ' + col + ': '+ num(df_input[col].sum()))

    #if not isinstance(driver_numerator_keys, list):
        #driver_numerator_keys = [driver_numerator_keys]

    # 4.) Rename numerators (driver data) to "split"
    driver_split = driver_data + '__split'
    df_driver.rename(columns={driver_data: driver_split}, inplace=True)

    # 5.) Join denominators
    df_driver = lookup(
        df_driver,
        df_denominators,
        driver_denominator_keys,
        driver_denominator_keys,
    )
    driver_value = driver_data + '__value'
    df_driver.rename(columns={driver_data: driver_value}, inplace=True)

    #Reorder columns. Put driver split and value at the front
    driver_cols = [driver_split, driver_value]
    for item in list(df_driver.columns):
        if item not in driver_cols:
            driver_cols.append(item)
    df_driver = df_driver[
        driver_cols
    ]

    #logging.debug('\nDriver Table w n/d splits...')
    #logging.debug(get_text_table(df_driver, 5, 'df_result'))

    #logging.debug('#5.) Join denominators')
    #logging.debug(get_text_table(df_driver, 5, 'drivers'))
    #logging.debug('Control total: numerators: ' + num(df_driver[driver_split].sum()))

    #7.) Only consider input records with keys matching allocable cost pools & driver split sums <> 0.

    #20150330 Could add 2nd condition check to say "If you don't have any non-zero driver data, you sir are NOT allocable.


    #TODO 1 : Temporarily add '__split_check_sum' column to df_input.  Look it up from df_temp_check.
    #         Lookup is on driver_denominator_keys

    df_driver_split_sum = df_driver.groupby(
        driver_denominator_keys
        ).agg(
            {
                driver_split: 'sum'
            }
        ).reset_index()

    #TODO 2 : Turn this pseudo-code into real code:
    df_input = lookup(
        df_input,
        df_driver_split_sum,
        driver_denominator_keys,
        driver_denominator_keys,
        keep_columns=[driver_split],
        exclude_duplicate_columns=True
    ) #19714,311

    # Beware any soul who enters here.  If ye seek refactor, test ye NaN driver_split.

    #df_temp_delete = df_input(math.isnan(df_input[driver_split]))

    df_input[driver_split] = list(map(dh.cast_as_float, df_input[driver_split]))
    df_input_allocable = df_input[(df_input['allocable'] == True) & (df_input[driver_split] != 0)]
    df_input_not_allocable = df_input[(df_input['allocable'] == False) | (df_input[driver_split] == 0)]

    df_input_not_allocable['allocable'] = False

    del df_input_allocable[driver_split]
    del df_input_not_allocable[driver_split]

    #TODO 3 : Drop our '__split_check_sum'.

    # TODO: At some point, we need to separate negative driver data from positive driver data, and
    # handle it appropriately.  Right now, mix of negative and positive driver data would really foul things
    # up. Not important when drive = email count, but it'd eventually bite us.
    # 8.) Join driver data to input costs

    df_result = left_join(
        df_input_allocable,
        df_driver,
        input_keys,
        driver_denominator_keys,
    )

    # 9.) Glue together allocable results and unallocable-by-design input data
    df_result = pd.concat([df_result, df_input_not_allocable])

    # 10.) Allocate costs
    # result_checksum_columns = input_keys

    #21051118 New Goodness Starts here.  We're going to shred ONE thing, (all ones) and multiply this new shred coefficient X all of
    # the cols we wish to shred.
    df_result['shred'] = 1
    df_result['shred'] = list(map(
        shred,
        df_result['shred'],
        df_result[driver_split],
        df_result[driver_value],
        df_result['allocable']
    ))
    #May not need this
    axis = list(set(df_result.columns) - set(input_data) - {'shred'})
    #df_result.set_index(axis)
    #df_result_2 = df_result.copy(deep=True)
    df_result[input_data] = df_result[input_data].multiply(df_result['shred'], axis='index')

    df_result['alloc_status'] = list(map(alloc_status, df_result[driver_value]))

    #df_result = df_result.sort_index(by=['alloc_status'], ascending=[False])

    df_result = df_result.sort_values(by=['alloc_status'], ascending=[False])

    df_result = _clean_names(df_result)

    return df_result


def save(frame, name, conn=None, append=False, chunk_size=500000):
    """Saves the frame to the specified name

    Args:
        frame (pd.DataFrame):
        name (str): Name and perhaps full path to a table
        conn (Connect object): plaid connect object
        append (bool): Append to existing table or truncate and rewrite
        chunk_size (int, optional):

    Returns:
        None
    """

    if not conn:
        try:
            rpc = Connect()
        except:
            return False
        #logger.debug('Connection found at: {0}'.format(rpc.cfg_path))
        conn = Connection(project=rpc.project_id)

    #logger.debug('Project ID: {0}'.format(conn.project_id))

    t = Table(conn, name, columns=[
        {
            'id': col,
            'dtype': analyze_type(dtype)
        }
        for col, dtype in zip(frame.columns, frame.dtypes)
    ])
    logger.debug('Table ID is: {0}'.format(t.id))
    conn.bulk_insert_dataframe(t, frame, append=append, chunk_size=chunk_size)
