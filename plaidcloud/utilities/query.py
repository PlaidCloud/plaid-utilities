import base64
import logging
import os
import tempfile
import uuid
import csv
from typing import Any, overload, NamedTuple

import pandas as pd
import numpy as np
import requests
from requests.adapters import HTTPAdapter
import sqlalchemy
from sqlalchemy.sql import selectable
from sqlalchemy.dialects import registry
from urllib3.util.retry import Retry
from urllib.parse import urlparse, urlunparse

from plaidcloud.rpc.database import PlaidDate, PlaidTimestamp
from plaidcloud.rpc.rpc_connect import Connect, PlaidXLConnect
from plaidcloud.rpc.type_conversion import sqlalchemy_from_dtype, pandas_dtype_from_sql, analyze_type
from plaidcloud.utilities import data_helpers as dh
from plaidcloud.utilities.remote.dimension import Dimensions
from plaidcloud.utilities.stringtransforms import apply_variables

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2024, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


logger = logging.getLogger(__name__)
SCHEMA_PREFIX = 'anlz'
TABLE_PREFIX = 'analyzetable_'

# We must override the default pandas na values to disallow 'NA'.
# We are doing this by setting our own list, rather than using pandas.io.common._NA_VALUES in
# order to future-proof, as pandas.io.common._NA_VALUES does not exist in pandas versions > 0.24.
_NA_VALUES = ('-1.#IND', '1.#QNAN', '1.#IND', '-1.#QNAN', '#N/A N/A', '#N/A',
              'N/A', 'n/a', '#NA', 'NULL', 'null', 'NaN', '-NaN', 'nan',
              '-nan', '')

class UDFParams(NamedTuple):
    source_by_name: dict[str, "Table"]
    sources: list["Table"]
    target_by_name: dict[str, "Table"]
    targets: list["Table"]
    variable_by_name: dict[str, str]
    variables: list[str]


class Connection:

    def __init__(self, project: str = None, rpc: [Connect, PlaidXLConnect] = None):
        """

        Args:
            project (str, optional): A Project Identifier
            rpc (Connect, PlaidXLConnect, optional): An RPC Connection object
        """
        if rpc:
            self.rpc = rpc
        else:
            self.rpc = Connect()

        self.dims = Dimensions(conn=self.rpc)

        if project:
            try:
                # See if this is a project ID already
                uuid.UUID(project)
                self._project_id = str(project)
            except ValueError:
                if '/' in project:
                    # This is a path lookup
                    self._project_id = self.rpc.analyze.project.lookup_by_full_path(path=project)
                else:
                    # This is a name lookup
                    self._project_id = self.rpc.analyze.project.lookup_by_name(name=project)
        else:
            self._project_id = rpc.project_id

        if self._project_id:
            self._project_name = self.rpc.analyze.project.project(project_id=self._project_id, keys=['name'])['name']

        _dialect_kind = self.rpc.analyze.query.dialect()

        try:
            dialect_cls = registry.load(_dialect_kind)
        except:
            dialect_cls = registry.load('postgresql')
        self.dialect = dialect_cls()
        self.variables = self.refresh_variables()
        self._load_udf_params()

    def _compiled(self, sa_query):
        """Returns SQL query for datastore dialect, in the form of a string, given a
        sqlalchemy query. Also returns a params dict."""
        compiled_query = sa_query.compile(dialect=self.dialect, compile_kwargs={"render_postcompile": True})
        logger.info(self.dialect.name)
        logger.info(str(compiled_query))
        return str(compiled_query).replace('\n', ''), compiled_query.params

    def get_csv(self, table_name, encoding="utf-8", clean=False):
        """Returns a file path to the entire table as a CSV file.

        Args:
            table_name (str): The table's full name, in "schema"."table" format
            clean (bool, optional): If set to True, will remove newlines and non-ascii characters
                                    from the resulting CSV.

        Returns:
            The CSV file streamed from PlaidCloud"""
        if clean:
            # Slice off the table from the full name.
            table_id = table_name.split('.')[1][1:-1]
            meta = self.rpc.analyze.table.table_meta(
                project_id=self._project_id,
                table_id=table_id
            )
            if meta is None:
                raise Exception('The table is not created in the database')

            if encoding != 'utf-8':
                replace_func = 'utf8_to_{}'.format(encoding.replace('-', '_').lower())
                column_list = [
                    """CONVERT(REPLACE(\"{col}\", '\\n', '') using {rep_func}) AS \"{col}\"""".format(
                        col=m['id'],
                        rep_func=replace_func
                    )
                    if m['dtype'] == 'text'
                    else '"{}"'.format(m['id'])
                    for m in meta
                ]
            else:
                column_list = [
                    """REPLACE(\"{col}\", '\\n', '') AS \"{col}\"""".format(
                        col=m['id'],
                    )
                    if m['dtype'] == 'text'
                    else '"{}"'.format(m['id'])
                    for m in meta
                ]
            column_string = ','.join(column_list)
            query = """SELECT {columns} FROM {table}""".format(
                columns=column_string,
                table=table_name,
            )
            return self.rpc.analyze.query.download_csv(
                project_id=self._project_id,
                query=query,
            )

        return self.rpc.analyze.query.download_csv(
            project_id=self._project_id,
            table_name=table_name,
        )

    def get_csv_by_query(self, query, params=None) -> str:
        """Returns a file path to the query results as a CSV file."""
        if isinstance(query, str):
            query_string = query
        else:
            query_string, params = self._compiled(query)

        return self.rpc.analyze.query.download_csv(
            project_id=self._project_id,
            query=query_string,
            params=params,
        )

    def get_iterator(self, table, preserve_nulls=True):
        """Returns a generator that yields each row as a dict."""
        return self._csv_stream(self.get_csv(table.fully_qualified_name), table.columns, preserve_nulls)

    def get_iterator_by_query(self, sa_query, preserve_nulls=True):
        """Returns a generator that yields each row as a dict."""
        query, params = self._compiled(sa_query)
        return self._csv_stream(self.get_csv_by_query(query, params), sa_query.selected_columns, preserve_nulls)

    def _csv_stream(self, file_name, columns, preserve_nulls):
        type_lookup = {c.name: c.type for c in columns}
        with open(file_name, 'r') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                for k, v in row.items():
                    # Keep the value as None if we want to preserve nulls.
                    if preserve_nulls and v is None:
                        pass
                    elif isinstance(type_lookup[k], sqlalchemy.types.Numeric):
                        row[k] = float(v or 0)
                    elif isinstance(type_lookup[k], sqlalchemy.types.Integer):
                        row[k] = int(v or 0)
                    elif any((
                        isinstance(type_lookup[k], sqlalchemy.types.DateTime),
                        isinstance(type_lookup[k], PlaidDate),
                        isinstance(type_lookup[k], sqlalchemy.types.Date),
                        isinstance(type_lookup[k], sqlalchemy.types.Time),
                    )):
                        row[k] = pd.to_datetime(v)
                    elif isinstance(type_lookup[k], sqlalchemy.types.Interval):
                        row[k] = pd.to_timedelta(v)

                yield row

    def get_data(self, data_source, return_type='df', encoding='utf-8', clean=True):
        if return_type == 'df':
            if isinstance(data_source, Table):
                return self.get_dataframe(data_source, encoding=encoding, clean=clean)
            if isinstance(data_source, sqlalchemy.sql.Select):
                return self.get_dataframe_by_query(data_source, encoding=encoding)
            if isinstance(data_source, str):
                return self.get_dataframe_by_querystring(data_source, encoding=encoding)
            raise Exception('Unknown type for Data Source {}'.format(repr(data_source)))
        elif return_type == 'csv':
            if isinstance(data_source, Table):
                return self.get_csv(data_source.fully_qualified_name, encoding=encoding, clean=clean)
            if isinstance(data_source, str):
                if str(data_source).lower().startswith('select'):
                    return self.get_csv_by_query(data_source)
                return self.get_csv(data_source, encoding=encoding, clean=clean)
            raise Exception('Unknown type for Data Source {}'.format(repr(data_source)))
        else:
            raise Exception('Unsupported type {} for get_data.'.format(return_type))

    def get_dataframe(self, table, encoding="utf-8", clean=True):
        """Returns a pandas dataframe representation of `table`

        Args:
            table (`plaidtools.query.Table`): Object representing desired table
            encoding (str, optional):
            clean (bool, optional): If set to true, newline characters and non-ascii
                                    characters will be removed from the resulting data

        Returns:
            `pandas.DataFrame`: A DataFrame representing the table and the data it contains"""
        file_path = self.get_csv(table.fully_qualified_name, encoding=encoding, clean=clean)

        try:
            return self._get_df_from_csv(file_path, table.columns, encoding)
        finally:
            try:
                os.remove(file_path)
            except Exception as e:
                # import traceback
                logger.warning('Failed to delete temporary file {}, {}.'.format(file_path, str(e)))

    def get_dataframe_by_query(self, sa_query, encoding='utf-8'):
        # TODO: Somehow get a list of column names/types from query arg to use with _get_df_from_csv.
        query, params = self._compiled(sa_query)
        file_path = self.get_csv_by_query(query, params)
        try:
            return self._get_df_from_csv(file_path, sa_query.selected_columns, encoding)
        finally:
            try:
                os.remove(file_path)
            except Exception as e:
                # import traceback
                logger.warning('Failed to delete temporary file {}, {}.'.format(file_path, str(e)))

    def get_dataframe_by_querystring(self, query, encoding='utf-8'):
        # TODO: Somehow get a list of column names/types from query arg to use with _get_df_from_csv.
        file_path = self.get_csv_by_query(query)
        try:
            return self._get_df_from_csv(file_path=file_path, encoding=encoding)
        finally:
            try:
                os.remove(file_path)
            except Exception as e:
                # import traceback
                logger.warning('Failed to delete temporary file {}, {}.'.format(file_path, str(e)))

    def _get_df_from_csv(self, file_path: str, columns=None, encoding='utf-8'):
        # TODO: Determine if converters are needed for various column types.
        # Examples:
        #   default null dates to a valid date (1900-01-01) for better parsing
        #   encode string values as ascii for backward compatibility
        falsey_strings = {'f', 'F', 'no', 'false', 'FALSE'}

        def to_bool(val):
            if val in falsey_strings:
                return True
            return bool(val)

        def to_timedelta(val):
            return pd.to_timedelta(val)

        converters = {}
        dtypes = {}
        parse_dates = []
        # raise Exception('\n'.join(['c.name: {}, c.type: {}'.format(repr(c.name), repr(c.type)) for c in columns]))
        if columns:
            for c in columns:
                if isinstance(c.type, sqlalchemy.types.Boolean):
                    converters[c.name] = to_bool
                elif any((
                    isinstance(c.type, sqlalchemy.types.Date),
                    isinstance(c.type, sqlalchemy.types.Time),
                    isinstance(c.type, sqlalchemy.types.DateTime),
                    isinstance(c.type, sqlalchemy.types.TIMESTAMP),
                    isinstance(c.type, PlaidTimestamp),
                    isinstance(c.type, PlaidDate),
                )):
                    # https://stackoverflow.com/a/37453925
                    parse_dates.append(c.name)
                elif isinstance(c.type, sqlalchemy.types.Interval):
                    converters[c.name] = to_timedelta
                else:
                    dtypes[c.name] = pandas_dtype_from_sql(c.type)

            # So, here's the why of all of this. Our expected behavior is that nulls become empty string in data frames if they are strings, and they
            # stay as nulls if they are other data types (floats, etc.)  We got this behavior out of the box in the old days, and thus stuff we built downstream
            # now expects it.  We need to keep it now, and do so as efficiently as possible.
            #
            # TODO: We should perhaps consider doing this differently to cause minimal rework of Null/NaN values and thus be more memory-efficient.
            # It would be good to try to read a set of object columns with keep_default_na = False and
            # a set of object columns with keep_default_na = True and then recombine them into 1 dataframe with all of the columns.
            # Pandas supports reading a subset of columns from a source file. See 'usecols' kwarg.
            # Probably not a big deal though. We're being memory-efficient with our non-object columns now (nans come in as Null now in numeric cols (float, int, etc)
            df = pd.read_csv(
                file_path,
                dtype=dtypes,
                parse_dates=parse_dates,
                converters=converters,
                encoding=encoding,
                keep_default_na=False,
                na_values=_NA_VALUES,
            )
            nan_overrides = {}

            # Create a list of string (object) columns and NaN override for each ('').
            for col in df.columns:
                if df[col].dtype == np.dtype('object'):
                    nan_overrides[col] = ''

            df = df.fillna(value=nan_overrides)  # pylint: disable=no-member

        else:
            # No column information is available.  Blind dataframe creation with implicit guessing.
            df = pd.read_csv(file_path, encoding=encoding, keep_default_na=False)

        return df

    def execute(self, query, params=None, return_df=False):

        if isinstance(query, str):
            query_string = query
        else:
            query_string, params = self._compiled(query)

        result = self.rpc.analyze.query.query(
            project_id=self._project_id, query=query_string, params=params,
        )

        if return_df:
            if isinstance(result, list):
                df = pd.DataFrame(result)
                return dh.clean_frame(df)
            else:
                return None
        else:
            return result

    def _get_table_columns(self, table_object):
        columns = table_object.cols()

        if columns:
            return [c['id'] for c in columns]
        return []

    def _load_csv(
        self, project_id, table_id, meta, csv_data, header, delimiter, null_as, quote, escape='\\',
        date_format='YYYY-MM-DD"T"HH24:MI:SS', source_columns=None, append=False, update_table_shape=True, compressed=True
    ):
        return self.rpc.analyze.table.load_csv(
            project_id=project_id,
            table_id=table_id,
            meta=meta,
            csv_data=csv_data,
            header=header,
            delimiter=delimiter,
            null_as=null_as,
            quote=quote,
            escape=escape,
            date_format=date_format,
            source_columns=source_columns,
            append=append,
            update_table_shape=update_table_shape,
            compressed=compressed,
        )

    def _load_parquet(self, project_id: str, table_id: str, append: bool=False, update_table_shape: bool=True, compression: str=None):
        pass

    def query(self, entities, **kwargs):
        """Declarative approach execution compiler"""
        #  TODO: compile query and execute based on declarative object chaining
        pass

    def bulk_save_objects(self, objects=None):
        """This is a wrapper method to the SQLAlchemy bulk_save_objects
        bulk_save_objects(objects, return_defaults=False, update_changed_only=True, preserve_order=True)
        """

        if objects:
            if len(objects):
                table_object = objects[0]
                mappings = [mapper.get_values_as_dict() for mapper in objects]

                self.bulk_insert_mappings(table_object, mappings)

    def bulk_insert_mappings(self, table_object, mappings=None):
        """This is a wrapper method to the SQLAlchemy bulk_save_objects
        bulk_insert_mappings(mapper, mappings, return_defaults=False, render_nulls=False)
        """

        # TODO - Fix this so we can save data using list of dicts
        raise NotImplementedError()  # This looks like it was once used, but no longer as the _load_csv params were incorrect
        # if table_object and mappings:
        #     if len(mappings):
        #
        #         # Get the list of columns that need to be populated
        #         columns = self._get_table_columns(table_object)
        #         path = 'out.csv'
        #         with open(path,'w') as f:
        #             w = csv.DictWriter(
        #                 f,
        #                 fieldnames=columns,
        #                 encoding='utf-8',
        #                 quoting=csv.QUOTE_MINIMAL,
        #                 extrasaction='ignore',
        #                 delimiter='\t'
        #             )
        #             w.writeheader()
        #
        #             for values in mappings:
        #                 w.writerow(values)
        #
        #         self._load_csv(table_object, path)

    def bulk_insert_dataframe(self, table_object: 'Table', df: pd.DataFrame, append: bool = False, chunk_size: int = 500000, load_greenplum_parquet: bool = False):
        """Pandas-flavored wrapper method to the load data into PlaidCloud Table from a Dataframe
        bulk_insert_dataframe(table_object, df, append, chunk_size)
        """
        try:
            import pyarrow as pa
        except ImportError as exc:
            raise ImportError('Use of this method requires full install. Try running `pip install plaid-rpc[full]`') from exc
        if len(df) == 0:
            logger.debug('Empty dataframe - nothing to insert')
            return

        # get table metadata for existing table object from analyze
        table_meta_in = self.rpc.analyze.table.table_meta(project_id=self._project_id, table_id=table_object.id)
        cols_analyze = []
        col_order = []
        if table_meta_in and len(table_meta_in) > 0:
            for rec in table_meta_in:
                cols_analyze.append(rec['id'])
                rec['source'] = rec['id']
                rec['target'] = rec['id']

            col_order = cols_analyze

        # get column order of dataframe
        cols_dataframe = df.columns

        cols_append = [c for c in cols_analyze if c in cols_dataframe]  # in target and df
        cols_leftover = [c for c in cols_dataframe if c not in cols_analyze]  # in df, but not target
        cols_missing = [c for c in cols_analyze if c not in cols_dataframe]  # in target, but not df
        cols_overwrite = cols_append + cols_leftover  # use if append=false... best effort to maintain col order

        table_meta_out = None
        if append:
            # order dataframe according to the existing structure

            # create any missing columns
            for col in cols_missing:
                df[col] = None

            if table_meta_in and len(table_meta_in) > 0:
                # ensure outbound matches incoming
                # drop any columns in table_meta_in that aren't in df
                table_meta_out = table_meta_in
            else:
                # table doesn't exist and/or doesn't have metadata... need to use df metadata
                append = False
                col_order = cols_overwrite

        else:
            # match order according to the existing structure, adding new cols to the end of the table
            col_order = cols_overwrite

        if not table_meta_out:
            # either there was no inbound metadata (table didn't exist) or append is false, so we're overwriting anyway
            dtype_list = [str(dtype) for dtype in list(df[col_order].dtypes)]
            table_meta_out = [
                {
                    'id': col,
                    'source': col,
                    'target': col,
                    'dtype': analyze_type(dtype_list[idx])
                } for idx, col in enumerate(col_order)
            ]

        if not col_order:
            col_order = cols_overwrite

        df = df.reindex(columns=col_order)

        data_load = self.rpc.analyze.table.create_data_load(
            project_id=self._project_id,
            table_id=table_object.id,
            load_type='parquet',
            load_greenplum_parquet=load_greenplum_parquet,
        )
        if data_load:
            try:
                schema = pa.Schema.from_pandas(df)
            except pa.lib.ArrowTypeError as e:
                bad_columns = []
                for col in df.columns:
                    try:
                        pa.Schema.from_pandas(df[[col]].copy())
                    except pa.lib.ArrowTypeError:
                        bad_columns.append(col)
                raise Exception(f'Mixed data in column(s) {bad_columns}') from e

            for col in schema:
                if isinstance(col.type, (pa.ListType, pa.StructType)):
                    df[col.name] = df[col.name].map(str)
            with tempfile.NamedTemporaryFile(mode='wb+') as pq_file:
                df.to_parquet(pq_file)
                # upload the file
                pq_file.seek(0)
                self._upload(table_object.id, data_load['load_type'], data_load['upload_path'], pq_file)

            # execute the load
            self.rpc.analyze.table.execute_data_load(
                project_id=self._project_id,
                table_id=table_object.id,
                meta=table_meta_out,
                load_type=data_load['load_type'],
                upload_path=data_load['upload_path'],
                append=append,
            )
        else:
            # Do it the old way
            for row in range(0, df.shape[0], chunk_size):
                with tempfile.NamedTemporaryFile(mode='wb+') as csv_file:
                    df[row:row + chunk_size].to_csv(
                        csv_file,
                        index=False,
                        header=True,
                        na_rep='NaN',
                        sep='\t',
                        encoding='UTF-8',
                        quoting=csv.QUOTE_MINIMAL,
                        escapechar='"',
                        compression='zip',
                        date_format='%Y-%m-%dT%H:%M:%S', # This needs to match format passed in _load_csv below
                    )
                    csv_file.seek(0)
                    self._load_csv(
                        project_id=self._project_id,
                        table_id=table_object.id,
                        meta=table_meta_out,
                        csv_data=base64.b64encode(csv_file.read()),
                        header=True,
                        delimiter='\t',
                        null_as='NaN',
                        quote='"',
                        escape='"',
                        date_format='YYYY-MM-DD"T"HH24:MI:SS',
                        append=append or row > 0,
                        compressed=True,
                    )

    def _upload(self, table_id: str, load_type: str, upload_path: str, pfile):

        upload_url = urlunparse(urlparse(self.rpc.rpc_uri)._replace(path='upload_data'))

        params = {
            'table_id': table_id,
            'load_type': load_type,
            'upload_path': upload_path,
        }
        headers = {
            "Authorization": f'Bearer {self.rpc.auth_token() if callable(self.rpc.auth_token) else self.rpc.auth_token}'
        }

        # logger.info('Preparing to open and upload {}'.format(archive_name))

        with requests.sessions.Session() as session:
            retry = Retry(
                total=5,
                allowed_methods=None,  # retry for any method
                status_forcelist=[500, 502, 504],
                backoff_factor=0.1,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount('http://', adapter)
            session.mount('https://', adapter)

            r = session.post(
                upload_url,
                headers=headers,
                verify=self.rpc.verify_ssl,
                files={
                    'upload_file': pfile
                },
                params=params,
                timeout=300,
            )
            r.raise_for_status()
            return r.json()

    def commit(self):
        """Here for completeness.  Does nothing"""
        pass

    def rollback(self):
        """Auto commit is the mode for this.  No rollback possible"""
        raise Exception('No Rollback is possible using a PlaidTools connection.')

    def close(self):
        """Here for completeness.  Does nothing"""
        pass

    def add(self, mapping):
        """Inserts a single record"""
        objects = [mapping]
        self.bulk_save_objects(objects=objects)

    def truncate(self, table):
        return self.rpc.analyze.table.clear_data(
                project_id=self._project_id, table_id=table.id
            )

    def drop(self, table):
        return self.rpc.analyze.table.delete(
                project_id=self._project_id, table_id=table.id
            )

    def save_data(self, query, table, append: bool = False):
        """Saves the data from the give query as a table in the database

        Args:
            query (): Query from which to create the table data
            table (str or Table): Table name/path, or Table object
            append (bool): If true, append the data to the table

        Returns:
            None
        """
        def _table_meta():
            return [
                {
                    'id': col.name,
                    'dtype': analyze_type(col.type.compile(self.dialect))
                }
                for col in query.selected_columns
            ]

        # ensure the table exists as per the metadata
        table = Table(
            self,
            table if isinstance(table, str) else table.id,
            columns=_table_meta(),
            overwrite=not append
        )

        # use the upsert method to add the data
        insert_query, insert_params = self._compiled(table.insert().from_select(query.selected_columns, query))
        self.rpc.analyze.query.upsert(
            project_id=self._project_id,
            table_id=table.id,
            update_query=None,
            update_params=None,
            insert_query=insert_query,
            insert_params=insert_params,
            recreate=True
        )

    @property
    def project_id(self):
        if not self._project_id:
            raise Exception('Project Id has not been set')
        return self._project_id

    @property
    def project_name(self):
        if not self._project_id:
            raise Exception('Project Id has not been set')
        return self._project_name

    def get_dimension(self, dimension_name):
        return self.dims.get_dimension(name=dimension_name, replace=False).hierarchy_table()

    def get_table(self, table_name):
        return Table(self, table_name)

    def _load_udf_params(self):
        self.udf = None
        if not isinstance(self.rpc.step_id, str):
            return
        config = self.rpc.analyze.step.step(
            project_id=self._project_id,
            step_id=self.rpc.step_id
        )
        _sources = [
            (self.get_table(apply_variables(s['source'], self.variables)), s['id'])
            for s in config.get('sources', [])
        ]
        _targets = [
            (self.get_table(apply_variables(t['target'], self.variables)), t['id'])
            for t in config.get('targets', [])
        ]
        _variables = [
            (apply_variables(v['value'], self.variables), v['name'])
            for v in config.get('variables', [])
        ]
        self.udf= UDFParams(
            source_by_name={s[1]: s[0] for s in _sources},
            sources=[s[0] for s in _sources],
            target_by_name={t[1]: t[0] for t in _targets},
            targets=[t[0] for t in _targets],
            variable_by_name={v[1]: v[0] for v in _variables},
            variables=[v[0] for v in _variables],
        )

    def refresh_variables(self) -> dict:
        if not isinstance(self.rpc.workflow_id, str):
            return self.rpc.analyze.project.variable_values(
                project_id=self._project_id,
            )
        return self.rpc.analyze.workflow.variable_values(
            project_id=self._project_id,
            workflow_id=self.rpc.workflow_id,
            include_project=True,
        )


class Table(sqlalchemy.Table):
    # TODO - This needs to create an object that can act as both a core SQLAlchemy object
    # or as a declarative object
    _conn = None

    def __new__(cls, conn, table, metadata=None, create_on_missing=True, columns=None, overwrite=False):
        """

        Args:
            conn (Connection):
            table (str):
            metadata (sqlalchemy.MetaData, optional):
            create_on_missing (bool, optional):
            columns (list, optional):
            overwrite (bool, optional):

        Returns:

        """
        _rpc = conn.rpc
        _project_id = conn.project_id

        if metadata:
            _metadata = metadata
        else:
            _metadata = sqlalchemy.MetaData()

        _table_id, _, _ = _get_table_id(_rpc, _project_id, table, raise_if_not_found=False)

        if create_on_missing and _table_id is None:
            # Since this is a new table.  Set overwrite to true to create physical table
            overwrite = True

            if not columns:
                columns = []

            if table.startswith(TABLE_PREFIX):
                # This is already the ID.  Use it
                name = 'Table {}'.format(table)
                path = '/'
            elif '/' in table:
                # This is a path.  Peform a path lookup.
                name = table.split('/')[-1]
                path = '/'.join(table.split('/')[:-1])
            else:
                # This must be a name only.  Perform a name lookup
                name = table
                path = '/'

            _table_id = _rpc.analyze.table.create(
                project_id=conn.project_id,
                path=path,
                name=name,
                memo=None,
                columns=columns
            )['id']

        if columns:
            # Only try to create a physical table if columns have been defined
            _rpc.analyze.table.touch(project_id=_project_id, table_id=_table_id, meta=columns, overwrite=overwrite)

        columns = _rpc.analyze.table.table_meta(
            project_id=_project_id, table_id=_table_id,
        )
        if not columns:
            columns = []  # If the table doesn't actually exist, we assume it's got no columns

        if _project_id.startswith(SCHEMA_PREFIX):
            _schema = _project_id
        else:
            _schema = '{}{}'.format(SCHEMA_PREFIX, _project_id)

        table_object = super(Table, cls).__new__(
            cls,
            _table_id,
            _metadata,
            *[
                sqlalchemy.Column(
                    c['id'], sqlalchemy_from_dtype(c['dtype']),
                )
                for c in columns
            ],
            schema=_schema,
            extend_existing=False  # If this is the second object representing
                                   # this table, update.
                                   # If you made it with this function, it should
                                   # be no different.
        )

        table_object._metadata = _metadata
        table_object._conn = conn
        table_object._rpc = _rpc
        table_object._project_id = _project_id
        table_object._table_id = _table_id
        table_object._schema = _schema

        # table must be created in database, if it doesn't already exist
        if columns:
            _rpc.analyze.table.touch(
                project_id=_project_id, table_id=_table_id, meta=columns, overwrite=overwrite
            )

        return table_object

    @property
    def project_id(self):
        return self._project_id  # pylint: disable=no-member

    @property
    def id(self):
        return self._table_id  # pylint: disable=no-member

    @property
    def fully_qualified_name(self):
        return '"{}"."{}"'.format(self.schema, self.id)

    def info(self, keys=None):  # pylint: disable=method-hidden
        return self.table_info(keys)

    def table_info(self, keys=None):
        return self._rpc.analyze.table(  # pylint: disable=no-member
            project_id=self.project_id, table_id=self.id,
            keys=keys,
        )

    def cols(self):
        """
        Ideally this would be named 'columns' but there is a
        name collision with SQLAlchemy's table.columns
        """
        return self._rpc.analyze.table.table_meta(  # pylint: disable=no-member
            project_id=self.project_id,
            table_id=self.id,
        )

    def head(self, conn, rows=10):
        if rows is not None:
            query = self.select().limit(rows)
        else:
            query = self.select()
        return conn.get_dataframe_from_select(query)

    def get_data(self, clean=False):
        return self._conn.get_dataframe(self, clean=clean)

    @overload
    def save(self, datasource: selectable.SelectBase, append: bool = False):
        ...

    @overload
    def save(self, datasource: pd.DataFrame, append: bool = False):
        ...

    def save(self, datasource: Any, append: bool = False):
        if isinstance(datasource, pd.DataFrame):
            return self._conn.bulk_insert_dataframe(self, datasource, append=append)
        return self._conn.save_data(datasource, self)


def _get_table_id(rpc, project_id, name, raise_if_not_found=True):
    if name.startswith(TABLE_PREFIX):
        # This is already the ID.  Use it
        #logger.warning('Table ID passed to _get_table_id. Not searching for paths or name.')  # P.B. Turning off warning 19/05/25
        return name, None, None
    # elif '/' in name:
    #     # This is a path.  Perform a path lookup.
    #     _table_id = rpc.analyze.table.lookup_by_full_path(project_id=_project_id, path=table)
    # else:
    #     # This must be a name only.  Perform a name lookup
    #     _table_id = rpc.analyze.table.lookup_by_name(project_id=_project_id, name=table)
    else:
        path, table_name = os.path.split(name)
        if not path.startswith('/'):
            path = '/{}'.format(path)
        # Attempt to determine the table ID from the name
        tables_by_name = rpc.analyze.table.search_by_name(
            project_id=project_id,
            text=table_name,
            criteria='exact',
            keys=['id', 'paths']
        )
        if len(tables_by_name) == 1:
            # There's only one table with that name, so it must be the one we want.
            return tables_by_name[0]['id'], path, table_name
        elif len(tables_by_name) > 1:
            # There's more than one table, so try to disambiguate by path.
            table_ids = [t['id'] for t in tables_by_name if path in t['paths']]
            if len(table_ids) == 1:
                # If we have only one left, return it.
                return table_ids[0], path, table_name
            elif raise_if_not_found:
                # We do not have exactly 1 table that matches both name and path.
                raise Exception('Ambiguous table reference `{}`. '
                                '{} tables found that match that name and path.'
                                ''.format(name, len(tables_by_name)))
            elif len(table_ids) > 1:
                # We have multiple that match both name and path, and we aren't
                # supposed to raise, so arbitrarily return the first.
                return table_ids[0], path, table_name
            else:  #elif len(table_ids) < 1:
                # We have multiple that match name, but 0 that match path, and
                # we aren't supposed to raise, so arbitrarily return the first
                # that matches name.
                return tables_by_name[0]['id'], path, table_name
        else:
            # There weren't any with that name.
            if raise_if_not_found:
                raise Exception('Unable to find specified table. No tables '
                                'matched the name {}'.format(name))
            else:
                return None, path, table_name
