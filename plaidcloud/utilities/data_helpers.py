#!/usr/bin/env python
# coding=utf-8

"""Data manipulation and cost allocation helpers."""

import os
import re
import math
import platform
import locale
import functools

import numpy as np
import pandas as pd
from pandas.api.types import is_numeric_dtype
import texttable

# Note: one function imports from IPython

__author__ = 'Michael Rea'
__copyright__ = ' Copyright 2014-2019, Tartan Solutions, Inc'
__credits__ = ['Michael Rea', 'Andrew Hodgson']
__license__ = 'Apache 2.0'
__maintainer__ = 'Michael Rea'
__email__ = 'michael.rea@tartansolutions.com'

# default formatting for report_to_xl
report_formatting = {
    'report_title': {
        'size': 16,
        'color': 2,
        'background': (0, 32, 96),
        'bold': True
    },
    'report_subtitle': {
        'size': 12,
        'color': 56,
        'background': (149, 179, 215),
        'bold': True,
        'italic': False
    },
    'table_title': {
        'size': 11,
        'background': (199, 199, 199),
        'bold': True
    },
    'table_subtitle': {
        'size': 11,
        'background': (242, 242, 242),
        'bold': True
    },
    'table_header': {
        'size': 8,
        'bold': True
    },
    'table_data':{
        'size': 8,
        'word_wrap': False
    },
    'table_summary': {
        'bold': True
    }
}

if platform.system() == "Windows":
    locale.setlocale(locale.LC_ALL, 'english_us')  # <--this setting will be different in linux
else:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')


def suppress_non_unicode(dirty_string):
    """Get rid of non-unicode characters.

    WARNING:  This loops through each character in strings.
              Use sparingly.

    http://stackoverflow.com/questions/20078816/replace-non-ascii-characters-with-a-single-space
    """
    return '' .join([i if ord(i) < 128 else ' ' for i in dirty_string])


def clean_frame(df):
    """
    for each column in the frame with dtype==object,
    return the list of decoded cell in the Series instead
    This presumes any type of object is intended to be a string.  Be careful.
    This feels inefficient as hell, but it freaking works.
    """

    def clean_ascii(cell):
        try:
            return ''.join([c.decode('unicode_escape').encode('ascii', 'ignore') for c in cell]).replace('\n"', '"')
        except Exception:
            return ' '

    for col in df:
        if str(df[col].dtype) == 'object':
            df[col] = list(map(clean_ascii, df[col]))

    return df


def jupyter_table(input):
    """
    Produce a pretty text table that will show up as such in Jupyter.

    Use this by first importing importing into your notebook
    from IPython.core.display import display, HTML

    Then display table in jupyter like this

    my_table = dh.get_text_table(df_nsv) #where dh = data_helpers
    display(jupyter_table(my_table))

    This is way better for readability than the way jupyter renders pandas tables.
    Also, it will work on non-pandas stuff where browser-based
    line wrapping is not desired.
    """

    # Importing here because IPython is a big import that we don't need for anything else in data_helpers
    from IPython.core.display import HTML

    # TODO: Just make this an option of inspect
    jupyter_wrapper = ''.join(['<div style="font-weight: normal;font-family: monospace;white-space:pre;">', input, '</div>'])

    jt = HTML(jupyter_wrapper)
    return jt


def cast_as_str(input_val):
    """Force a value to be of type 'str'.

    Args:
        input_val (mixed): Input of mixed type

    Returns:
        str: String of the value, defaults to ' ' if the value cannot be cast as an string
    Examples:
        >>> cast_as_str('3')
        '3'
        >>> cast_as_str(3.55)
        '3.55'
        >>> cast_as_str('Three')
        'Three'
        >>> cast_as_str(None)
        ' '
        >>> cast_as_str(np.nan)
        ' '
        >>> cast_as_str({})
        '{}'
        >>> cast_as_str(pd.NA)
        ' '
    """
    if input_val is None:
        return ' '

    try:
        if pd.isna(input_val) or np.isnan(input_val):
            return ' '
        else:
            try:
                return str(input_val)
            except:
                return ''.join([i if ord(i) < 128 else ' ' for i in input_val])

    except:
        try:
            return str(input_val)
        except:
            return ''.join([i if ord(i) < 128 else ' ' for i in input_val])


def coalesce(*options):
    """
    Args:
        *options: A list of objects some of which might be None, ending in a default value.

    Returns:
        The leftmost object in the list that is not None, '', or NaN. If no
        such objects exist, returns the rightmost object in the list, or None
        if called with no arguments.
    Examples:
        >>> coalesce()
        >>> coalesce(None)
        >>> coalesce(None, 'a')
        'a'
        >>> coalesce(None, None, 'a')
        'a'
        >>> coalesce(None, 'a', 'b')
        'a'
        >>> coalesce('', 'a')
        'a'
        >>> coalesce(float('nan'), 'a')
        'a'
        >>> coalesce(None, None, 'c')
        'c'
    """
    # pseudocode for list comp version:
    # [o for o in options if o is not None and o != '' and o is not NaN][0]
    def coalesce2(option_a, option_b):
        try:
            if math.isnan(option_a):
                return option_b
        except:
            pass
        if option_a is None:
            return option_b
        elif option_a == '':
            return option_b
        else:
            return option_a

    return functools.reduce(coalesce2, options, None)


def cast_as_int(input_val):
    """
    Args:
        input_val: A value of unknown type

    Returns:
        Int of the value, defaults to 0 if the value cannot be cast as an int.
    Examples:
        >>> cast_as_int('3')
        3
        >>> cast_as_int(3.55)
        3
        >>> cast_as_int('Three')
        0
        >>> cast_as_int(None)
        0
        >>> cast_as_int(np.nan)
        0
        >>> cast_as_int(pd.NA)
        0
    """
    if input_val is None:
        return 0

    try:
        if pd.isna(input_val) or np.isnan(input_val):
            return 0
        else:
            try:
                return int(input_val)
            except ValueError:
                return 0
    except:
        try:
            return int(input_val)
        except ValueError:
            return 0


def cast_as_float(input_val):
    """Force a value to be of type 'float'.

    Args:
        input_val (mixed): Mixed input type

    Examples:
        >>> cast_as_float('3')
        3.0
        >>> cast_as_float(3.55)
        3.55
        >>> cast_as_float('Three')
        0.0
        >>> cast_as_float(None)
        0.0
        >>> cast_as_float(np.nan)
        0.0
        >>> cast_as_float(pd.NA)
        0.0
    """

    if input_val is None:
        return 0.0
    try:
        if pd.isna(input_val) or np.isnan(input_val):
            return 0.0
        else:
            try:
                return float(input_val)
            except ValueError:
                return 0.0
    except:
        try:
            return float(input_val)
        except ValueError:
            return 0.0


def num(num):
    """Make numbers pretty with comma separators."""
    if math.isnan(num):
        num = 0
    return locale.format_string("%d", num, grouping=True)


def cols(input_frame, title='table', print_out=True, show_dtype=True, double_quotes=False):
    """
    This is just a quick wrapper of get_columns with flipped defaults.
    """

    return get_columns(input_frame, title, print_out)


def get_columns(input_frame, title='table', print_out=False):
    """
    Accepts a pandas data frame and returns a reader-friendly listing of columns and datatype.

    Very useful in debug mode for quick copy/paste when writing UDFs

    Args:
        input_frame (Pandas.Dataframe): Dataframe
        title: (str): Title of table.
        print_out: (boolean): print directly, so the user doesn't need to wrap the call in a print().  Useful for debugging

    Returns:
        string: print of a table
    """
    col_info = ''
    for column in input_frame.columns:
        #col_info += ("   '" + i + "', #" + str(input_frame[i].dtype)+"\n")
        col_info += "    '{}', #{}\n".format(column, input_frame[column].dtype)

    if print_out:
        print(col_info)
    else:
        return col_info


def list_columns(input_frame, title='table', print_out=True):
    """
    Accepts a pandas data frame and returns a reader-friendly listing of columns and datatype.

    Very useful in debug mode for quick copy/paste when writing UDFs

    Args:
        input_frame (Pandas.Dataframe): Dataframe
        title: (str): Title of table.
        print_out: (boolean): print directly, so the user doesn't need to wrap the call in a print().  Useful for debugging

    Returns:
        string: print of table columns for easy copy-paste into superset json config.

    Was going to do this as a call to get_columns with a bunch of optional params set a certain way, but was faster to just
    make a seprate method.
    """
    col_info = ''
    dq = '"'
    for column in input_frame.columns:
        #col_info += '"{}", '.format(column)
        col_info += "{dq}{column}{dq}, \n".format(column=column, dq=dq)

    if print_out:
        print(col_info)
    else:
        return col_info


def inspect(input_frame, nrows=10, title='table', types=True, print_out=True):
    """This is just a quick wrapper of get_text_table with flipped defaults."""
    return get_text_table(input_frame, nrows, title, types, print_out)


def get_text_table(df, nrows=10, title='table', types=True, print_out=False):
    """Creates a printed text table of a dataframe (or eventually other table-structured input).

    Accepts a pandas data frame and returns a print-friendly table.


    Args:
        df (Pandas.Dataframe): Dataframe
        nrows: (int): Number of rows printed
        title: (str): Title of table.
        types: (boolean): Indicate data type in column headers
        print_out: (boolean): print directly, so the user doesn't need to wrap the call in a print().  Useful for debugging

    Returns:
        string: print of a table, or None, if print_out=True.  In the later case, it just prints directly.

    Todo:   * Allow optional arguments to change table cosmetics such
              as column width and borders.

            * Could be expanded to check type of inbound recordset and
              handle many different types of recordsets including:
              pandas data frame
              NumPy recarray
              hdf5
              list of lists / list of tuples

    .. http://foutaise.org/code/texttable/

    """
    frame_length = len(df)
    frame_width = len(df.columns)

    input_frame = df.copy(deep=True)

    if nrows is not None:
        input_frame = input_frame[:nrows] # do this early on, so we don't cast_as_foo() on stuff that we don't need.

    col_types = ['i']  # inbound tables has an index of type int.
    for i in input_frame.columns:
        cur_type = input_frame[i].dtype
        if cur_type == object:
            col_types.append('t')
        elif 'int' in str(cur_type):
            # facepalm. Hey, it works, and with all flavors of int.
            col_types.append('i')
        elif cur_type == float:
            col_types.append('f')
        else:
            col_types.append('a')

    if types is True:
        for i in input_frame.columns:
            cur_type = input_frame[i].dtype
            if cur_type == object:
                input_frame.rename(columns={input_frame[i].name : input_frame[i].name + '::str'}, inplace=True)
            elif 'int' in str(cur_type):
                input_frame.rename(columns={input_frame[i].name : input_frame[i].name + '::int'}, inplace=True)
            elif cur_type == float:
                input_frame.rename(columns={input_frame[i].name : input_frame[i].name + '::float'}, inplace=True)
            else:
                input_frame.rename(columns={input_frame[i].name : input_frame[i].name + '::{}'.format(cur_type)}, inplace=True)

    # TODO from Pat: Maybe this loop and the one above could be combined into a method - def gather_column_types(rename)

    input_recarray = input_frame.iloc[0:nrows].to_records()

    headers = input_recarray.dtype.names
    records = input_recarray.tolist()
    input_list = []
    input_list.append(headers)
    input_list.extend(records)

    tt = texttable.Texttable(max_width=0)
    tt.set_deco(texttable.Texttable.HEADER)
    tt.set_cols_dtype(col_types)

    tt.add_rows(input_list)

    output = '------------------\n' +\
        title + ': ' + num(frame_length) + ' records, ' + num(frame_width) +\
        ' columns \n------------------\n' + tt.draw() + '\n'

    if print_out is True:
        print(output)
    else:
        return output


def mask(df, key, value):
    """
    Useful for daisy-chaining dataframe filters
    """
    return df[df[key] == value]


def not_equal(df, key, value):
    """
    Useful for daisy-chaining dataframe filters
    """
    return df[df[key] != value]


def in_list(df, key, value):
    """
    Useful for daisy-chaining dataframe filters
    """
    return df[df[key] in value]


def not_in_list(df, key, value):
    """
    Useful for daisy-chaining dataframe filters
    """
    return df[df[key] not in value] # maybe this should be df[~df[key] in value]


def is_equal(df, key, value):
    """
    Useful for daisy-chaining dataframe filters
    """
    return df[df[key] == value]


def clean_names(frame):
    """
    Delete input columns "foo__in" from frame.
    Clean output columns "foo__out" -> "foo"
    #TODO: Delete or clean WIP columns...not sure what to do yet ("foo__bar")
    """

    col_list = list(frame.columns)

    for col in col_list:
        if col.endswith('__in'):
            del frame[col]
        elif col.endswith('__out'):
            frame.rename(columns={col: col[:-5]}, inplace=True)
        elif col.endswith('__value'):
            del frame[col]
        elif col.endswith('__split'):
            frame.rename(columns={col: col[:-7]}, inplace=True)

    return frame


def remove_nan_values_from_dict(d):
    """
    removes any nan values from a dict
    input: d (type:dict)
    return: no_nan_dict (type:dict)
    """
    no_nan_dict = {}
    for key, val in d.items():
        if pd.notnull(val):
            no_nan_dict[key] = val

    return no_nan_dict


def _get_wb(book_name):
    import xlwings as xw  # pylint:disable=import-error

    if book_name is None:
        book_name = '\\'.join(['~', 'src', 'debug', 'template.xlsm'])

    book_name = expand_user_path(book_name)

    if not os.path.exists(book_name):
        status = "Book {0} does not exist".format(book_name)
        print(status)

        if book_name.lower().endswith('xlsm'):
            base, extension = os.path.splitext(book_name)
            # xlwings can't create new book and save as xlsm
            extension = '.xlsx'
            book_name = base + extension

        # try to create book
        try:
            wb = xw.Book()
            wb.save(book_name)
        except:
            status = "Invalid workbook: {0}".format(book_name)
            print(status)
            return None

    try:
        return xw.Book(book_name)  # open/activate workbook

    except:
        status = "Invalid workbook: {0}".format(book_name)
        print(status)
        return None


def _get_sheet(wb, sheet):
    try:
        sht = wb.sheets[sheet]  # attempt to access existing sheet
    except:
        sht = wb.sheets.add(sheet)  # creates new worksheet and activates it

    sht.activate()
    return sht


def _set_wb_visibility(wb, visibility):
    if visibility is False:
        wb.app.visible = False
        wb.app.screen_updating = False
    else:
        wb.app.visible = True
        wb.app.screen_updating = True


def to_xl(df_source,
          nrows=None,
          wb=None,
          book=None,
          sheet="RAW DATA",
          autofit=True,
          show_index=False,
          silent=False):
    """
    writes a given data frame to Excel.  expecting one of wb/book, but not both
    input: df (type:Pandas dataframe)
           nrows (type:int)
           wb (type:xlwings book object)
           book (type:File path)
           sheet (type:Excel worksheet name)
           autofit (type:bool)
           show_index (type:bool)
           silent (type:bool)
    return: String status of import
    """

    # MUST do this, otherwise, the source df itself is modified, and buggered up with preceding ' on strings.
    df = df_source.copy(deep=True)

    if nrows is not None:
        df = df[:nrows]

    # prevent error when data frame is too large to fit into Excel
    if (nrows is None) or (nrows > 1048575):
        if (df.shape[0] > 1048575) or (df.shape[1] > 16384):
            status = "Data frame is too large to write to XLS! Try writing a subset of the data instead."
            print(status)
            return status

    save_on_exit = False  # only trigger save at end of this function if wb object is created internally
    if wb:
        # workbook passed in... [over]write book variable w/ actual path for exception string only
        book = wb.fullname
    else:
        # no workbook object passed in... create one & toggle save_on_exit flag
        save_on_exit = True
        wb = _get_wb(book)

    if silent:
        visibility = False
    else:
        visibility = True

    _set_wb_visibility(wb, visibility)
    try:
        sht = _get_sheet(wb, sheet)
        sht.clear()  # clear contents of worksheet

        def force_text(item):
            return f"'{item}"

        for col in df.columns:
            cur_type = df[col].dtype
            if cur_type == object:
                # This will force strings w/ numeric-only chars to go to Excel as strings. 0004 = '0004', not 4.
                df[col] = list(map(force_text, df[col]))

        # write contents of df
        sht.range('A1').options(index=False).value = df

        freeze_top = wb.macro("xlwings.FreezeTop")
        turn_on_filters = wb.macro("xlwings.TurnOnFilters")

        try:
            freeze_top()
            turn_on_filters()
        except:
            status = "Macro(s) didn't run: {0}".format(book)
            print(status)

        if autofit:
            sht.autofit()
    finally:
        if not silent:
            _set_wb_visibility(wb, True)

        if save_on_exit:
            wb.save()
            wb.close()

    return "Data frame has been written to Excel"


def report_to_xl(data_frames,
                 book=None,
                 sheet="SUMMARY",
                 report_title=None,
                 report_subtitle=None,
                 report_formatting=report_formatting,
                 report_padding=2,
                 df_start_col='A',
                 autofit=True,
                 column_max_width=50,
                 show_index=False,
                 silent=False):
    """
    writes any number of data frames to the same sheet in Excel
    input: data_frames (type:list of dicts)
           book (type:File path)
           sheet (type:Excel worksheet name)
           report_title (type:str)
           report_subtitle (type:str)
           report_formatting (type:dict)
           report_padding (type:int)
           df_start_col (type:str)
           autofit (type:bool)
           column_max_width (type:int)
           show_index (type:bool)
           silent (type:bool)
    return: String status of import
    """
    import xlwings as xw  # pylint:disable=import-error

    def _apply_formatting(sht, cell_range, format_dict, format_level):
        if format_dict.get(format_level).get('bold'):
            sht.range(cell_range).api.Font.Bold = format_dict.get(format_level).get('bold')
        if format_dict.get(format_level).get('italic'):
            sht.range(cell_range).api.Font.Italic = format_dict.get(format_level).get('italic')
        if format_dict.get(format_level).get('underline'):
            sht.range(cell_range).api.Font.Underline = format_dict.get(format_level).get('underline')

        if format_dict.get(format_level).get('size'):
            sht.range(cell_range).api.Font.Size = format_dict.get(format_level).get('size')
        if format_dict.get(format_level).get('color'):
            sht.range(cell_range).api.Font.ColorIndex = format_dict.get(format_level).get('color')
        if format_dict.get(format_level).get('background'):
            sht.range(cell_range).color = format_dict.get(format_level).get('background')

        if format_dict.get(format_level).get('word_wrap'):
            sht.range(cell_range).api.WrapText = format_dict.get(format_level).get('word_wrap')

    def _apply_borders(sht, cell_range, border_location, border_weight):
        border_location = border_location.lower()
        if border_location == 'left':
            border_position = 1
        elif border_location == 'right':
            border_position = 2
        elif border_location == 'top':
            border_position = 3
        elif border_location == 'bottom':
            border_position = 4
        else:
            return

        sht.range(cell_range).api.Borders(border_position).Weight = border_weight
    # establish XLS connection - workbook & sheet
    wb = _get_wb(book)

    _set_wb_visibility(wb, silent)
    try:
        sht = _get_sheet(wb, sheet)
        sht.clear()  # clear contents of worksheet

        # keep track of XLS row number
        xls_index = 1

        if report_title:
            sht.range('A{0}'.format(xls_index)).value = report_title
            _apply_formatting(
                sht,
                xw.Range(xw.Range('A{0}'.format(xls_index)),
                         xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right').end('right')),
                report_formatting,
                'report_title')
            xls_index += 1

        if report_subtitle:
            sht.range('A{0}'.format(xls_index)).value = report_subtitle
            _apply_formatting(
                sht,
                xw.Range(xw.Range('A{0}'.format(xls_index)),
                         xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right').end('right')),
                report_formatting,
                'report_subtitle')
            xls_index += 1

        if report_title or report_subtitle:
            xls_index += 1

        max_width_df = 0

        # loop through each data frame and write contents to a single sheet
        for data_frame_info in data_frames:
            df = data_frame_info.get('df').copy(deep=True)

            len_df = len(df) + 1 # account for row of column names
            width_df = len(df.columns)
            if width_df > max_width_df:
                max_width_df = width_df

            if data_frame_info.get('title'):
                sht.range('{0}{1}'.format(df_start_col, xls_index)).value = data_frame_info.get('title')
                _apply_formatting(
                    sht,
                    xw.Range(xw.Range('A{0}'.format(xls_index)),
                             xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right').end('right')),
                    report_formatting,
                    'table_title')
                xls_index += 1

            if data_frame_info.get('subtitle'):
                subtitle = data_frame_info.get('subtitle')
                if isinstance(subtitle, str):
                    subs = [subtitle]
                elif isinstance(subtitle, list):
                    subs = subtitle
                else:
                    subs = []

                for sub in subs:
                    sht.range('{0}{1}'.format(df_start_col, xls_index)).value = sub

                    _apply_formatting(
                        sht,
                        xw.Range(xw.Range('A{0}'.format(xls_index)),
                                 xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right').end('right')),
                        report_formatting,
                        'table_subtitle')

                    xls_index += 1

            if data_frame_info.get('sum_columns'):
                # append a total row at the bottom - check to make sure sum cols are valid first
                missing_cols = [col for col in data_frame_info.get('sum_columns') if col not in df.columns]
                if len(missing_cols) > 0:
                    print("Column(s) not found in {0}: {1}".format(data_frame_info.get('title'), missing_cols))
                # verify that all columns are numeric
                numeric_sum_cols = [col for col in data_frame_info.get('sum_columns') \
                                    if col in df.columns and is_numeric_dtype(df[col])]
                sht.range('{0}{1}'.format(df_start_col, xls_index)).options(index=False).value = \
                    df.append(df[numeric_sum_cols].sum().rename('Total'))
            else:
                sht.range('{0}{1}'.format(df_start_col, xls_index)).options(index=False).value = df

            # apply formatting to table column headers
            if report_formatting.get('table_header'):
                _apply_formatting(
                    sht,
                    xw.Range(xw.Range('A{0}'.format(xls_index)),
                         xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right')),
                    report_formatting,
                    'table_header')


            range_of_df_without_header = sht.range(
                xw.Range('{0}{1}'.format(df_start_col, xls_index+1)),
                xw.Range('{0}{1}'.format(df_start_col, xls_index)).offset(len_df-1, width_df-1)
            )

            range_of_df_with_header = sht.range(
                xw.Range('{0}{1}'.format(df_start_col, xls_index)),
                xw.Range('{0}{1}'.format(df_start_col, xls_index)).offset(len_df-1, width_df-1)
            )

            # underline column header row
            _apply_borders(
                sht,
                xw.Range(xw.Range('A{0}'.format(xls_index)),
                         xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right')),
                'bottom',
                2)

            # apply named ranges
            if data_frame_info.get('title'):
                try:
                    sht.range(range_of_df_with_header).name = re.sub(r'[^\w\_\-\s]', '', data_frame_info.get('title'))
                except:
                    print('Named range invalid.  Skipping: {0}'.format(re.sub(r'[^\w\_\-\s]', '', data_frame_info.get('title'))))

            # apply formatting to table data
            if report_formatting.get('table_data'):
                _apply_formatting(
                    sht,
                    range_of_df_without_header,
                    report_formatting,
                    'table_data')

            xls_index += len_df

            # underline bottom row
            _apply_borders(
                sht,
                xw.Range(xw.Range('A{0}'.format(xls_index-1)), range_of_df_without_header.last_cell),
                'bottom',
                2)

            if data_frame_info.get('sum_columns'):
                if report_formatting.get('table_summary'):
                    _apply_formatting(sht,
                                      xw.Range(xw.Range('{0}{1}'.format(df_start_col, xls_index)),
                                               xw.Range('{0}{1}'.format(df_start_col, xls_index)).end('right').end('right')),
                                      report_formatting,
                                      'table_summary')
                xls_index += 1

            xls_index += report_padding

        if autofit:
            sht.autofit()
            if (report_title or report_subtitle) and df_start_col != 'A':
                xw.Range('A1').column_width = 2
            i = 1
            while i <= max_width_df:
                if xw.Range((1,i)).column_width > column_max_width:
                    xw.Range((1,i)).column_width = column_max_width
                i += 1
    finally:
        if not silent:
            _set_wb_visibility(wb, True)


def expand_user_path(fname):
    expanded_path = os.path.expanduser(fname)

    # Freaking believe it.  This is necessary.  Excel wants paths to look a very specific way.
    # No mix of \\ and /.  And must be C:\user, not C:user.
    # os.path.join by itself does NOT get it done :(...
    return os.path.normpath(expanded_path)


def safe_divide(numerator, denominator, error_return_value=0):
    """Safe Divide function, sends specified return value on error

    Args:
        numerator:
        denominator:
        error_return_value:

    Returns:

    Examples:
        >>> safe_divide(1, 4.0, False)
        0.25
        >>> safe_divide(1, 4, False)
        0
        >>> safe_divide(1, 1.0, False)
        1.0
        >>> safe_divide(4, 1.0, False)
        4.0
        >>> safe_divide(1, 0, False)
        False
        >>> safe_divide(1, None, False)
        False
        >>> safe_divide(1, np.nan, False)
        False
        >>> safe_divide(None, 1, False)
        False
        >>> safe_divide(1, pd.NA, False)
        False
        >>> safe_divide(pd.NA, 1, False)
        False
    """
    try:
        if isinstance(numerator, int) and isinstance(denominator, int):
            result = numerator//denominator
        else:
            result = numerator/denominator
    except:
        result = error_return_value

    if pd.isna(result) or abs(result) == np.inf or pd.isnull(result):
        return error_return_value

    return result


def remove_all(string, substrs):
    """Removes a whole list of substrings from a string, returning the cleaned
    string.

    Args:
        string (str): A string to be filtered
        substrs (:obj:`list` of :obj:`strs`): The substrings to remove from the string

    Returns:
        str: string with all substrings removed from it.

    Examples:
        >>> remove_all('Four score and seven years ago', [])
        'Four score and seven years ago'
        >>> remove_all('Four !score! an!d s!ev!en yea!rs a!g!o!', ['!'])
        'Four score and seven years ago'
        >>> remove_all('Fo?ur !sc?ore! an!d s!ev!en ye?a!rs a!g!o!', ['!', '?'])
        'Four score and seven years ago'
        >>> remove_all('Four score and seven years ago', ['score and seven '])
        'Four years ago'
        >>> remove_all('Four score and seven years ago', ['score ', 'and ', 'seven '])
        'Four years ago'
    """

    def remove1(string, substr):
        return string.replace(substr, '')

    return functools.reduce(remove1, substrs, string)
