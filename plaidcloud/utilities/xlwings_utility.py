import xlwings as xw  # pylint: disable=import-error

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'


def activate_sheet(sheet):
    """
    First try to switch to existing sheet. If it doesn't exist, create it.

    Args:
        sheet (str or int): Sheet name or index
    """
    wb = xw.books.active
    try:
        wb.sheets[sheet].activate()
    except:
        wb.sheets.add(sheet)


def write_to_excel(df, chunk_size=1000, start_cell='A1'):
    """
    Writes data to excel in chunks.
    First chunk includes column headers, others do not.

    Args:
        df (`pandas.DataFrame`): The dataframe to write out
        chunk_size (int, optional): The chunk size to use while writing
        start_cell (str, optional): The cell to begin writing from
    """
    sht = xw.sheets.active
    i = 0  # counter
    while (i * chunk_size) < len(df):
        if i == 0:
            sht.range(start_cell).value = df[:chunk_size]
        else:
            sht.range(start_cell).offset((i * chunk_size) + 1, 0).value = df[(i * chunk_size):(i * chunk_size) + chunk_size]
        i += 1


def xlwings_run_macros(wb, macro_list):
    """Runs a set of XLWings macros

    Args:
        wb (`xlwings.Book`): The xlwings workbook to run macros on
        macro_list (list): A list of macros to run
    """
    for m in macro_list:
        wb.macro(m)


def xls_macro_freeze_top(wb):
    """Runs the FreezeTop macro on the provided workbook

    Args:
        wb (`xlwings.Book`): The workbook to run FreezeTop on
    """
    wb.macro("FreezeTop")


def xls_macro_turn_on_filters(wb):
    """Runs the TurnOnFilters macro on the provided workbook

    Args:
        wb (`xlwings.Book`): The workbook to run TurnOnFilters on
    """
    wb.macro("TurnOnFilters")


def xls_macro_refresh_pivots(wb):
    """Runs the RefreshAllWorkbookPivots macro on the provided workbook

    Args:
        wb (`xlwings.Book`): The workbook to run RefreshAllWorkbookPivots on
    """
    wb.macro("RefreshAllWorkbookPivots")


def get_col_range(start_cell, nrows, header=True):
    """
    Returns column range when given a starting position
    and height of table.
    -----------
    -----------
    20160621 ANH:  This doesn't seem to be calculating the way
    I would expect it to.  It made sense when I wrote it, but it's
    already confusing after a few weeks of idle time.  This
    definitely needs cleaned up so that it's 10000 times more intuitive.
    -----------
    -----------
    Args:
        start_cell (str): The cell to start calculating from
        nrows (int): The number of rows in the table
        header (Bool, optional): If the spreadsheet has a header row. Defaults to `True`
    Returns:
        str: The range of columns (in A1:A5 format)
    """
    wb = xw.books.active
    sht = wb.sheets.active
    # Pat - I'd probably write it like below, but I'm not sure about the header implementation in the original anyway
    return sht.range(start_cell).resize(nrows-(1 if header else 0), 0).get_address(False, False)
    #
    # start_cell = sht.range(start_cell)
    #
    # if header:
    #     end_cell = start_cell.offset(nrows - 2, 0)
    # else:
    #     end_cell = start_cell.offset(nrows - 1, 0)
    #
    # return ':'.join([start_cell.get_address(False, False), end_cell.get_address(False, False)])


def build_formula(target_cell, formula, header=True, propagate=True, direction='down', number_format=None):
    """Writes formula to specified cell(s), assuming target cell is the first row and NOT
        the first column in a 2D table range
    Args:
        target_cell (str): The cell to write to
        formula (str): The formula to write (`=sum(A1:A3)`)
        header (bool, optional): Does this file have a header row? Default to `True`
        propagate (bool, optional): Apply formula to entire column in range? Defaults to `True`
        direction (str, optional): Propagation direction. Defaults to `'down'`
        number_format (str, optional): Excel number formatting to apply. Defaults to `None`
    """
    wb = xw.books.active
    sht = wb.sheets.active

    # get row num of target cell
    target_row = sht.range(target_cell).row

    # convert to A1-style range
    target_cell = sht.range(target_cell).get_address(False, False)

    # get table dimensions
    table = sht.range(target_cell).current_region  # range object

    # get last row
    last_row = table.last_cell.row

    # calculate rows in table from target to end, ignoring header
    table_rows = last_row - target_row

    # end cell remains in target row, vertically offset by number of rows
    end_cell = sht.range(target_cell).offset(table_rows, 0).get_address(False, False)

    # build column range for formula
    # col_range = get_col_range(target_cell, table_rows, header=header)
    col_range = ':'.join([target_cell, end_cell])

    # write formula to XLS
    sht.range(col_range).formula = formula

    if number_format:
        sht.range(col_range).number_format = number_format
