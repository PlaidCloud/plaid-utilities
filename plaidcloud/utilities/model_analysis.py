"""
Model Analysis
Generate this Excel worksheet to illustrate model dependencies.
"""
from __future__ import absolute_import
import orjson as json
import logging
import sys
import pandas as pd
import numpy as np

from plaidtools import config
import xlwings as xw  # pylint: disable=import-error
from shutil import copy

logger = logging.getLogger(__name__)
conf = config.get_dict()


def parse_docstrings_and_inputs(model_steps):
    """
    Read each python job in the model and capture docstring, inputs, & outputs
    Desired output: list of dicts
    {
        "title": "My Job Title. Line 1 of docstring",
        "description": "My job description. Line 2/3 of docstring",
        "inputs": ["PATH_1", "PATH_2"],
        "outputs": ["PATH_3", "PATH_4"]
    }
    """

    master_docstring_list = []
    order = 1

    for job in model_steps:
        logger.debug("Reading %s", job)

        # open file, read lines, close file
        f = open(job + ".py", 'r')
        lines = f.readlines()
        f.close()

        # grab docstring at top, triggered by triple quotes as start/stop
        i = 0
        triple_quote_count = 0
        docstring_lines = []
        while i <= 10:
            logger.debug("Line: %s", lines[i])
            if lines[i] == '"""\n':
                triple_quote_count += 1
            else:
                if 0 < triple_quote_count < 2:
                    docstring_lines.append(lines[i].replace('\n',''))
            i += 1

        # first line is title, remaining lines are description
        title = docstring_lines[0]
        description = ' '.join(docstring_lines[1:])

        # grab inputs
        in_lines = [x for x in lines if x.lstrip()[:3] == "IN_"]
        in_path_list = []
        for line in in_lines:
            # find brackets, keep contents inside
            try:
                open_bracket = line.index('[')
                close_bracket = line.index(']')
                in_path_list.append(line[open_bracket+2:close_bracket-1])
            except:
                pass # instance not found

        # grab outputs
        out_lines = [x for x in lines if x.lstrip()[:4] == "OUT_"]
        out_path_list = []
        for line in out_lines:
            # find brackets, keep contents inside
            try:
                open_bracket = line.index('[')
                close_bracket = line.index(']')
                out_path_list.append(line[open_bracket+2:close_bracket-1])
            except:
                pass # instance not found

        # write results to dict, then add to master list
        docstring_dict = {
            "order": order,
            "title": title,
            "description": description,
            "inputs": in_path_list,
            "outputs": out_path_list
        }
        master_docstring_list.append(docstring_dict)
        order += 1

    return master_docstring_list


def listRightIndex(alist, value):
    """
    Get index position of right-most value
    """
    return len(alist) - alist[-1::-1].index(value) -1


def get_titles(model_io):
    """
    Returns a list of all Title entries
    """
    all_titles = []
    for item in model_io:
        all_titles.append(item['title'])
    return all_titles


def get_descriptions(model_io):
    """
    Returns a list of all Description entries
    """
    all_desc = []
    for item in model_io:
        all_desc.append(item['description'])
    return all_desc


def get_title_and_description(model_io):
    """
    Returns a list of dicts with title/description
    """
    all_title_desc = []
    for item in model_io:
        title = item['title']
        desc = item['description']
        order = item['order']
        all_title_desc.append({"Job Title":title,
                               "Job Description":desc,
                               "Manifest Order":order})
    return all_title_desc


def get_io_in_order(model_io):
    """
    Returns a list of all IO files, in the order in which they appear
    """
    io_list = []
    for model_step in model_io:
        """
        check inputs first, then add outputs
        """
        for input_val in model_step['inputs']:
            if input_val not in io_list:
                io_list.append(input_val)
        for output_val in model_step['outputs']:
            if output_val not in io_list:
                io_list.append(output_val)
    return io_list


def main(MODEL_PERIOD=None, manifest_name='all_steps'):
    paths = conf['paths']
    opts = conf['options']

    if MODEL_PERIOD is None:
        MODEL_PERIOD = opts['MODEL_PERIOD']

    PATHS_MODEL     = opts['PATHS_MODEL']
    PATHS_SYNC      = opts['PATHS_SYNC']
    PATHS_PUBLISH   = opts['PATHS_PUBLISH']

    OUT_MODEL_ANALYSIS = paths['MODEL_ANALYSIS'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)
    OUT_MODEL_ANALYSIS_XLS = paths['MODEL_ANALYSIS_XLS'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)

    COPY_FRC_DIR      = paths['RESULTS_FRC_PARENT_DIR'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)
    COPY_EQUITIES_DIR = paths['RESULTS_EQUITIES_PARENT_DIR'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)
    COPY_RESEARCH_DIR = paths['RESULTS_RESEARCH_PARENT_DIR'].format(period=MODEL_PERIOD, PATHS_MODEL=PATHS_MODEL)

    # model_steps = conf['manifest']['debug']
    # model_steps = conf['manifest']['create_drivers']
    # model_steps = conf['manifest']['manifest']
    # model_steps = conf['manifest']['model']
    # model_steps = conf['manifest']['relationship_score']
    # model_steps = conf['manifest']['all_steps']
    # model_steps = conf['manifest']['mike'] # you want the line above..just in case mike accidentally commits this code
    try:
        # Try specified manifest first.
        model_steps = conf['manifest'][manifest_name]
    except KeyError:
        logger.error("Could not find manifest '%s'. Exiting.",
                     manifest_name)
        sys.exit(-255)

    model_io = parse_docstrings_and_inputs(model_steps)

    json_output = json.dumps(model_io, option=json.OPT_INDENT_2)
    with open(OUT_MODEL_ANALYSIS, 'w') as fo:
        fo.write(json_output)
        bytes_written = fo.tell() - 1

    # get data to build df
    all_titles = get_titles(model_io)
    all_descriptions = get_descriptions(model_io)
    all_title_desc = get_title_and_description(model_io)
    all_io = get_io_in_order(model_io)

    # create data frame
    df = pd.DataFrame(all_title_desc, index=all_titles, columns=['Manifest Order', 'Job Title', 'Job Description'])

    # append blank columns in order
    for col in all_io:
        df[col] = None

    for job in model_io:
        """
        Cycle through each row, accessed by index & set IO
        values accordingly. "inputs" & "outputs" below correspond
        to column names.
        df.set_value(ROW_INDEX, COLUMN_NAME, VALUE)
        """
        for inputs in job['inputs']:
            df.set_value(job['title'], inputs, 'I')
        for outputs in job['outputs']:
            df.set_value(job['title'], outputs, 'O')

    for index, row in df.iterrows():
        """
        Find first & last IO for each row, then set null values
        in between to '-'
        """
        # get first and last index positions for I/O values
        has_io = [i for i, value in enumerate(row) if value in ['I', 'O']]
        try:
            first_io = has_io[0]
            last_io = has_io[-1]
        except IndexError:
            logger.debug("No I/O for this row")
        else:
            fill = np.array([False] * len(row))
            fill[first_io + 1:last_io] = True
            for col_num in has_io:
                fill[col_num] = False
            df.loc[index, fill] = '-'

    # remove false positives caused by paths in directories
    false_positives = [
        'RESULTS_DIR',
        'RAW_EXCHANGE_EMAIL_DIR',
        'EXCHANGE_EMAIL_DIR',
        'BBG_CHAT_DIR',
        'BBG_EMAIL_DIR',
        'RESULTS_TEMPLATE',
        'RESULTS_FRC_PARENT_DIR',
        'RESULTS_EQUITIES_PARENT_DIR',
        'RESULTS_RESEARCH_PARENT_DIR'
    ]
    for f in false_positives:
        try:
            del df[f]
        except:
            logger.debug('Path %s not found', f)

    # write manifest name to XLS file as column header
    manifest_name = 'Job Description -- (' + manifest_name + ') manifest'
    df.rename(columns={
        'Job Description' : manifest_name
    }, inplace=True)

    logger.debug("Break stuff")

    # fillna with blank spaces for XLS formatting
    df = df.fillna(' ')

    app = xw.App(visible=False)
    try:
        wb = xw.Book(OUT_MODEL_ANALYSIS_XLS)
        try:
            output_sheet = 'MODEL STEPS & IO'

            sht = wb.sheets[output_sheet]
            sht.activate()
            sht.clear()

            # for large data frames, attempting to write everything at once can cause crashes
            chunk_size = 1000

            i = 0  # counter
            while (i * chunk_size) <= len(df):
                # write data frame to excel in bits and pieces
                if i == 0:
                    sht.range('A1', index=False).value = df[:chunk_size]
                else:
                    cell = 'A' + str((i * chunk_size)+2)
                    # header = false for all sequent chunks
                    sht.range(cell, header=False, index=False).value = df[(i * chunk_size):(i * chunk_size) + chunk_size]
                i += 1

            sht.autofit()

            # apply XLS macros
            app.macro("format_model_analysis")  # or perhaps wb.macro("format_model_analysis")
        finally:
            # save and close
            wb.save()
            wb.close()
    finally:
        app.quit()

    # copy model analysis to each business area for syncing to shared drives
    copy_dir_paths = [
        {'dir': COPY_EQUITIES_DIR, 'biz': 'Equities'},
        {'dir': COPY_FRC_DIR, 'biz': 'FRC'},
        {'dir': COPY_RESEARCH_DIR, 'biz': 'Global Research'}
    ]
    for copy_dir in copy_dir_paths:
        # copy file
        copy(OUT_MODEL_ANALYSIS_XLS, copy_dir['dir'])

    logger.debug("Model analysis is complete")
