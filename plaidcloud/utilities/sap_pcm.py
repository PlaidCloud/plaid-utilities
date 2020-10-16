#!/usr/bin/env python
# coding=utf-8

"""PCM-specific parameters and methods"""

from __future__ import absolute_import
import os
import math
import platform
import locale

import pandas as pd
import numpy as np
import texttable
from IPython.core.display import display, HTML

__author__     = 'Michael Rea'
__copyright__  = ' Copyright 2018, Tartan Solutions, Inc'
__credits__    = ['Michael Rea']
__license__    = 'Proprietary'
__maintainer__ = 'Michael Rea'
__email__      = 'michael.rea@tartansolutions.com'

if platform.system() == "Windows":
    locale.setlocale(locale.LC_ALL, 'english_us') # <--this setting will be different in linux
else:
    locale.setlocale(locale.LC_ALL, 'en_US.UTF-8')

DIMENSIONS = [
    'Versions',
    'Periods',
    'Responsibility Centers',
    'Currencies',
    'Line Items',
    'Line Item Details',
    'Activities',
    'Resource Drivers',
    'Cost Objects 1',
    'Cost Objects 2',
    'Cost Objects 3',
    'Cost Objects 4',
    'Cost Objects 5',
    'Revenue Types',
    'Activity Drivers',
    'Services',
    'Spreads',
    'Work Sheets 1',
    'Work Sheets 2',
    'Employees',
    'Capacity Rules',
    'User Defined Rules',
]

ADV_DIMENSIONS = [
    'Versions',
    'Periods',
    'Responsibility Centers',
    #'Currencies',
    #'Line Items',
    #'Line Item Details',
    #'Activities',
    #'Resource Drivers',
    'Cost Objects 1',
    'Cost Objects 2',
    'Cost Objects 3',
    'Cost Objects 4',
    'Cost Objects 5',
    #'Revenue Types',
    'Activity Drivers',
    #'Services',
    #'Spreads',
    #'Work Sheets 1',
    #'Work Sheets 2',
    #'Employees',
    #'Capacity Rules',
    #'User Defined Rules',
]

LIV_DIMENSIONS = [
    'Versions',
    'Periods',
    'Responsibility Centers',
    'Currencies',
    'Line Items',
    #'Line Item Details',
    #'Activities',
    #'Resource Drivers',
    #'Cost Objects 1',
    #'Cost Objects 2',
    #'Cost Objects 3',
    #'Cost Objects 4',
    #'Cost Objects 5',
    #'Revenue Types',
    #'Activity Drivers',
    #'Services',
    #'Spreads',
    #'Work Sheets 1',
    #'Work Sheets 2',
    #'Employees',
    #'Capacity Rules',
    #'User Defined Rules',
]

COV_DIMENSIONS = [
    'Versions',
    'Periods',
    'Responsibility Centers',
    'Currencies',
    'Line Items',
    #'Line Item Details',
    'Activities',
    #'Resource Drivers',
    'Cost Objects 1',
    'Cost Objects 2',
    'Cost Objects 3',
    'Cost Objects 4',
    'Cost Objects 5',
    #'Revenue Types',
    #'Activity Drivers',
    #'Services',
    #'Spreads',
    #'Work Sheets 1',
    #'Work Sheets 2',
    #'Employees',
    #'Capacity Rules',
    #'User Defined Rules',
]

DCOV_DIMENSIONS = [
    'Versions',
    'Periods',
    'Responsibility Centers',
    'Currencies',
    'Line Items',
    #'Line Item Details',
    #'Activities',
    #'Resource Drivers',
    'Cost Objects 1',
    'Cost Objects 2',
    'Cost Objects 3',
    'Cost Objects 4',
    'Cost Objects 5',
    #'Revenue Types',
    #'Activity Drivers',
    #'Services',
    #'Spreads',
    #'Work Sheets 1',
    #'Work Sheets 2',
    #'Employees',
    #'Capacity Rules',
    #'User Defined Rules',
]
