# coding=utf-8

import unittest
import pytest
import tempfile
import os
import time
import datetime

from plaidcloud.utilities import clean_files

__author__ = "Pat Buxton"
__copyright__ = "© Copyright 2009-2018, Tartan Solutions, Inc"
__credits__ = ["Pat Buxton"]
__license__ = "Apache 2.0"
__maintainer__ = "Pat Buxton"
__email__ = "patrick.buxton@tartansolutions.com"


class TestShouldClean(unittest.TestCase):
    """These tests validate the should_clean method"""
    _temp_folder = ''
    _old_clean_file = ''
    _dirty_file = ''
    _new_clean_file = ''

    @classmethod
    def create_temp_file(cls):
        if cls._temp_folder == '':
            cls._temp_folder = tempfile.mkdtemp()
        file_handle, file_name = tempfile.mkstemp(dir=cls._temp_folder)
        os.fsync(file_handle)
        os.close(file_handle)
        return file_name

    @classmethod
    def setUpClass(cls):
        old_date = datetime.datetime(year=2019, month=9, day=1, hour=6, minute=0, second=0)
        old_time = time.mktime(old_date.timetuple())
        dirty_date = datetime.datetime(year=2019, month=9, day=1, hour=6, minute=0, second=1)
        dirty_time = time.mktime(dirty_date.timetuple())
        new_date = datetime.datetime(year=2019, month=9, day=1, hour=6, minute=0, second=2)
        new_time = time.mktime(new_date.timetuple())
        # create clean file (old)
        cls._old_clean_file = cls.create_temp_file()
        os.utime(cls._old_clean_file, (old_time, old_time))
        # create dirty file (new)
        cls._dirty_file = cls.create_temp_file()
        os.utime(cls._dirty_file, (dirty_time, dirty_time))
        # create clean file (newest)
        cls._new_clean_file = cls.create_temp_file()
        os.utime(cls._new_clean_file, (new_time, new_time))

    def test_should_clean_no_clean_file(self):
        assert clean_files.should_clean('dirty', 'clean') is True

    def test_should_clean_no_dirty_file(self):
        with pytest.raises(EnvironmentError):
            clean_files.should_clean('dirty', self._old_clean_file)

    def test_should_clean_old_clean_file(self):
        assert clean_files.should_clean(self._dirty_file, self._old_clean_file) is True

    def test_should_clean_new_clean_file(self):
        assert clean_files.should_clean(self._dirty_file, self._new_clean_file) is False

    @classmethod
    def tearDownClass(cls):
        if cls._temp_folder != '':
            os.unlink(cls._old_clean_file)
            os.unlink(cls._dirty_file)
            os.unlink(cls._new_clean_file)
            # os.unlink(cls._temp_folder)
