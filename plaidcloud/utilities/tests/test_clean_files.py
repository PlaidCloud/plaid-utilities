# coding=utf-8
"""Tests for plaidcloud.utilities.clean_files."""

import csv
import datetime
import os
import tempfile
import time
import unittest

import pytest

from plaidcloud.utilities import clean_files


class TestShouldClean(unittest.TestCase):
    """These tests validate the should_clean method."""

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
        cls._old_clean_file = cls.create_temp_file()
        os.utime(cls._old_clean_file, (old_time, old_time))
        cls._dirty_file = cls.create_temp_file()
        os.utime(cls._dirty_file, (dirty_time, dirty_time))
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


class TestCleanEmailAddressStr(unittest.TestCase):

    def test_already_clean_passthrough(self):
        self.assertEqual(
            clean_files.clean_email_address_str('test_name@email.com'),
            'test_name@email.com',
        )

    def test_strips_whitespace_and_control_chars(self):
        dirty = '\rtest??  _name\t@email.com\n'
        self.assertEqual(
            clean_files.clean_email_address_str(dirty),
            'test_name@email.com',
        )

    def test_removes_commas(self):
        self.assertEqual(
            clean_files.clean_email_address_str('a,b@example.com'),
            'ab@example.com',
        )


class TestFindCleanPath(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()
        self.input_dir = os.path.join(self.tmp, 'input')
        self.clean_dir = os.path.join(self.tmp, 'clean')
        os.mkdir(self.input_dir)

        self.config = {
            'paths': {
                'input_to_clean_dirs': {
                    self.input_dir: self.clean_dir,
                },
            },
            'options': {'PATHS_MODEL': 'MODEL'},
        }

    def tearDown(self):
        for root, dirs, files in os.walk(self.tmp, topdown=False):
            for f in files:
                os.unlink(os.path.join(root, f))
            for d in dirs:
                os.rmdir(os.path.join(root, d))
        os.rmdir(self.tmp)

    def test_creates_clean_dir_and_returns_normalized_path(self):
        original = os.path.join(self.input_dir, 'file.csv')
        with open(original, 'w') as f:
            f.write('x')

        result = clean_files.find_clean_path('Q1', original, self.config)

        self.assertTrue(os.path.isdir(self.clean_dir))
        self.assertEqual(
            result,
            os.path.normpath(os.path.join(self.clean_dir, 'file.csv')),
        )

    def test_missing_input_mapping_raises(self):
        original = os.path.join(self.tmp, 'unrelated', 'file.csv')
        with self.assertRaises(KeyError):
            clean_files.find_clean_path('Q1', original, self.config)

    def test_missing_input_to_clean_dirs_key_raises(self):
        bad_config = {
            'paths': {},
            'options': {'PATHS_MODEL': 'MODEL'},
        }
        with self.assertRaises(KeyError):
            clean_files.find_clean_path('Q1', '/anything', bad_config)


if __name__ == '__main__':
    unittest.main()
