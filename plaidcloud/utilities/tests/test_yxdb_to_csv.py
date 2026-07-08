#!/usr/bin/env python
# coding=utf-8
"""Data-fidelity tests for the .yxdb -> CSV staging conversion.

Covers the three Alteryx-correctness defects:
  * narrow strings (String/V_String) are CP1252 in practice, not latin-1
    (mojibake for 0x80-0x9F: euro sign, curly quotes, dashes)
  * FixedDecimal must survive as its exact decimal string, never through a float
  * Blob/SpatialObj bytes must never be written as the Python bytes repr
"""

import csv
import struct

import pytest

from plaidcloud.utilities import frame_manager
from plaidcloud.utilities.frame_manager import _convert_yxdb_value, _yxdb_fixed_decimal_extractor, yxdb_to_csv

__author__ = 'Paul Morel <paul@plaidcloud.com>'
__copyright__ = '© Copyright 2026, PlaidCloud, Inc'
__license__ = 'Apache 2.0'


# --- CP1252 narrow strings (F22) ---------------------------------------------

@pytest.mark.parametrize('alteryx_type', ['String', 'V_String'])
def test_narrow_string_c1_bytes_transcode_to_cp1252(alteryx_type):
    # The yxdb library decodes narrow-string bytes 0x80/0x93/0x96 as latin-1
    # C1 control characters; they are really CP1252 euro / left curly quote /
    # en dash.
    assert _convert_yxdb_value('\x80\x93\x96', alteryx_type) == '€“–'


def test_narrow_string_undefined_cp1252_byte_is_replaced_not_fatal():
    # 0x81 has no CP1252 mapping; policy is errors='replace', never an exception.
    assert _convert_yxdb_value('\x81', 'V_String') == '�'


def test_wide_string_is_untouched():
    value = 'café € “quoted”'
    assert _convert_yxdb_value(value, 'V_WString') == value


def test_none_passes_through():
    assert _convert_yxdb_value(None, 'V_String') is None


# --- FixedDecimal precision (F20) ---------------------------------------------

def test_fixed_decimal_extractor_returns_exact_string():
    # Raw field bytes: null-terminated ASCII decimal padded to the declared
    # size, followed by the 1-byte null flag (0 = value present).
    raw = b'1234567890.123456789'  # 19 significant digits: a float can't hold it
    length = 22
    buffer = memoryview(raw + b'\x00' * (length - len(raw)) + b'\x00')
    assert _yxdb_fixed_decimal_extractor(0, length)(buffer) == '1234567890.123456789'


def test_fixed_decimal_extractor_null_flag():
    buffer = memoryview(b'\x00' * 22 + b'\x01')
    assert _yxdb_fixed_decimal_extractor(0, 22)(buffer) is None


def test_fixed_decimal_extractor_negative_value_exact():
    # Leading '-' sign must survive verbatim, not be lost or reformatted.
    raw = b'-9876543210.987654321'
    length = 22
    buffer = memoryview(raw + b'\x00' * (length - len(raw)) + b'\x00')
    assert _yxdb_fixed_decimal_extractor(0, length)(buffer) == '-9876543210.987654321'


# --- Blob / SpatialObj bytes (F21) ---------------------------------------------

def test_blob_bytes_become_hex_never_bytes_repr():
    assert _convert_yxdb_value(b'\x01\xab\xff', 'Blob') == '01abff'


def _point_blob(lng, lat):
    # Alteryx spatial blob: obj type 8 = points, point count at 36:40,
    # lng/lat doubles at 40:56.
    return struct.pack('<i', 8) + b'\x00' * 32 + struct.pack('<i', 1) + struct.pack('<2d', lng, lat)


def test_spatialobj_decodes_to_wkt():
    pytest.importorskip('yxdb.spatial')
    assert _convert_yxdb_value(_point_blob(-105.5, 44.2), 'SpatialObj') == 'POINT (-105.5 44.2)'


def test_spatialobj_malformed_blob_falls_back_to_hex():
    pytest.importorskip('yxdb.spatial')
    assert _convert_yxdb_value(b'\x01\x02', 'SpatialObj') == '0102'


def test_spatialobj_without_yxdb_spatial_falls_back_to_hex(monkeypatch):
    monkeypatch.setattr(frame_manager, '_yxdb_spatial', None)
    blob = _point_blob(-105.5, 44.2)
    assert _convert_yxdb_value(blob, 'SpatialObj') == blob.hex()


# --- End-to-end through a synthetic .yxdb file ---------------------------------

def _build_yxdb(path):
    """Write a minimal valid .yxdb: FixedDecimal + V_String + Blob, one record.

    The record block is stored as a single uncompressed lzf block (high bit
    set on the block length word).
    """
    meta_xml = (
        '<RecordInfo>'
        '<Field name="Amount" size="22" scale="9" type="FixedDecimal" />'
        '<Field name="Notes" size="0" type="V_String" />'
        '<Field name="Payload" size="0" type="Blob" />'
        '</RecordInfo>'
    )
    meta_bytes = meta_xml.encode('utf_16_le')

    header = bytearray(512)
    header[0:21] = b'Alteryx Database File'
    header[80:84] = (len(meta_bytes) // 2 + 1).to_bytes(4, 'little')  # meta info size
    header[104:108] = (1).to_bytes(4, 'little')  # num_records

    amount = b'1234567890.123456789'
    fixed_decimal = amount + b'\x00' * (22 - len(amount)) + b'\x00'  # padded + null flag
    # Tiny inline blobs: 3 data bytes in the low bytes of the 4-byte fixed
    # portion, length (3) in the top nibble of the 4th byte (0x30).
    v_string = b'\x80\x93\x96\x30'  # CP1252 euro, left curly quote, en dash
    blob = b'\x01\xab\xff\x30'
    record = fixed_decimal + v_string + blob + (0).to_bytes(4, 'little')  # empty var portion

    with open(path, 'wb') as f:
        f.write(bytes(header))
        f.write(meta_bytes)
        f.write(b'\x00\x00')
        f.write((len(record) | 0x80000000).to_bytes(4, 'little'))
        f.write(record)


def test_yxdb_to_csv_end_to_end(tmp_path):
    pytest.importorskip('yxdb')
    yxdb_path = str(tmp_path / 'data.yxdb')
    csv_path = str(tmp_path / 'data.csv')
    _build_yxdb(yxdb_path)

    yxdb_to_csv(yxdb_path, csv_path)

    with open(csv_path, newline='') as f:
        rows = list(csv.reader(f, delimiter='\t'))
    assert rows[0] == ['Amount', 'Notes', 'Payload']
    amount, notes, payload = rows[1]
    assert amount == '1234567890.123456789'  # exact, never through a float
    assert notes == '€“–'
    assert payload == '01abff'


def test_amp_format_file_raises_actionable_error(tmp_path):
    pytest.importorskip('yxdb')
    yxdb_path = str(tmp_path / 'amp.yxdb')
    with open(yxdb_path, 'wb') as f:
        f.write(b'Alteryx e2 Database file' + b'\x00' * 488)

    with pytest.raises(ValueError, match='AMP'):
        yxdb_to_csv(yxdb_path, str(tmp_path / 'amp.csv'))


def test_yxdb_fixed_decimal_override_is_restored(tmp_path):
    yxdb_extractors = pytest.importorskip('yxdb._extractors')
    original = yxdb_extractors.new_fixed_decimal_extractor
    yxdb_path = str(tmp_path / 'data.yxdb')
    _build_yxdb(yxdb_path)
    yxdb_to_csv(yxdb_path, str(tmp_path / 'data.csv'))
    assert yxdb_extractors.new_fixed_decimal_extractor is original
