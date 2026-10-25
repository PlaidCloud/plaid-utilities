import os

import pytest

from plaidcloud.utilities import convert_encoding


def test_convert_warns_and_output_is_unchanged(tmp_path):
    in_path = tmp_path / 'in.txt'
    in_path.write_bytes('café ²\n'.encode('utf-8'))
    out_path = tmp_path / 'out.txt'

    with pytest.warns(FutureWarning, match=r'removed no earlier than 2027-01-24') as record:
        convert_encoding.convert('ascii', str(in_path), str(out_path))

    assert 'plaidcloud.utilities.convert_encoding.convert is deprecated' in str(record[0].message)
    assert os.path.realpath(record[0].filename) == os.path.realpath(__file__)
    assert out_path.read_bytes() == b'cafe 2\n'


def test_convert_utf16_target_still_writes_locale_bytes(tmp_path):
    in_path = tmp_path / 'in.txt'
    in_path.write_bytes(b'abc\n')
    out_path = tmp_path / 'out.txt'

    with pytest.warns(FutureWarning):
        convert_encoding.convert('utf-16le', str(in_path), str(out_path))

    assert out_path.read_bytes() == b'abc\n'
