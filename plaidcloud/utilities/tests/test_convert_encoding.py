# coding=utf-8
"""Tests for plaidcloud.utilities.convert_encoding."""

import os
import tempfile
import unittest

from plaidcloud.utilities import convert_encoding


class TestConvert(unittest.TestCase):

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for root, _, files in os.walk(self.tmp, topdown=False):
            for f in files:
                os.unlink(os.path.join(root, f))
            os.rmdir(root)

    def _write_bytes(self, name, data):
        path = os.path.join(self.tmp, name)
        with open(path, 'wb') as f:
            f.write(data)
        return path

    def test_utf8_to_ascii_drops_non_ascii(self):
        in_path = self._write_bytes(
            'in.txt',
            'hello café world\n'.encode('utf-8'),
        )
        out_path = os.path.join(self.tmp, 'out.txt')
        convert_encoding.convert('ascii', in_path, out_path)

        with open(out_path, 'r', encoding='ascii') as f:
            result = f.read()

        # 'café' -> 'caf' after dropping non-ascii characters.
        self.assertIn('hello', result)
        self.assertNotIn('é', result)

    def test_creates_missing_output_directory(self):
        in_path = self._write_bytes('in.txt', b'simple ascii\n')
        out_path = os.path.join(self.tmp, 'nested', 'dir', 'out.txt')
        convert_encoding.convert('ascii', in_path, out_path)

        self.assertTrue(os.path.isfile(out_path))

    def test_bom_not_written_for_non_utf16(self):
        in_path = self._write_bytes('in.txt', b'abc\n')
        out_path = os.path.join(self.tmp, 'out.txt')
        convert_encoding.convert('ascii', in_path, out_path, include_bom=True)

        with open(out_path, 'rb') as f:
            head = f.read(4)

        # include_bom is only honored for utf-16 variants; ascii output should
        # start with the payload rather than any BOM bytes.
        self.assertEqual(head[:1], b'a')


class TestConvertFallbackEncoding(unittest.TestCase):
    """Trigger the chardet fallback path when the first guess fails."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for root, _, files in os.walk(self.tmp, topdown=False):
            for f in files:
                os.unlink(os.path.join(root, f))
            os.rmdir(root)

    def test_latin1_input_falls_back_via_chardet(self):
        # Windows-1252 / latin-1 bytes that don't decode as UTF-8. The
        # ``convert`` function starts by guessing utf8, traps the decode
        # failure, and asks chardet for a better guess.
        data = 'Résumé café\n'.encode('windows-1252')
        in_path = os.path.join(self.tmp, 'in.txt')
        with open(in_path, 'wb') as f:
            f.write(data)
        out_path = os.path.join(self.tmp, 'out.txt')

        convert_encoding.convert('ascii', in_path, out_path)

        with open(out_path, 'r') as f:
            result = f.read()

        # Non-ascii characters should be dropped, ascii ones preserved.
        self.assertIn('sum', result)
        self.assertNotIn('é', result)


class TestConvertChunkRetry(unittest.TestCase):
    """Force the chunk-resize retry branch when a multi-byte character is
    split across a chunk boundary."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for root, _, files in os.walk(self.tmp, topdown=False):
            for f in files:
                os.unlink(os.path.join(root, f))
            os.rmdir(root)

    def test_multi_byte_char_across_chunk_boundary_retries(self):
        # Build a file whose first 1MB chunk ends mid-UTF-8 sequence. 'é'
        # (U+00E9) encodes to two bytes in UTF-8; placing it so that the
        # first byte is at index 1MB-1 forces a truncation-style decode
        # error that the retry path has to recover from.
        chunk_size = 1 * 1024 * 1024
        filler = b'a' * (chunk_size - 1)
        multibyte = 'é'.encode('utf-8')  # two bytes
        tail = b'b' * 128
        data = filler + multibyte + tail

        in_path = os.path.join(self.tmp, 'in.txt')
        with open(in_path, 'wb') as f:
            f.write(data)
        out_path = os.path.join(self.tmp, 'out.txt')

        convert_encoding.convert('ascii', in_path, out_path)

        with open(out_path, 'r', encoding='ascii') as f:
            result = f.read()
        # Conversion applies NFKD normalization: 'é' decomposes to 'e' + combining
        # acute accent; the accent is dropped under the ascii 'ignore' encoding,
        # but the 'e' is kept. So expect length filler + 1 + tail.
        self.assertEqual(len(result), len(filler) + 1 + len(tail))
        # And the tail bytes (plain ascii 'b') must be intact at the end.
        self.assertTrue(result.endswith('b' * 128))


class TestConvertChardetMatchRaises(unittest.TestCase):
    """When chardet returns the same encoding that just failed, the
    implementation must re-raise the original UnicodeDecodeError."""

    def setUp(self):
        self.tmp = tempfile.mkdtemp()

    def tearDown(self):
        for root, _, files in os.walk(self.tmp, topdown=False):
            for f in files:
                os.unlink(os.path.join(root, f))
            os.rmdir(root)

    def test_chardet_same_guess_raises_original(self):
        from unittest import mock

        in_path = os.path.join(self.tmp, 'in.txt')
        out_path = os.path.join(self.tmp, 'out.txt')
        # Bytes that won't decode as utf-8 and aren't a truncation.
        with open(in_path, 'wb') as f:
            f.write(b'\xff\xfe\xfd' * 10)

        with mock.patch.object(
            convert_encoding.chardet, 'detect', return_value={'encoding': 'utf8'},
        ):
            with self.assertRaises(UnicodeDecodeError):
                convert_encoding.convert('ascii', in_path, out_path)


if __name__ == '__main__':
    unittest.main()
