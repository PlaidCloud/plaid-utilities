#!/usr/bin/env python
# coding=utf-8

import os
import chardet
import unicodedata
import sys
import codecs

starting_chunksize = 1 * 1024 * 1024  # 1 MB
max_chunksize = 8 * 1024 * 1024  # 8 MB

__author__ = 'Paul Morel'
__copyright__ = 'Copyright 2010-2020, Tartan Solutions, Inc'
__credits__ = ['Paul Morel']
__license__ = 'Apache 2.0'
__maintainer__ = 'Paul Morel'
__email__ = 'paul.morel@tartansolutions.com'

def convert(target_encoding, in_path, out_path, include_bom=False):
    """Converts character encoding of a file to `target_encoding`

    This is designed to determine the source encoding automatically

    Args:
        target_encoding (str): The encoding to convert the file to
        in_path (str): The path to the file to convert
        out_path (str): Where to write out the converted file
        include_bom (bool, optional): If the BOM marker should be added to
            the top of the file. Defaults to `False`"""

    guess = 'utf8'

    dir_path = os.path.dirname(out_path)

    if not os.path.exists(dir_path):
        os.makedirs(dir_path)

    with open(in_path, 'rb') as f_in:
        with open(out_path, 'w') as f_out:
            if include_bom is True:
                # Add the bom marker at the top of the file
                # This only applies to UTF16
                if target_encoding == 'utf-16le':
                    f_out.write(codecs.BOM_UTF16_LE)
                elif target_encoding == 'utf-16be':
                    f_out.write(codecs.BOM_UTF16_BE)

            chunksize = starting_chunksize

            while True:
                last_file_position = f_in.tell()
                chunk = f_in.read(chunksize)
                if not chunk:  # We've hit the end of the file
                    break
                else:
                    try:
                        # Convert using current guess
                        f_out.write(
                            unicodedata.normalize(
                                'NFKD', chunk.decode(guess)
                            ).encode(target_encoding, 'ignore').decode(target_encoding).replace('\n"\n', '"\n')
                        )

                    except UnicodeDecodeError as e:
                        if e.reason in ('truncated data', 'unexpected end of data') and max_chunksize > chunksize:
                            f_in.seek(last_file_position)  # Go back to the beginning of the chunk
                            chunksize += starting_chunksize  # Try a bigger chunk
                            continue  # Try bigger chunk, to overshoot the truncation

                        else:
                            # Assume the encoding is the problem
                            encoding = chardet.detect(chunk)['encoding']

                            if encoding == guess:
                                raise e  # That encoding has already been tried, so there's a real problem

                            else:
                                f_in.seek(last_file_position)  # Go back to the beginning of the chunk
                                guess = encoding

                                continue  # Try chunk again with new guess
                    else:
                        chunksize = starting_chunksize  # reset chunksize, after a successful conversion
                        continue  # On to next chunk


if __name__ == '__main__':
    convert('ascii', sys.argv[1], '{}.ascii'.format(sys.argv[1]))
