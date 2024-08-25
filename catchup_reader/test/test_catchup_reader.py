from collections import deque
import io
import sys
import pytest
import random
import subprocess

from catchup_reader.src.catchup_reader import CatchUpReader

def test_catchup_reader_init():
    reader = CatchUpReader(seekable=True, keepends=True)
    assert reader.seekable == True
    assert reader.writer_has_stopped == False
    assert reader.keepends == True
    assert reader.num_bytes_read == 0
    assert reader.num_bytes_delimited == 0
    assert reader.num_lines_delimited == 0
    assert reader.num_lines_returned == 0
    assert isinstance(reader._queued_bytes, bytearray)
    assert isinstance(reader._delimited_bytes, deque)

def test_catchup_reader_set_seekable():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.set_seekable(False)
    assert reader.seekable == False

def test_catchup_reader_set_writer_as_stopped():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.set_writer_as_stopped(True)
    assert reader.writer_has_stopped == True

def test_catchup_reader_set_keepends():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.set_keepends(False)
    assert reader.keepends == False

def test_catchup_reader_read_from_bytes_no_newline():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.read(b"X")
    assert reader.num_bytes_read == 1
    assert reader.num_bytes_delimited == 0
    reader.set_writer_as_stopped()
    reader.read(b"")
    assert reader.num_bytes_read == 1
    assert reader.num_bytes_delimited == 1

def test_catchup_reader_read_from_bytes_one_newline():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.read(b"X\n")
    assert reader.num_bytes_read == 2
    assert reader.num_bytes_delimited == 2
    reader.set_writer_as_stopped()
    reader.read(b"")
    assert reader.num_bytes_read == 2
    assert reader.num_bytes_delimited == 2

def test_catchup_reader_read_from_bytes_one_newline_not_last():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.read(b"X\nX")
    assert reader.num_bytes_read == 3
    assert reader.num_bytes_delimited == 2
    reader.set_writer_as_stopped()
    reader.read(b"")
    assert reader.num_bytes_read == 3
    assert reader.num_bytes_delimited == 3

def test_catchup_reader_read_twice_from_bytes_one_newline_not_last():
    reader = CatchUpReader(seekable=True, keepends=True)
    reader.read(b"X")
    assert reader.num_bytes_read == 1
    assert reader.num_bytes_delimited == 0
    reader.read(b"\nX")
    assert reader.num_bytes_read == 3
    assert reader.num_bytes_delimited == 2
    reader.set_writer_as_stopped()
    reader.read(b"")
    assert reader.num_bytes_read == 3
    assert reader.num_bytes_delimited == 3

def test_catchup_reader_len_64k_no_newline():
    reader = CatchUpReader(seekable=True, keepends=True)
    with io.BytesIO(b"X" * 65536) as f:
        reader.read(f)
    assert reader.num_bytes_read == 65536
    assert reader.num_bytes_delimited == 0

def test_catchup_reader_len_64k_one_newline_xa_is_last():
    data = b"X" * 65535 + b"\n"
    reader = CatchUpReader(seekable=True, keepends=True)
    with io.BytesIO(data) as f:
        reader.read(f)
    assert reader.num_bytes_read == 65536
    assert reader.num_bytes_delimited == 65536

def test_catchup_reader_len_64k_one_newline_xa_is_not_last():
    STREAM_LEN = 65536
    data = bytearray(b"X" * STREAM_LEN)
    idx = random.randint(100, STREAM_LEN - 100)
    data[idx] = ord("\n")
    reader = CatchUpReader(seekable=True, keepends=True)
    with io.BytesIO(data) as f:
        reader.read(f)
        assert reader.num_bytes_read == 65536
        assert reader.num_bytes_delimited < 65536

def test_catchup_reader_len_64k_100_newline_xa_not_last():
    STREAM_LEN = 65536
    NEWLINE_COUNT = 100
    data = bytearray(b"X" * STREAM_LEN)
    num_newlines_to_add = NEWLINE_COUNT
    while num_newlines_to_add > 0:
        idx = random.randint(0, STREAM_LEN - 1)
        if data[idx] == ord("\n"):
            continue
        data[idx] = ord("\n")
        num_newlines_to_add -= 1
    reader = CatchUpReader(seekable=True, keepends=False)
    with io.BytesIO(data) as f:
        reader.read(f)
        assert reader.num_bytes_read == STREAM_LEN
        assert reader.num_bytes_delimited < STREAM_LEN
        reader.set_writer_as_stopped()
        reader.read(f)
        assert reader.num_bytes_delimited == STREAM_LEN
    total_chars = 0
    for line in reader.readlines():
        total_chars += line.count("X")
    assert total_chars == STREAM_LEN - NEWLINE_COUNT

###
### TODO add tests with truly non-seekable streams.
###

###
### TODO add tests with async sub-processes with arbitrary delays,
###      also with and without a final newline in their outputs.
###

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
