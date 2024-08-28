import builtins
from collections import deque
import io
import sys
import pytest
import random
import subprocess
from typing import Any, Protocol, runtime_checkable

from catchup_reader.src.catchup_reader import CatchUpReader, SeekableStream, ReadableStream

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

@pytest.mark.parametrize(
    "newline_delimiter,keepends",
    [
        (b"\n", False),
        (b"\n", True),
        (b"\r", False),
        (b"\r", True),
        ###
        ### NOTE docs\technical_notes\multi_byte_newline_delimiters.md
        ###
        pytest.param(b"\n\r", False, marks=pytest.mark.xfail),
        pytest.param(b"\n\r", True, marks=pytest.mark.xfail),
        pytest.param(b"\r\n", False, marks=pytest.mark.xfail),
        pytest.param(b"\r\n", True, marks=pytest.mark.xfail),
    ],
)
def test_catchup_reader_random_chop_multibyte_newlines(newline_delimiter: bytes, keepends: bool, capsys):
    LIMIT_DETAILS_PRINTED = 5
    ENABLE_PRINT = True
    def print_fn(*args):
        with capsys.disabled():
            builtins.print(*args)
    random_seeds = random.SystemRandom().randbytes(16)
    random_src = random.Random(random_seeds)
    PRINTABLES_PER_LINE = 100
    LINE_COUNT = 100
    LIMIT_BYTES_PER_SEND = 1000
    data = ((b"X" * PRINTABLES_PER_LINE) + newline_delimiter) * LINE_COUNT
    total_bytes = len(data)
    bytes_sent = 0
    reader = CatchUpReader(seekable=False, keepends=keepends)
    chop_history = list[int]()
    while bytes_sent < total_bytes:
        max_bytes_sendable = total_bytes - bytes_sent
        capped_bytes_sendable = min(max_bytes_sendable, LIMIT_BYTES_PER_SEND)
        chop_len = random_src.randint(1, capped_bytes_sendable)
        chop_history.append(chop_len)
        chopped_data = data[bytes_sent:bytes_sent + chop_len]
        bytes_sent += chop_len
        with io.BytesIO(chopped_data) as non_rewinding_source:
            reader.read(non_rewinding_source)
    reader.set_writer_as_stopped()
    reader.read(None)
    if ENABLE_PRINT:
        print_fn(f"Random seeds: {random_seeds.hex()}")
        print_fn(f"Chop history: {chop_history}")
        actual_text = list(reader.readlines())
        expected_line_len = PRINTABLES_PER_LINE + (len(newline_delimiter) if keepends else 0)
        num_details_printed = 0
        for actual_line_idx, actual_line in enumerate(actual_text):
            actual_line_len = len(actual_line)
            print_fn(f"[{actual_line_idx}] len={actual_line_len}")
            if actual_line_len != expected_line_len:
                if num_details_printed > LIMIT_DETAILS_PRINTED:
                    print_fn('    (Details omitted.)')
                    break
                else:
                    num_details_printed += 1
                printable_line = actual_line.replace("\n", "n").replace("\r", "r")
                print_fn('    "' + printable_line + '"')
    assert reader.num_bytes_read == total_bytes
    assert reader.num_bytes_delimited == total_bytes
    for actual_line_idx, actual_line in enumerate(actual_text):
        actual_line_len = len(actual_line)
        assert actual_line_len == expected_line_len
    assert len(actual_text) == LINE_COUNT


@runtime_checkable
class MockStreamProtocol(ReadableStream, SeekableStream, Protocol):
    pass


class MockStreamSpy(MockStreamProtocol):
    _impl: MockStreamProtocol
    _call_history: list[tuple[str, Any, Any]]

    def __init__(self, impl: MockStreamProtocol, call_history: list[tuple[str, Any, Any]]):
        """A decorator for a minimal stream protocol that captures all calls to its methods.
        Arguments:
            impl
                The stream protocol implementation to spy on.
            call_history
                A list for storing captured calls. This list must be provided by the caller
                (test function) and is modified when calls are captured.
        """
        assert isinstance(impl, MockStreamProtocol)
        assert isinstance(call_history, list)
        self._impl = impl
        self._call_history = call_history

    def readable(self) -> bool:
        call_result = ("readable", None, self._impl.readable())
        self._call_history.append(call_result)
        return call_result[2]
    
    def read(self, size: int) -> bytes:
        call_result = ("read", size, self._impl.read(size))
        self._call_history.append(call_result)
        return call_result[2]
    
    def seekable(self) -> bool:
        call_result = ("seekable", None, self._impl.seekable())
        self._call_history.append(call_result)
        return call_result[2]
    
    def seek(self, offset: int, whence: int) -> int:
        call_result = ("seek", (offset, whence), self._impl.seek(offset, whence))
        self._call_history.append(call_result)
        return call_result[2]

def test_catchup_reader_noseek_since_init():
    LEN_X = 100
    LEN_Y = 100
    data_x = b"X" * LEN_X
    data_y = b"Y" * LEN_Y
    reader = CatchUpReader(seekable=False, keepends=True)
    history = list[tuple[str, Any, Any]]()
    with io.BytesIO(data_x) as stream_x:
        spy_x = MockStreamSpy(stream_x, history)
        reader.read(spy_x)
    with io.BytesIO(data_y) as stream_y:
        spy_y = MockStreamSpy(stream_y, history)
        reader.read(spy_y)
    for name, _, _ in history:
        assert name != "seek"
    reader.set_writer_as_stopped()
    reader.read(None)
    line = reader.readline()
    assert len(line) == LEN_X + LEN_Y

def test_catchup_reader_seek_then_noseek():
    LEN_X = 100
    LEN_Y = 100
    data_x = b"X" * LEN_X
    data_y = b"Y" * LEN_Y
    ###
    ### Initialize as seekable.
    ###
    reader = CatchUpReader(seekable=True, keepends=True)
    history = list[tuple[str, Any, Any]]()
    with io.BytesIO(data_x) as stream_x:
        spy_x = MockStreamSpy(stream_x, history)
        reader.read(spy_x)
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert contains_seek
    ###
    ### Second part, set to no seek.
    ### Should ready data_y in full.
    ### Final result should be data_x + data_y.
    ###
    history.clear()
    ###
    reader.set_seekable(False)
    with io.BytesIO(data_y) as stream_y:
        spy_y = MockStreamSpy(stream_y, history)
        reader.read(spy_y)
    reader.set_writer_as_stopped()
    reader.read(None)
    ###
    line = reader.readline()
    assert len(line) == LEN_X + LEN_Y
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert not contains_seek

def test_catchup_reader_noseek_then_seek_read_none():
    LEN_X = 100
    LEN_Y = 100
    data_x = b"X" * LEN_X
    data_y = b"Y" * LEN_Y
    ###
    ### Initialize as not seekable.
    ###
    reader = CatchUpReader(seekable=False, keepends=True)
    history = list[tuple[str, Any, Any]]()
    ###
    with io.BytesIO(data_x) as stream_x:
        spy_x = MockStreamSpy(stream_x, history)
        reader.read(spy_x)
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert not contains_seek
    ###
    ### Second part, set to seek.
    ### Should not read anything from data_y, since it will be
    ### seeked to the number of bytes already read, which matches
    ### the length of data_y.
    ### Final result should be data_x.
    ###
    history.clear()
    reader.set_seekable(True)
    ###
    with io.BytesIO(data_y) as stream_y:
        spy_y = MockStreamSpy(stream_y, history)
        reader.read(spy_y)
    reader.set_writer_as_stopped()
    reader.read(None)
    ###
    line = reader.readline()
    assert len(line) == LEN_X
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert contains_seek


def test_catchup_reader_noseek_then_seek_read_partial():
    LEN_X = 100
    LEN_YZ = (LEN_X, 50)
    data_x = b"X" * LEN_X
    data_yz = (b"Y" * LEN_YZ[0]) + (b"Z" * LEN_YZ[1])
    ###
    ### Initialize as not seekable.
    ###
    reader = CatchUpReader(seekable=False, keepends=True)
    history = list[tuple[str, Any, Any]]()
    ###
    with io.BytesIO(data_x) as stream_x:
        spy_x = MockStreamSpy(stream_x, history)
        reader.read(spy_x)
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert not contains_seek
    ###
    ### Second part, set to seek.
    ### Should seek to LEN_X, and then read the remainder of data_yz.
    ### Final result would contain "X" and "Z", but not "Y".
    ###
    history.clear()
    reader.set_seekable(True)
    ###
    with io.BytesIO(data_yz) as stream_yz:
        spy_yz = MockStreamSpy(stream_yz, history)
        reader.read(spy_yz)
    reader.set_writer_as_stopped()
    reader.read(None)
    ###
    line = reader.readline()
    assert len(line) == LEN_X + LEN_YZ[1]
    ###
    contains_seek = False
    for name, _, _ in history:
        contains_seek |= (name == "seek")
    assert contains_seek
    ###
    assert "X" in line
    assert "Y" not in line
    assert "Z" in line


def test_catchup_reader_seek_truncated(capsys):
    ENABLE_PRINT = False
    def print_fn(*args):
        with capsys.disabled():
            builtins.print(*args)
    LEN_1 = 100
    LEN_2 = 50
    data_1 = b"X" * LEN_1
    data_2 = b"Y" * LEN_2
    reader = CatchUpReader(seekable=True, keepends=True)
    history = list[tuple[str, Any, Any]]()
    with io.BytesIO(data_1) as stream_1:
        spy_1 = MockStreamSpy(stream_1, history)
        reader.read(spy_1)
    with io.BytesIO(data_2) as stream_2:
        spy_2 = MockStreamSpy(stream_2, history)
        reader.read(spy_2)
    if ENABLE_PRINT:
        print_fn(f"\nCall history:")
        for name, args, rets in history:
            print_fn(f"    {name}({args}) -> ({rets})")
    assert reader.num_bytes_read == LEN_1

###
### TODO add tests with async sub-processes with arbitrary delays,
###      also with and without a final newline in their outputs.
###

if __name__ == "__main__":
    sys.exit(pytest.main(["-v", __file__]))
