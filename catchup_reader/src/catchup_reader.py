from collections import deque
import os
from typing import Iterable, Optional, overload, Protocol, runtime_checkable, Union

__all__ = ['CatchUpReader']

@runtime_checkable
class ReadableStream(Protocol):
    def readable(self) -> bool: ...
    def read(self, size: int) -> bytes: ...

@runtime_checkable
class SeekableStream(Protocol):
    def seekable(self) -> bool: ...
    def seek(self, offset: int, whence: int) -> int: ...

class CatchUpReader:
    """A class that reads text lines from a stream (file or pipe) in a non-blocking way
    while the stream is being written into from a separate process.
    """
    _seekable: bool
    _writer_has_stopped: bool
    _keepends: bool
    _num_bytes_read: int
    _num_bytes_delimited: int
    _num_lines_delimited: int
    _num_lines_returned: int
    _queued_bytes: bytearray
    _delimited_bytes: deque[bytearray]

    def __init__(self, seekable: bool, keepends: bool) -> None:
        self._seekable = seekable
        self._writer_has_stopped = False
        self._keepends = keepends
        self._num_bytes_read = 0
        self._num_bytes_delimited = 0
        self._num_lines_delimited = 0
        self._num_lines_returned = 0
        self._queued_bytes = bytearray()
        self._delimited_bytes = deque[bytearray]()

    @property
    def seekable(self) -> bool:
        return self._seekable

    def set_seekable(self, value: bool = True) -> None:
        """Specifies whether the stream is seekable. If the stream is seekable, each
        call to read() will set the stream position to the end of the last read position.
        """
        self._seekable = value

    @property
    def writer_has_stopped(self) -> bool:
        return self._writer_has_stopped

    def set_writer_as_stopped(self, value: bool = True) -> None:
        """Indicates that the writer has stopped writing to the stream. On the next call 
        to read(), all queued bytes will be converted into delimited bytes, regardless of
        whether they contain newline delimiters or not.
        """
        self._writer_has_stopped = value

    @property
    def keepends(self) -> bool:
        return self._keepends

    def set_keepends(self, value: bool) -> None:
        """Specifies whether the readline() and readlines() methods should include
        newlines in their output.
        """
        self._keepends = value

    @property
    def num_bytes_read(self) -> int:
        return self._num_bytes_read

    @property
    def num_bytes_delimited(self) -> int:
        return self._num_bytes_delimited

    @property
    def num_lines_delimited(self) -> int:
        return self._num_lines_delimited

    @property
    def num_lines_returned(self) -> int:
        return self._num_lines_returned

    @overload
    def read(self, stream: ReadableStream) -> None: ...

    @overload
    def read(self, stream: Union[bytes, bytearray]) -> None: ...

    def read(self, stream: Union[ReadableStream, bytes, bytearray, None]) -> None:
        """Reads as much data as possible from the source stream, and performs
        newline delimiter processing on the data.
        """
        if stream is None:
            new_data = bytes()
        elif isinstance(stream, (bytes, bytearray)):
            new_data = stream
        elif isinstance(stream, ReadableStream):
            if self._seekable:
                assert isinstance(stream, SeekableStream)
                if not stream.seekable():
                    self._seekable = False
                else:
                    stream.seek(self._num_bytes_read, os.SEEK_SET)
            if stream.readable():
                new_data = stream.read(-1) or bytes()
            else:
                new_data = bytes()
        else:
            raise TypeError("stream must be either bytes, bytearray, or a readable stream.")
        self._num_bytes_read += len(new_data)
        ### NOTE it is always necessary to call _process_new_data() in order
        ###      to handle the case where the writer has stopped and all
        ###      queued bytes must be converted into delimited bytes.
        self._process_new_data(new_data)

    def readline(self) -> Optional[str]:
        """Pops a decoded text line from the queue.
        Returns None if the decoded text line queue is empty.
        """
        if len(self._delimited_bytes) > 0:
            line = self._delimited_bytes.popleft().decode()
            if not self._keepends:
                line = line.rstrip("\r\n")
            self._num_lines_returned += 1
            return line
        return None

    def readlines(self) -> Iterable[str]:
        """Pops all decoded text lines from the queue.
        """
        while True:
            line = self.readline()
            if line is None:
                break
            yield line

    def __iter__(self) -> Iterable[str]:
        return self.readlines()

    def _scan_last_newline(self, new_data: bytes) -> int:
        """Returns the index of the last occurrence of the newline delimiter 
        (either '\n' or '\r'), whichever comes last in the new data. Returns -1
        if there is no occurrence.
        """
        last_n = new_data.rfind(b"\n")
        last_r = new_data.rfind(b"\r")
        return max(last_n, last_r)

    def _process_new_data(self, new_data: bytes) -> None:
        """Process the new data along with the queued bytes.
        
        If the writer has stopped, all bytes will be converted into delimited.
        (Specifically, the last line of text will be returned, regardless of
        whether it is newline-terminated or not.)

        Otherwise, the new bytes will be processed depending on whether it
        contains newline delimiters. If it does, the bytes will be split into
        two halves; the first half will be converted into delimited bytes, and
        the second half will will be queued for the next iteration.

        Note: this method can be called with empty new data. This is relevant
        when the writer has stopped, as the queued bytes are being processed
        regardless of the presense of newline delimiters.
        """
        if self.writer_has_stopped:
            if new_data:
                self._queued_bytes.extend(new_data)
            self._consume_queued_bytes()
        else:
            if not new_data:
                return
            last_delim = self._scan_last_newline(new_data)
            if last_delim < 0:
                self._queued_bytes.extend(new_data)
            else:
                cut_idx = last_delim + 1
                new_data_1 = new_data[:cut_idx]
                new_data_2 = new_data[cut_idx:]
                self._queued_bytes.extend(new_data_1)
                self._consume_queued_bytes()
                self._queued_bytes = bytearray(new_data_2)

    def _consume_queued_bytes(self) -> None:
        """Convert all queued bytes into delimited bytes.
        When this method finishes, the queued bytes will be empty.
        """
        self._num_bytes_delimited += len(self._queued_bytes)
        raw_lines = self._queued_bytes.splitlines(keepends=True)
        self._delimited_bytes.extend(raw_lines)
        self._num_lines_delimited += len(raw_lines)
        self._queued_bytes = bytearray()
