# Spurious blank lines when stream contains multi-byte newline delimiters

### Description

This is a known issue. 

When the stream contains multi-byte newline delimiters, such as ```(0x0d, 0x0a)```
or ```(0x0a, 0x0d)```, occasionally the reader might receive the first byte of
the delimiter sequence but not the second, due to the normal operating behaviors
of buffered pipes and streams.

When this happens, CatchUpReader will delimit a line of text containing the first
delimiter byte. Later, when CatchUpReader receives the second byte of the delimiter
sequence, it will interpret that as a second newline. This causes a blank line
to be read into the text queue.

### Impact on test suite execution

For test functions that rely on randomization to simulate reception of incomplete
data, such tests might pass or fail depending on whether a multi-byte newline
delimiter sequence was received as a whole or separately. Thus, such test cases
may pass or fail at random.

These test cases have been marked with ```pytest.mark.xfail```.

For ease of reproduction, printing is enabled on these test cases, and the
randomization seed bytes are included as part of print output.

### Resolution

Currently there is no plan for a fix.
