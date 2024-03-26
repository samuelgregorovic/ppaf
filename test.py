import os
import ctypes

# Load the C library to access fallocate
libc = ctypes.CDLL("libc.so.6")
fallocate = libc.fallocate
fallocate.restype = ctypes.c_int
fallocate.argtypes = [ctypes.c_int, ctypes.c_int, ctypes.c_int64, ctypes.c_int64]

# Constants from fcntl.h for fallocate
FALLOC_FL_KEEP_SIZE = 0x01
FALLOC_FL_INSERT_RANGE = 0x20


def insert_text_in_file(filename, offset, text):
    # Open the file
    fd = os.open(filename, os.O_RDWR)

    # Calculate the length of text to be inserted
    length = len(text)

    # Use fallocate to insert space in the file
    result = fallocate(fd, 0, 0, 1000)

    result = fallocate(fd, FALLOC_FL_INSERT_RANGE, offset, length)
    # if result != 0:
    #     errno = ctypes.get_errno()
    #     print(f"fallocate error: {os.strerror(errno)}")
    #     os.close(fd)
    #     return

    # Write the new text into the space created
    os.lseek(fd, offset, os.SEEK_SET)
    os.write(fd, text.encode("utf-8"))

    os.close(fd)


# Usage example
filename = "testfile.txt"
insert_text = "INSERTED TEXT "
insert_offset = 6  # After "Hello,"
insert_text_in_file(filename, insert_offset, insert_text)
