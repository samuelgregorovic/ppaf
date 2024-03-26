import os
import asyncio
import hashlib
import aiofiles
import ctypes

# Constants from the Linux kernel source
FALLOC_FL_INSERT_RANGE = (
    0x20  # This value may vary, check your system's fcntl.h for exact value
)

# Load the C library
LIBC = ctypes.CDLL("libc.so.6")


class PAFFFile:
    DEFAULT_VERSION = "1.0"
    DEFAULT_MODE = "1"

    def __init__(self, file_path, version=DEFAULT_VERSION, mode=DEFAULT_MODE):
        if not file_path:
            raise ValueError("file_path is required")
        self.file_path = file_path
        self._version = version
        self._mode = mode
        self._metadata = None
        self._load_metadata_async()

    def _load_metadata_async(self):
        with open(self.file_path, "r") as file:
            self._metadata = [file.readline() for _ in range(4)]

    @property
    def version(self):
        return self._version

    @property
    def mode(self):
        return self._mode

    @property
    def partition_count(self):
        if not self._metadata:
            return 0
        return int(self._metadata[2].strip().split()[1])

    def get_version_line(self):
        return f"PAFF {self.version}\n"

    def get_mode_line(self):
        return f"mode {self.mode}\n"

    def get_partition_count_line(self, part_count):
        if not part_count:
            return f"parts {self.partition_count}\n"
        return f"parts {part_count}\n"

    @property
    def offsets_map(self):
        if not self._metadata:
            return {}
        parts = self._metadata[3].strip().split()
        return {
            i: (int(parts[i * 3]), int(parts[i * 3 + 1]), parts[i * 3 + 2])
            for i in range(self.partition_count)
        }

    async def read_partition_async(self, index):
        if index < 0 or index >= self.partition_count:
            raise ValueError("Index out of range")

        metadata_parts = self._metadata[3].strip().split()
        offset, size = int(metadata_parts[index * 3]), int(
            metadata_parts[index * 3 + 1]
        )

        # Ensure the data start offset calculation is correct
        # You should calculate the offset precisely where the partition data starts
        data_start_offset = sum(map(len, self._metadata))  # Adding 4 for the newlines

        async with aiofiles.open(self.file_path, "r") as file:
            await file.seek(
                data_start_offset + offset - 1
            )  # Adjust offset if necessary
            return await file.read(size)

    def read_partition(self, index):
        return asyncio.run(self.read_partition_async(index))

    async def read_partitions_async(self, indices=None):
        if indices is None:
            indices = range(self.partition_count)
        return await asyncio.gather(
            *[self.read_partition_async(index) for index in indices]
        )

    def read_partitions(self, indices=None):
        return asyncio.run(self.read_partitions_async(indices))

    def calculate_checksum(self, partition):
        return hashlib.md5(partition.encode("utf-8")).hexdigest()

    async def validate_async(self):
        self._load_metadata_async()
        version_line, mode_line, parts_line, metadata_line = self._metadata

        assert version_line.strip() == f"PAFF {self.version}", "Version mismatch"
        assert mode_line.strip() == f"mode {self.mode}", "Mode mismatch"
        assert parts_line.strip().startswith("parts"), "Parts line format error"

        n_parts = int(parts_line.strip().split()[1])
        assert n_parts == self.partition_count, "Partition count mismatch"

        data_starts_after = sum(len(line) for line in self._metadata)  # +4 for newlines

        async with aiofiles.open(self.file_path, "r") as file:
            await file.seek(data_starts_after)
            metadata_parts = metadata_line.strip().split()
            for i in range(n_parts):
                offset, size = int(metadata_parts[i * 3]), int(
                    metadata_parts[i * 3 + 1]
                )
                checksum = metadata_parts[i * 3 + 2]
                await file.seek(data_starts_after + offset - 1)  # Adjust offset
                partition = await file.read(size)
                assert (
                    self.calculate_checksum(partition) == checksum
                ), f"Checksum mismatch at partition {i+1}"

        print("File OK!")

    def validate(self):
        return asyncio.run(self.validate_async())

    async def write_partitions_async(self, partitions):
        # Placeholder for the actual implementation
        header_str = (
            self.get_version_line()
            + self.get_mode_line()
            + self.get_partition_count_line(len(partitions))
        )

        metadata_str = ""
        data_str = ""
        offsets = [1]  # Start from 1 after metadata line

        for i, partition in enumerate(partitions):
            size = len(partition)
            checksum = self.calculate_checksum(partition)
            metadata_str += f"{' ' if i else ''}{offsets[-1]} {size} {checksum}"
            data_str += partition
            if len(partitions) > len(offsets):  # Prepare offset for the next partition
                offsets.append(offsets[-1] + size)
        async with aiofiles.open(self.file_path, "w", encoding="utf-8") as f:
            await f.write(f"{header_str}{metadata_str}\n{data_str}")

    def write_partitions(self, partitions):
        asyncio.run(self.write_partitions_async(partitions))

    async def add_partition_async(self, data, index=None):
        if not self._metadata:
            await self._load_metadata_async()

        new_size = len(data)
        new_checksum = self.calculate_checksum(data)

        # Determine where to insert the new data partition
        if index is None or index >= self.partition_count:
            if self.partition_count > 0:
                last_offset, last_size, _ = max(self.offsets_map.values())
                new_offset = last_offset + last_size
            else:
                new_offset = sum(
                    len(line) for line in self._metadata[:-1]
                )  # Assuming metadata is at the beginning
        else:
            new_offset, _, _ = self.offsets_map[index]

        # Calculate the actual position to write the new data
        data_starts_after = sum(len(line) for line in self._metadata)
        new_data_position = data_starts_after + new_offset - 1

        # Write the new data
        fd = os.open(self.file_path, os.O_RDWR)
        self.allocate_range(fd, new_data_position, new_size)

        with os.fdopen(fd, "r+b") as file:
            file.seek(new_data_position)
            file.write(data.encode("utf-8"))

            # Update in-memory metadata
            new_metadata_entry = f"{new_offset} {new_size} {new_checksum}\n"
            updated_metadata = (
                self._metadata[3].strip() + " " + new_metadata_entry
                if self._metadata[3].strip()
                else new_metadata_entry
            )
            self._metadata[3] = updated_metadata
            self._metadata[2] = self.get_partition_count_line(self.partition_count + 1)
            full_metadata = "".join(self._metadata)

            additional_space = len(full_metadata)
            # exit()
            self.allocate_range(fd, 0, additional_space)
            # exit()
            # Write the updated metadata at the beginning of the file
            # exit()
            file.seek(0)
            file.write(full_metadata.encode("utf-8"))

        # Update the partition count and offsets map
        # self.partition_count += 1
        self.offsets_map[self.partition_count] = (new_offset, new_size, new_checksum)

    def allocate_range(self, file_descriptor, offset, size):
        # Define the fallocate function prototype
        fallocate = LIBC.fallocate
        fallocate.argtypes = [
            ctypes.c_int,
            ctypes.c_int,
            ctypes.c_int64,
            ctypes.c_int64,
        ]
        fallocate.restype = ctypes.c_int

        result = fallocate(file_descriptor, FALLOC_FL_INSERT_RANGE, offset, size)
        # if result != 0:
        #     raise OSError(
        #         ctypes.get_errno(),
        #         f"Error calling fallocate: {os.strerror(ctypes.get_errno())}",
        #     )

    def add_partition(self, data, index=None):
        asyncio.run(self.add_partition_async(data, index))

    def update_partition(self, index, data):
        # Placeholder for the actual implementation
        pass

    def remove_partition(self, index):
        # Placeholder for the actual implementation
        pass

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass  # Add cleanup logic if necessary

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Handle closing of resources if necessary
        pass
