import asyncio
import hashlib

import aiofiles


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
        asyncio.run(self._load_metadata_async())

    async def _load_metadata_async(self):
        async with aiofiles.open(self.file_path, "r") as file:
            self._metadata = [await file.readline() for _ in range(4)]

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

    def get_partition_count_line(self):
        return f"parts {self.partition_count}\n"

    @property
    def offsets_map(self):
        if not self._metadata:
            return {}
        parts = self._metadata[3].strip().split()
        return {
            int(parts[i * 3]): (int(parts[i * 3 + 1]), parts[i * 3 + 2])
            for i in range(self.partition_count)
        }

    async def read_partition_async(self, index):
        if not self._metadata:
            await self._load_metadata_async()

        if index >= self.partition_count or index < 0:
            raise ValueError("Index out of range")

        metadata_parts = self._metadata[3].strip().split()
        offset, size = int(metadata_parts[index * 3]), int(
            metadata_parts[index * 3 + 1]
        )
        data_start_offset = sum(map(len, self._metadata)) - 1  # Counting newlines

        async with aiofiles.open(self.file_path, "r") as file:
            await file.seek(data_start_offset + offset)
            return await file.read(size)

    def read_partition(self, index):
        return asyncio.run(self.read_partition_async(index))

    async def read_partitions_async(self, indices=None):
        if not self._metadata:
            await self._load_metadata_async()

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
        if not self._metadata:
            await self._load_metadata_async()

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
            + self.get_partition_count_line()
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

        with open(self.file_path, "w", encoding="utf-8") as f:
            f.write(f"{header_str}{metadata_str}\n{data_str}")

    def write_partitions(self, partitions):
        asyncio.run(self.write_partitions_async(partitions))

    def update_partition(self, index, data):
        # Placeholder for the actual implementation
        pass

    def add_partition(self, data):
        # Placeholder for the actual implementation
        pass

    def remove_partition(self, index):
        # Placeholder for the actual implementation
        pass

    async def __aenter__(self):
        await self._load_metadata_async()
        return self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass  # Add cleanup logic if necessary

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        # Handle closing of resources if necessary
        pass
