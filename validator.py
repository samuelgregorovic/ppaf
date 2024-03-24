from ppaf import VERSION, MODE
from utils import get_version_str, get_mode_str, calculate_checksum


def validate_ppaf(filename: str, version: str = VERSION, mode: str = MODE):
    with open(filename, "r", encoding="utf-8") as f:
        version_line = f.readline().strip()
        mode_line = f.readline().strip()
        metadata_line = f.readline().strip()

        assert version_line == get_version_str(version).strip()
        assert mode_line == get_mode_str(mode).strip()

        metadata_parts = metadata_line.split()
        n_parts = int(metadata_parts[0])
        data_starts_after = (
            len(version_line) + len(mode_line) + len(metadata_line) + 3
        )  # +3 for newlines

        partitions = []
        for i in range(n_parts):
            offset = (
                int(metadata_parts[i * 3 + 1]) + data_starts_after - 1
            )  # Adjust offset position
            size = int(metadata_parts[i * 3 + 2])
            checksum = metadata_parts[i * 3 + 3]

            f.seek(offset)
            partition = f.read(size)
            assert (
                calculate_checksum(partition) == checksum
            ), f"Checksum mismatch at partition {i+1}"
            partitions.append(partition)

    print("File OK!")
    return partitions
