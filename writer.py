from ppaf import VERSION, MODE
from utils import get_mode_str, get_version_str, calculate_checksum


def write_ppaf(
    filename: str, partitions: list[str], version: str = VERSION, mode: str = MODE
):
    header_str = get_version_str(version) + get_mode_str(mode) + str(len(partitions))
    metadata_str = ""
    data_str = ""
    offsets = [1]  # Start from 1 after metadata line

    for partition in partitions:
        size = len(partition)
        checksum = calculate_checksum(partition)
        metadata_str += f" {offsets[-1]} {size} {checksum}"
        data_str += partition
        if len(partitions) > len(offsets):  # Prepare offset for the next partition
            offsets.append(offsets[-1] + size)

    with open(filename, "w", encoding="utf-8") as f:
        f.write(f"{header_str}{metadata_str}\n{data_str}")
