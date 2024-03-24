import hashlib


def get_version_str(version):
    return f"PAFF {version}\n"


def get_mode_str(mode):
    return f"mode: {mode}\n"


def calculate_checksum(partition):
    return hashlib.md5(partition.encode("utf-8")).hexdigest()
