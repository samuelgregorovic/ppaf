import time
from ppaf import PAFFFile


file_path = "demo.ppaf"
# Timing the parallel read
start_time = time.time()
ppaf_file = PAFFFile(file_path)
part_data = ppaf_file.read_partition(0)
end_time = time.time()
parallel_time = end_time - start_time
print(f"Partition read time: {parallel_time:.4f} seconds")
print(f"data len: {len(part_data)}")
# print(f"data: {part_data}")
