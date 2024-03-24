import time
from ppaf import PAFFFile

file_path = "demo.ppaf"
# Timing the parallel read
start_time = time.time()
paff_file = PAFFFile(file_path)
data = paff_file.read_partitions([0, 1])
end_time = time.time()
parallel_time = end_time - start_time
print(f"Parallel read time: {parallel_time:.4f} seconds")
print(f"data len: {len(data)}")
print(f"data: {data}")
