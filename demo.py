from writer import write_ppaf
from validator import validate_ppaf

# Testing the functions
partitions = ["A", "B", "C", "D"]
write_ppaf("demo.ppaf", partitions)
partitions_test = validate_ppaf("demo.ppaf")
assert partitions == partitions_test
