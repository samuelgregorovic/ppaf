# from writer import write_ppaf
# from validator import validate_ppaf
from ppaf import PAFFFile

# mult = 100000000
mult = 1
# Testing the functions
partitions = ["A" * mult, "B" * mult, "C" * mult, "D" * mult]
with PAFFFile("demo.ppaf") as file:
    file.write_partitions(partitions)
    file.validate()
    partitions_test = file.read_partitions()
    # print(partitions_test)
    assert partitions == partitions_test
