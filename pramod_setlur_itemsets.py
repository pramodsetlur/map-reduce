import MapReduce
import sys
import itertools

"""
Problem 2 - Frequent item sets
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #record: Each line in the transaction file
    combinations_list = record
    if (len(record) > 1):
        combinations_of_two = itertools.combinations(record, 2)
        combinations_list = list(combinations_of_two)

    for i in range(len(combinations_list)):
        mr.emit_intermediate(combinations_list[i],1)

def reducer(key, dict_values):
    #key: one of the combinations
    #dict_values: dictionary of document_id, 1 for each occurance of the string in the document
    if len(dict_values) > 100:
        mr.emit(key)

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
