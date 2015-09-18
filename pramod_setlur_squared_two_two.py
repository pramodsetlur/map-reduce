import MapReduce
import sys

"""
Problem 4 - Multiplication of matrices using two phases
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #record: Each line is an intermediate line passed in by the Reducer 1
    i = record[0]
    k = record[1]
    element = record[2]

    #Emit elements for the A matrix
    mr.emit_intermediate((i,k),element)

def reducer(key, value_list):
    #key: j
    #dict_values: all the values in list needs to be added
    total = 0
    for v in value_list:
      total += v
    mr.emit([key[0], key[1], total])

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
