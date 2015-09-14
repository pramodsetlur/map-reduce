import MapReduce
import sys
import itertools

"""
Problem 3 - Multiplication of matrices using single phase
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #record: Each line is an element in the matrix. The input is a sparsed matrix format
    i = record[0]
    j = record[1]
    element = record[2]

    #Emit element for the A matrix
    for k in range(0,5):
        mr.emit_intermediate((i,k),('A',j,element))
        print ((i,k),('A',j,element))
    #Emit element as B matrix
    for i in range(0,5):
        mr.emit_intermediate((i,k),('B',j,element))
        print ((i,k),('B',j,element))

def reducer(key, dict_values):
    #key: one of the combinations
    #dict_values: dictionary of document_id, 1 for each occurance of the string in the document
    print "test"

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
