import MapReduce
import sys

"""
Problem 4 - Multiplication of matrices using two phases
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    #record: Each line is an element in the matrix. The input is a sparsed matrix format
    i = record[0]
    j = record[1]
    element = record[2]

    #Emit elements for the A matrix
    mr.emit_intermediate(j, ('A', i, element))

    j = record[0]
    k = record[1]
    element = record[2]

    #Emit elements for the B matrix
    mr.emit_intermediate(j, ('B', k, element))

def reducer(key, value_list):
    #key: j
    #dict_values: list of lists. each list is a triplet
    for i in range (0, len(value_list)-1):
        for j in range (i+1, len(value_list)):
            if (value_list[i][0] != value_list[j][0]):
                if (value_list[i][0] == 'A'):
                    mr.emit([value_list[i][1],value_list[j][1],(value_list[i][2]*value_list[j][2])])
                else:
                    mr.emit([value_list[j][1],value_list[i][1],(value_list[i][2]*value_list[j][2])])

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
