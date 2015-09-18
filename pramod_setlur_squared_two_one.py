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
    length_list = len(value_list)
    for i in range (0, length_list-1):
        for j in range (i+1, length_list):
            if (value_list[i][0] != value_list[j][0]): #Check if A and B are different
                if (value_list[i][0] == 'A'): #If the element is from A then emit (i,k)
                    mr.emit([value_list[i][1],value_list[j][1],(value_list[i][2]*value_list[j][2])])
                else: #If the element is from B then emit (k,i)
                    mr.emit([value_list[j][1],value_list[i][1],(value_list[i][2]*value_list[j][2])])

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
