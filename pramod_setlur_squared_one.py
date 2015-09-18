import MapReduce
import sys

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

    j = record[0]
    k = record[1]
    element = record[2]

    #Emit element as B matrix
    for i in range(0,5):
        mr.emit_intermediate((i,k),('B',j,element))

def reducer(key, value_list):
    #key: i,k
    #dict_values: values to be first multiplied and then added
    j_list = []
    j_dictionary = {}

    for value in value_list:
        j = extract_j(value)
        j_dictionary.setdefault(j, [])
        j_dictionary[j].append(extract_element(value))
    j_list.append(j_dictionary)

    multipled_results = []
    for each_dict in j_list:
        for result_matrix_key, multiplying_numbers in each_dict.iteritems():
            if (len(multiplying_numbers) == 2):
                mult_result = multiplying_numbers[0] * multiplying_numbers[1]
                multipled_results.append(mult_result)

    addition_result = 0
    for each_number in multipled_results:
        addition_result += each_number

    mr.emit ([key[0], key[1], addition_result])

def extract_j(value):
    return value[1]

def extract_element(value):
    return value[2]

# Do not modify below this line
# =============================
if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
