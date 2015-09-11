import MapReduce
import sys
import re

"""
Problem 1 - Term Frequency and Document Frequency
"""

mr = MapReduce.MapReduce()

# =============================
# Do not modify above this line

def mapper(record):
    # key: document identifier
    # value: document contents
    document_id = record[0]
    text = record[1]
    regex = r'\w+'
    words = re.findall(regex, text)
    #print words
    for w in words:
        #Create a dictionary with {document_id, 1} and pass that as the value
        #print w
        dict_tf = {document_id: 1}
        mr.emit_intermediate(w, dict_tf)

def reducer(key, dict_values):
    # key: word
    # value: list of occurrence counts
    dict_documents =  {}
    for document_id, tf in dict_values.iteritems():
        dict_documents.setdefault(document_id, 0)
        old_tf = dict_documents[document_id]
        dict_documents[document_id] = old_tf + 1
    df = len(dict_documents)
    value = [df, dict_documents]
    mr.emit(key, value)

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
