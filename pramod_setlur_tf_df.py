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
    regex = '\w+'
    words = re.findall(regex, text)
    for w in words:
        #Create a dictionary with {document_id, 1} and pass that as the value
        dict_tf = {document_id: 1}
        mr.emit_intermediate(str(w), dict_tf)

def reducer(key, dict_values):
    # key: word
    # dict_values: dictionary of document_id, 1 for each occurance of the string in the document
    dict_documents =  {}
    for doc_tf_tuple in dict_values:
        for document_id, tf in doc_tf_tuple.iteritems():
            dict_documents.setdefault(document_id, 0)
            old_tf = dict_documents[document_id]
            dict_documents[document_id] = old_tf + 1

    list_documents = []
    for document_id, tf in dict_documents.iteritems():
        doc_tf_pair = [document_id, tf]
        list_documents.append(doc_tf_pair)

    df = len(dict_documents)
    mr.emit((key,df, list_documents))

# Do not modify below this line
# =============================
if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
