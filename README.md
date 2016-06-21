# map-reduce
Data Mining Assignment #1: This project is based on Map Reduce. 

###How to execute:
`$ python *filename*.py data/*filename*`

The WordCount was an example given to us.
This counts the number of words in the document data/books.json

The mapper spits out a k-v pair as <word,1> for every word. The reducer inturn aggregates the counts for each word and tells the count.

There are 4 parts in this assignment:
###1. Term Frequency and Document Frequency
Term frequency is the number of times the word has repeated in each document. And, the document frequency is the number of documents the word has repeated in.

`$python pramod_setlur_tf_df.py data/book(1).json`
The output should look similar to ./output\ samples/tf_df_output.json


###2. Frequent itemsets
Each line of the input is a transaction made by a customer. We try to find out the pair of commodities that are bought by all the customers which account to be more than 100.

`$python pramod_setlur_itemsets.py data/transactions.json`
The output should look similar to ./output\ samples/itemsets_output.json

###3. Matrix Squaring using single phase
The input to the program is a sparsed matrix. The given matrix is squared with itself. If an element is missing it is considered as 0.

`$python pramod_setlur_squared_one.py data/matrix.json`
The output should look similar to ./output\ samples/squared_one_output.json

###4. Matrix Squared using 2 phases
The input to the matrix is again a sparsed matrix.

`$python pramod_setlur_squared_two_one.py data/matrix.json > /tmp/result`
`$python pramod_setlur_squared_two_two.py /tmp/result`
The output should look similar to ./output\ samples/squared_two_output.json
