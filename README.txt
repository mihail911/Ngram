CS 149 PA3 - Ngram

Collaborators: Mihail Eric - meric
               Raymond Wu  - wur911

We first created our own InputFormat class that we labeled as NgramInputFormat.
We implemented a Record Reader layered on top of a line record reader. Our 
record reader did the following:
1) Find the title and save it
2) Read in the body of the text until you reach the next title
3) Output the saved title as the key and the body as the value
4) Save the new title read in to replace the old title
5) Repeat
The record reader thus skipped the unlabeled beginning of the file input and
and the incomplete last document. 

In the main function, we parsed the query and we created a HashSet for the 
ngrams in each map task. 

We used a mapper for each fileinput. The mapper tokenized the value (the text
of the document) and created ngrams. It checked if the mapper was in the 
hashset, and incremented the score if so. Then it outputted the result with 
a key of 1 and a value of a (title,score) string.

The combiner took in the results from a mapper and found the top 20 scores,
breaking ties by taking the lexigraphically last title. It then output the 
top 20 with a key of 1 and a value of a [(score,title),(score,title),...]
string.

Lastly, the reducer took in results from the mapper and collected the top 20
results of the groups of 20 results it received from the combiner. 
It outputted these as a "score \tab title" string.

The runtime is O(q + n/p + logP). The main function reads in the query and 
takes O(q) time. Since each mapper is running in parallel, the HashSet creation
takes O(q) time and the creation of document ngrams takes O(n/p) time since
we are dividing the input between p processors. The combiner took O(n/p) time
since it processed O(n/p) intermediate outputs from the mapper. The reducer
received results from the p combiners and thus took O(p) time. 

