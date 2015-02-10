Craig Bryan - 6965144 - cbrya049@uottawa.ca
Sean Billings - 6426637 - sbill042@uottawa.ca

CSI4107 Assignment 1

Quick Instructions:

To compile - from this folder, run:

javac -cp "./lib/lucene-queryparser-4.10.3.jar:./lib/lucene-analyzers-common-4.10.3.jar:./lib/lucene-core-4.10.3.jar" -d ./bin ./src/*

Ensure that ./res includes the following files:
input_tweets.txt - the tweet document file
test_queries.txt - the queries file
Trec_microblog11-qrels.txt - the relevance for each query - FOR EVALUATION

Ensure that lib contains the folder trec_eval.8.1

Ensure that lib/trec_eval.8.1 contains the source for trec_eval or a compiled 
version of trec_eval. The program will compile trec_eval if your computer has
gcc. If you do not have gcc, ensure you put the compiled trec_eval in the 
lib/trec_eval.8.1 folder.

To run - from this folder, run:

java -cp "./bin:./lib/lucene-queryparser-4.10.3.jar:./lib/lucene-analyzers-common-4.10.3.jar:./lib/lucene-core-4.10.3.jar" Assignment1Runner

The second command can also be run with addition command line arguments. To 
get a list of command line arguments, run this command to see the arguments that can be appended to the run command:

java -cp "./bin:./lib/lucene-queryparser-4.10.3.jar:./lib/lucene-analyzers-common-4.10.3.jar:./lib/lucene-core-4.10.3.jar" Assignment1Runner -h

Please read report.pdf for more information on the program.