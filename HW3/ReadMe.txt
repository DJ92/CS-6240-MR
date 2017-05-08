Homework3:

To run the Programs for calculating PageRank Values and getting Top K records:

1. Open Folder Homework3

Running using Intellij IDE:
1. Import as Maven Project
2. Run make alone command using the makefile provided
	- The make alone command takes "Job Name" as an argument
	- The argument can be passed as shown below:
		eg: make alone jobname=MainAggregateJobClass
	- The job name with the main function for creating the Job object is:
		* MainAggregateJobClass
3. Run Program

Running using Command Prompt/Terminal:
1. Extract all classes from Homework3/src/main/java into the same directory
2. javac <Job name>.java
	- The job name with the main function for creating the Job object is:
		* MainAggregateJobClass
3. java <jobname> <input path> <output path>

- The program shall write the output (Top-K records) to the output path specified in the above command.
	- The intermediate outputs from the parser and the 10 PageRank iterations are written to directories (result0 - result11)
	- The Top K Job picks up input from result11 directory and then generates the output in the <output path> directory.

Files that are used in this homework:

-> MainAggregateJobClass.java
	- AdjacencyListNodeClass.java
	- Bz2WikiParser.java
	- ParserMapperClass.java
	- ParserReducerClass.java
	- PageRankMapperClass.java
	- PageRankMapperClass.java
	- TopKMapperClass.java
	- TopKReducerClass.java

*Note: 5 Machines Folder contains the syslog and Top K 100 records output from EMR
*Note: 10 Machines Folder contains the syslog and Top K 100 records output from EMR
*Note: Simple Corpus Folder contains Top K 100 records output from local execution of simple wikipedia .bz2 corpus.
*Note: Parser has been tweaked to handle “&” symbols, self links, “~” and uniqueness of outlinks.
*Note: Sink Nodes have been handled in the ParserMapperClass by emitting every page in the list of outlinks immediately as a page is encountered with the default NodeClass Object.
In case of diamond erros, please make sure the maven dependency in pom.xml has been imported.