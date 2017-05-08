PageRank:

To run the Programs for calculating PageRank of all pages:

1. Extract CS6240_Dheeraj_Joshi_HW4.zip

Running using Intellij IDE:
1. Import the Source Code folder as Maven Project
2. Run make alone command using the makefile provided
	- The make alone command takes "Job Name" as an argument
	- The argument can be passed as shown below:
		eg: make alone jobname=PageRankAggregateJob
3. Run Program

- The program takes two arguments <input> <output>

- The program shall write the output to the output path specified in the above command.

Files that are used in this homework:

-> src/main/java - Bz2WikiParser.java
-> src/main/java - PageRankAggregateJob.java

*Note: Output & Log Folders are:
	6 Machines MapReduce
	6 Machines Spark
	11 Machines MapReduce
	11 Machines Spark
 	Local MapReduce
	Local Spark
*Note: Makefile might require manual updates to following:
	- spark.root
	Configure your local spark directory here.
*Note: HW4 Report.pdf contains answers to all the questions
In case of diamond erros, please make sure the maven dependency in pom.xml has been imported.