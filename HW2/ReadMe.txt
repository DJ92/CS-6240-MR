Weather Data:

To run the Programs for calculating Weather Data TMAX/TMIN averages:

1. Extract Homework2.gzip

Running using Intellij IDE:
1. Import as Maven Project
2. Run make alone command using the makefile provided
	- The make alone command takes "Job Name" as an argument
	- The argument can be passed as shown below:
		eg: make alone jobname=NoCombinerAggregateJobClass
	- The list of classes (job names) with the main function for creating the Job object are:
		* NoCombinerAggregateJobClass
		* CombinerAggregateJobClass
		* InMapperCombinerAggregateJobClass
		* SecondarySortAggregateJobClass
3. Run Program

Running using Command Prompt/Terminal:
1. Extract all classes from Homework2/src/main/java
2. javac <Job name>.java
	- The list of classes with the main function for creating the Job object are (Jobname):
		* NoCombinerAggregateJobClass
		* CombinerAggregateJobClass
		* InMapperCombinerAggregateJobClass
		* SecondarySortAggregateJobClass
3. java <jobname> <input path> <outputpath>

- The program shall write the output to the output path specified in the above command.

Files that are used in this homework:

-> CombinerAggregateJobClass.java
		CombinerMapperClass.java
		CombinerClass.java
		CombinerReducerClass.java
-> InMapperCombinerAggregateJobClass.java
		InMapperCombinerMapperClass.java
		InMapperCombinerReducerClass.java

-> NoCombinerAggregateJobClass.java
		NoCombinerMapperClass.java
		NoCombinerReducerClass.java

-> SecondarySortAggregateJobClass.java
		SecondarySortKeyClass.java
		SecondarySortMapperClass.java
		SecondarySortPartitionerClass.java
		SecondarySortGroupComparatorClass.java
		SecondarySortReducerClass.java
		StationInputDataWritableClass.java
		StationOutputDataWritableClass.java

*Note: Unzip the EMR Output & Logs.zip
*Note: The Outputs have been provided in NoCombinerOutput.zip, CombinerOutput.zip, InMapperCombinerOutput.zip, SecondarySortOutput.zip
*Note: The Syslogs have been provided in NoCombinerSyslog, CombinerSyslog, InMapperSyslog, SecondarySortSyslog
In case of diamond erros, please make sure the maven dependency in pom.xml has been imported.