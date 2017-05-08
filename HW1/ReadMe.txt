Weather Data:

To run the Programs for calculating Weather Data TMAX averages:
1. Extract WeatherData.gzip

Running using Intellij IDE:
1. Import as Maven Project
2. Specify the filename/path ("1912.csv.gz") as a runtime argument in the runtime configuration
3. Run Program

Running using Command Prompt/Terminal:
1. Extract all classes from Part1/src/main/java
2. javac Main.java
3. java main <path to the file name(.gz)>

- The program shall print the total time taken by all 5 versions for each run with the fibonacci delay commented out.
- To execute with fibonacci, "new Fibonacci(17);" needs to be uncommented in files: 
	- SequentialTMAXClass.java 
	- NoLockTMAXAvgThreadClass.java
	- CoarseLockedTMAXAvgThreadClass.java
	- FineLockedTMAXAvgThreadClass.java
	- NoSharingTMAXAvgThreadClass.java
- To get the output of each of the versions (Station ID, TMAX Average), the "Print Comment block" in below files needs to be uncommented:
	- SequentialTMAXClass.java 
	- NoLockTMAXClass.java
	- CoarseLockTMAXClass.java
	- FineLockTMAXClass.java
	- NoSharingTMAXClass.java

Files that are used in this:
- Main.java
- LoadFile.java
- StationTMAXData.java
- Fibonacci.java
- SequentialTMAXClass.java
- NoLockTMAXClass.java
- CoarseLockTMAXClass.java
- FineLockTMAXClass.java
- NoSharingTMAXClass.java 
- NoLockTMAXAvgThreadClass.java
- CoarseLockedTMAXAvgThreadClass.java
- FineLockedTMAXAvgThreadClass.java
- NoSharingTMAXAvgThreadClass.java

*Note: The Output has been attached in WeatherOutput.xls
In case of diamond erros, please make sure the maven dependency in pom.xml has been imported.