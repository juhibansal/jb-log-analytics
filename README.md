# jb-log-analytics

Problem Statement - Purpose of the challenge was to create an aggregate of users visiting EDGAR system based on  the amount of time that elapses between document requests, number of documents etc

Running the code - ./run.sh

Details of Solution: Code is implemented in Spark using Python (version 2.7) as a Platform. Code is named sessionization.py as required by the challenge a run file (run.sh) is provided to run the scripts.

Spark is run using the spark-shell path "/mnt/alpha/3p/spark/bin/spark-submit" which is local the machine needs to be modified based on users environment

Code takes 2 input files, with user log information and elapsed time and output is saved in output folder.

Main features of solution

- Used distributed systems with RDDs and Dataframes to develop highly scalable solutions.

- Ran a test on 50000K lines of user information with a run-time of less than 1mins. Due to the restriction of Git I could not upload larger sample size, I have tested the code on GBs of data from EdGAR Location, typically it takes around 10-15mins in processing.

- I have spent around 3 hours building the solution
