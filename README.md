# jb-log-analytics

Purpose of the challenge was to create an aggregate of users visiting EDGAR system based on  the amount of time that elapses between document requests, number of documents etc

Details of Solution: Code is implemented based in Spark using Python (version 3.0) as a Platform. Code is named sessionization.py and as required by the challenge a run file (run.sh) is provided to run the scripts.

Spark is run using the spark-shell path "/mnt/alpha/3p/spark/bin/spark-submit" which is local the machine needs to be modified based on users environment

Code take the input files, with user log information and elapsed time and output is saved in output folder.

Main features of solution

- Used distributed systems with RDDs and Dataframes to develop highly scalable solutions.

- Ran a test on 2.66M lines of user information with a run-time of 5mins

- I have spent around 3 hours building the solution
