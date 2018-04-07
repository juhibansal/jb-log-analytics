#
# Licensed to the Apache Software Foundation (ASF) under one or more

from __future__ import print_function

import sys
from operator import add
import datetime
from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import udf,col,desc,concat,lit,lag,lead,when
import re
import os
from os.path import isfile,isdir
from pprint import pformat
from sys import argv, exit
from glob import glob
from dateutil import parser

import numpy as np
import pandas as pd
import itertools
import collections
from collections import OrderedDict
import gzip
import os
import subprocess
import pydoop.hdfs as hdfs
from pyspark.sql.window import Window

if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: logfile compilation <file>", file=sys.stderr)
        exit(-1)

    def readinactivity_time(inactivityfile):
    	fp = open(inactivityfile, 'r+')
        for line in fp:
            line = line.strip() 
            if not line: continue
            if line.startswith("#"):continue
            offset = int(line)
        return offset

    logfilepath=os.getcwd()+"/input/"+sys.argv[1]
    inactivityfilepath=os.getcwd()+"/input/"+sys.argv[2]
    outputfilepath=os.getcwd()+"/output/"+sys.argv[3]
    
    print ("DBG args:", logfilepath, inactivityfilepath,  file=sys.stderr)

    inactivity_time_seconds = readinactivity_time(inactivityfilepath)
 
    spark = SparkSession\
        .builder\
        .master("local")\
        .appName("PythonDeviceMap_Juhi")\
        .config("spark.some.config.option", "some-value")\
        .getOrCreate()
    sqlContext = SQLContext(spark)
   
    df = sqlContext.read.format('com.databricks.spark.csv').options(header='true').load("file:%s"%logfilepath)
    hist_columns = ("ip","date","time","zone","cik","accession","extention")
    df_sl = df.select(*hist_columns)
    df_mergedtimestamp = df_sl.withColumn("start_datetime", concat(col('date'),lit(' '),col('time'))).drop("date","time","zone").drop_duplicates()

    w_1 = Window.partitionBy("ip").orderBy("start_datetime")
    df_mergedtimestamp = df_mergedtimestamp.withColumn("lag_starttime",func.lag(func.unix_timestamp(df_mergedtimestamp["start_datetime"], format="yyyy-MM-dd HH:mm:ss")).over(w_1)-func.unix_timestamp(df_mergedtimestamp["start_datetime"], format="yyyy-MM-dd HH:mm:ss")+inactivity_time_seconds)
    df_mergedtimestamp=df_mergedtimestamp.withColumn("merge_ip", when(df_mergedtimestamp["lag_starttime"]>=0,0).otherwise(1))
    df_mergedtimestamp = df_mergedtimestamp.withColumn("merged_sum",func.sum('merge_ip').over(w_1))
    w_2 = Window.partitionBy("ip","merged_sum")
    df_mergedtimestamp = df_mergedtimestamp.withColumn("merged_count",func.count('merged_sum').over(w_2))

    df_mergedtimestamp = df_mergedtimestamp.withColumn("first_starttime", func.min(df_mergedtimestamp["start_datetime"]).over(w_2))
    df_mergedtimestamp = df_mergedtimestamp.withColumn("final_starttime", func.max(df_mergedtimestamp["start_datetime"]).over(w_2))
    df_mergedtimestamp = df_mergedtimestamp.withColumn("duration",func.unix_timestamp(df_mergedtimestamp["final_starttime"], format="yyyy-MM-dd HH:mm:ss")-func.unix_timestamp(df_mergedtimestamp["first_starttime"], format="yyyy-MM-dd HH:mm:ss")+1)
    df_mergedtimestamp = df_mergedtimestamp.withColumn("temp", func.unix_timestamp(df_mergedtimestamp["start_datetime"],format="yyyy-MM-dd HH:mm:ss"))
    row1 = df_mergedtimestamp.agg({"temp": "max"}).collect()[0][0]
    print(row1)
    print(df_mergedtimestamp.show(14))
    print(inactivity_time_seconds)

    df_stage1 = df_mergedtimestamp.select("ip","first_starttime","final_starttime","duration","merged_count").drop_duplicates()
    df_stage1=df_stage1.withColumn("orderinginfo", when(func.unix_timestamp(df_stage1["final_starttime"],format="yyyy-MM-dd HH:mm:ss")+inactivity_time_seconds-row1>=0,1).otherwise(0))
    df_stage1=df_stage1.withColumn("dateforordering", when(df_stage1["orderinginfo"]==1,df_stage1["first_starttime"]).otherwise(df_stage1["final_starttime"]))
    df_stage1=df_stage1.orderBy("orderinginfo", "dateforordering")
    print(df_stage1.show(14))
    stage_rdd = df_stage1.rdd
    f = open(outputfilepath,"w+")
    for row in stage_rdd.collect():
       d = row.asDict()
       s = "%s,%s,%s,%s,%s\n" % (d["ip"], d["first_starttime"], d["final_starttime"],d["duration"],d["merged_count"])
       f.write(s)
