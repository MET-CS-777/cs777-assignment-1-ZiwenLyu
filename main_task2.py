#!/usr/bin/python3
from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf, SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *
from datetime import datetime

spark = SparkSession.builder.master("local[*]").getOrCreate()
sc = SparkContext.getOrCreate()
sqlContext = SQLContext(sc)


# Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True

    except:
        return False


# Function - Cleaning
# For example, remove lines if they don’t have 16 values and
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles,
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if (len(p) == 17):
        if (isfloat(p[5]) and isfloat(p[11])):
            if (float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0):
                return p

# Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)

    spark_conf=SparkConf().setAppName("Assignment-1")
    sc = SparkContext.getOrCreate(conf=spark_conf)

    rdd = sys.argv[1]
    testDataFrame = sqlContext.read.format('csv').options(header='false', inferSchema='true', sep=',').load(rdd)

    testRDD = testDataFrame.rdd.map(tuple)
    taxilinesCorrected = testRDD.filter(correctRows) #Use the function to select only corrected rows

    # Task 2
    # Your code goes here
    lines_2 = taxilinesCorrected.map(lambda row: (row[1],(float(row[16]), (float(row[4]))))) #driver ID, totalAmount, tripDuration
    result = lines_2.reduceByKey(lambda a,b: (a[0]+b[0],a[1]+b[1])) #Add up each trip's totalAmount and tripDuration by key
    result = result.mapValues(lambda x: x[0]/(x[1]/60)) #totalAmount/tripDuration/60 = average amount per minute
    results_2 = result.takeOrdered(10, key=lambda x: -x[1]) #top 10
    results_2 = sc.parallelize(results_2)

    # savings output to argument
    results_2.coalesce(1).saveAsTextFile(sys.argv[2])


    sc.stop()