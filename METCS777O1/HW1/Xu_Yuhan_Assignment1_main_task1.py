from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    rdd = sc.textFile(sys.argv[1])

    #Task 1
    #Your code goes here
    cleaned_rdd = rdd.map(lambda line: line.split(",")) \
                .filter(lambda p: len(p) == 17 and isfloat(p[5]) and isfloat(p[11])) \
                .filter(lambda p: float(p[4]) > 60 and float(p[5]) > 0 and float(p[11]) > 0 and float(p[16]) > 0) \
                .map(lambda p: (p[0], p[1]))
    distinct_drivers_rdd = cleaned_rdd.distinct() \
                                  .map(lambda x: (x[0], 1)) \
                                  .reduceByKey(add)
    top_10_taxis = distinct_drivers_rdd.takeOrdered(10, key=lambda x: -x[1])
    print(top_10_taxis)
    results_1 = sc.parallelize(top_10_taxis)
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])


    # #Task 2
    # #Your code goes here
    # cleanedRdd = rdd.map(lambda x: x.split(",")).filter(correctRows)

    # # Map to include only the columns of interest: driver ID, trip time in secs, total amount
    # moneyPerMinuteRdd = cleanedRdd.map(lambda x: (x[1], (float(x[16]), float(x[4]) / 60)))  # (driver, (total_amount, time_in_minutes))

    # # Calculate the money earned per minute for each trip and aggregate by driver ID
    # aggregatedRdd = moneyPerMinuteRdd.reduceByKey(lambda a, b: (a[0] + b[0], a[1] + b[1]))  # Summing up total_amount and time_in_minutes for each driver

    # # Calculate the average money earned per minute for each driver
    # avgMoneyPerMinuteRdd = aggregatedRdd.mapValues(lambda x: x[0] / x[1])  # (total_amount / time_in_minutes)

    # # Sort by average money earned per minute in descending order and take the top 10
    # top10Drivers = avgMoneyPerMinuteRdd.sortBy(lambda x: x[1], ascending=False).take(10)

    # # Save the output
    # results_2 =sc.parallelize(top10Drivers)

    # #savings output to argument
    # results_2.coalesce(1).saveAsTextFile(sys.argv[2])


    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here


    sc.stop()