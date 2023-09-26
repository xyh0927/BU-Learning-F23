from __future__ import print_function

import os
import time
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
# checking if the trip duration is more than a minute, trip distance is more than 0.1 miles, 
# fare amount and total amount are more than 0.1 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 5:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-3")
    
    rdd = sc.textFile(sys.argv[1])

    #Task 1
    #Your code goes here
    cleaned_rdd = rdd.map(lambda line: line.split(',')).filter(correctRows).cache()

    # Increase the number of partitions
    cleaned_rdd = cleaned_rdd.repartition(10)

    # Task 1
    start_time = time.time()

    # Initialize accumulators
    sum_x = sc.accumulator(0.0)
    sum_y = sc.accumulator(0.0)
    sum_xy = sc.accumulator(0.0)
    sum_x2 = sc.accumulator(0.0)

    def accumulate_sums(p):
        global sum_x, sum_y, sum_xy, sum_x2
        x, y = float(p[5]), float(p[11])
        sum_x += x
        sum_y += y
        sum_xy += x * y
        sum_x2 += x ** 2

    cleaned_rdd.foreach(accumulate_sums)

    n = cleaned_rdd.count()
    m = (n * sum_xy.value - sum_x.value * sum_y.value) / (n * sum_x2.value - sum_x.value ** 2)
    b = (sum_y.value - m * sum_x.value) / n

    end_time = time.time()
    t = end_time - start_time

    print("Task 1")
    print("(m: ", m, ", b: ", b, ")")
    print("Execution Time for Task 1: %s seconds" % (t))

    results_1 = sc.parallelize([(m, b, t)])
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

    


    #Task 2
    #Your code goes here


    # print the cost, intercept, and the slope for each iteration

    # Results_2 should have m and b parameters from the gradient Descent Calculations
    #results_2.coalesce(1).saveAsTextFile(sys.argv[3])




    #Task 3 
    #Your code goes here


    # print the cost, intercept, the slopes (m1,m2,m3,m4), and learning rate for each iteration

    # Results_3 should have b, m1, m2, m3, and m4 parameters from the gradient Descent Calculations
    #results_3.coalesce(1).saveAsTextFile(sys.argv[4])
    

    sc.stop()

    