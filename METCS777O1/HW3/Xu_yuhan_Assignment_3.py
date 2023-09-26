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

    # #Task 1
    # #Your code goes here
    start_time = time.time()
    # Filter the data
    cleaned_rdd = rdd.map(lambda line: line.split(',')).filter(correctRows).cache()
    
    # Extract relevant data: trip distance and fare amount
    distance_fare_rdd = cleaned_rdd.map(lambda p: (float(p[5]), float(p[11]))).cache()
    
    # Task 1
    # Filter the rows using the correctRows function
    cleaned_rdd = rdd.map(lambda line: line.split(',')).filter(correctRows)

    # Extract the required columns: trip distance and fare amount
    # Convert them to float and cache the RDD as it will be used multiple times
    data_rdd = cleaned_rdd.map(lambda p: (float(p[5]), float(p[11]))).cache()

    # Calculate the required sums and counts needed for the equations
    n = data_rdd.count()
    sum_x, sum_y, sum_xy, sum_x2 = data_rdd.map(lambda p: (p[0], p[1], p[0] * p[1], p[0] ** 2)).reduce(
        lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1], p1[2] + p2[2], p1[3] + p2[3])
    )

    # Calculate the slope (m) and y-intercept (b) using the equations
    m = (n * sum_xy - sum_x * sum_y) / (n * sum_x2 - sum_x ** 2)
    b = (sum_y - m * sum_x) / n

    # Create an RDD with the results and save it to the output file
    
    end_time = time.time()

    t = end_time - start_time

    print("Task 1")
    print("(m: ", m ,", b: ", b , ")")
    print("Execution Time for Task 1: %s seconds" % (t))

    results_1 = sc.parallelize([(m, b, t)])

    
    # Results_1 should have m and b parameters from the calculations
    results_1.coalesce(1).saveAsTextFile(sys.argv[2])

    


    # Task 2
    # Your code goes here
    # Parse the input data and filter out incorrect rows
    cleaned_rdd = rdd.map(lambda line: line.split(',')).filter(correctRows).map(lambda p: (float(p[4]), float(p[5])))  # Extract trip duration and trip distance

    # Cache the RDD
    cleaned_rdd.cache()

    # Initialize parameters
    m_t2 = 0  # Slope
    b_t2 = 0  # Intercept
    learning_rate = 0.0001
    num_iterations = 50

    # Gradient Descent
    for i in range(num_iterations):
        try:
            # Compute the gradients
            gradients = cleaned_rdd.map(lambda p: (p[0], m_t2 * p[0] + b_t2 - p[1])).map(lambda p: (p[0] * p[1], p[1])).reduce(lambda p1, p2: (p1[0] + p2[0], p1[1] + p2[1]))
    
            # Update parameters
            m_t2 -= learning_rate * gradients[0]
            b_t2 -= learning_rate * gradients[1]
    
            # Compute the cost
            cost = cleaned_rdd.map(lambda p: (m_t2 * p[0] + b_t2 - p[1]) ** 2).mean()
    
            # Print the cost, intercept, and the slope for each iteration
            print(f"Iteration {i+1}: Cost={cost}, m={m_t2}, b={b_t2}")
        except Exception as e:
            break

    # # Save the results
    results_2 = sc.parallelize([(m_t2, b_t2)])

    # Results_2 should have m and b parameters from the gradient Descent Calculations
    results_2.coalesce(1).saveAsTextFile(sys.argv[3])




    #Task 3 
    #Your code goes here
    # Filtering the RDD
    corrected_rdd = rdd.map(lambda line: line.split(",")).filter(correctRows).cache()

    # Initializing parameters
    b1 = 0  # intercept
    m1 = m2 = m3 = m4 = 0  # slopes
    learning_rate = 0.0000001
    num_iterations = 50

    # Gradient Descent
    for i in range(num_iterations):
        try:
            gradient_sum = corrected_rdd.map(lambda p: (1,  # for counting
                float(p[16]) - (b1 + m1 * float(p[4]) + m2 * float(p[5]) + m3 * float(p[11]) + m4 * float(p[15])),  # error
                float(p[4]),  # trip_time_in_secs
                float(p[5]),  # trip_distance
                float(p[11]),  # fare_amount
                float(p[15])  # tolls_amount
            )).reduce(lambda x, y: (
                x[0] + y[0],
                x[1] + y[1],
                x[2] + y[2],
                x[3] + y[3],
                x[4] + y[4],
                x[5] + y[5]
            ))
    
            count = gradient_sum[0]
            error_sum = gradient_sum[1]
            cost = error_sum / (2 * count)
            print(f"Iteration {i}, Cost: {cost}, b: {b1}, m1: {m1}, m2: {m2}, m3: {m3}, m4: {m4}, learning_rate: {learning_rate}")
    
            # Update parameters
            b1 = b1 + learning_rate * error_sum / count
            m1 = m1 + learning_rate * gradient_sum[2] * error_sum / count
            m2 = m2 + learning_rate * gradient_sum[3] * error_sum / count
            m3 = m3 + learning_rate * gradient_sum[4] * error_sum / count
            m4 = m4 + learning_rate * gradient_sum[5] * error_sum / count
    
            # Implement Bold Driver technique to adjust learning rate
            if i > 0 and prev_cost < cost:
                learning_rate = learning_rate * 0.5  # Decrease learning rate if cost increased
            else:
                learning_rate = learning_rate * 1.05  # Increase learning rate if cost decreased
    
            prev_cost = cost
            
        except Exception as e:
            break
        

    # Save the final parameters
    results_3 = sc.parallelize([(b1, m1, m2, m3, m4)])

    # print the cost, intercept, the slopes (m1,m2,m3,m4), and learning rate for each iteration

    # Results_3 should have b, m1, m2, m3, and m4 parameters from the gradient Descent Calculations
    results_3.coalesce(1).saveAsTextFile(sys.argv[4])
    

    sc.stop()

    