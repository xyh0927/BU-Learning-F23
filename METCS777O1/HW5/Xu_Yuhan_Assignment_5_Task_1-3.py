from __future__ import print_function

import re
import sys
import numpy as np
from pyspark import SparkContext
from pyspark.sql import SparkSession

from pyspark.ml.feature import CountVectorizer, IDF, Tokenizer, StopWordsRemover
import time

from pyspark.ml.classification import LogisticRegression
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.sql.functions import col
from pyspark.ml.classification import LinearSVC

sc = SparkContext.getOrCreate()
spark = SparkSession.builder.appName("Assignment-5").getOrCreate()

# If needed, use this helper function
# You can implement your own version if you find it more appropriate 

if __name__ == "__main__":

    ################################################################# Task 1 #################################################################

    print("=========================================================== Task 1 ===========================================================")
    print()
    print("---------------------------------- For Train ----------------------------------")
    ##TRAIN
    # Use this code to reade the data
    corpus = sc.textFile(sys.argv[1], 1)
    keyAndText = corpus.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6])).map(lambda x: (x[0], int(x[0].startswith("AU")),x[1]))   
    # Spark DataFrame to be used wiht MLlib 
    df = spark.createDataFrame(keyAndText).toDF("id","label","text").cache()

    ############################################ My code(Train) #################################################
    start_time = time.time()
    
    tokenizer = Tokenizer(inputCol="text", outputCol="words")
    wordsData = tokenizer.transform(df)
    
    remover = StopWordsRemover(inputCol="words", outputCol="filtered_words")
    wordsData = remover.transform(wordsData)
    
    cv = CountVectorizer(inputCol="filtered_words", outputCol="rawFeatures", vocabSize=5000)
    cv_model = cv.fit(wordsData)
    featurizedData = cv_model.transform(wordsData)
    
    idf = IDF(inputCol="rawFeatures", outputCol="features")
    idfModel = idf.fit(featurizedData)
    rescaledData = idfModel.transform(featurizedData)
    
    rescaledData.cache()
    
    # Subtasks
    vocab = cv_model.vocabulary
    print("First 10 words in the vocabulary : ", vocab[:10])
    
    end_time = time.time()
    print("Total time to vectorize the data: {:.2f} seconds".format(end_time - start_time))

    print("-----------------------------------------------------------------------------------")
    print()
    ################################################################################################################
    

    print("---------------------------------- For Test ----------------------------------")
    ##TEST
    #Use this code to read the data
    corpust = sc.textFile(sys.argv[2], 1)
    keyAndTextt = corpust.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6])).map(lambda x: (x[0], int(x[0].startswith("AU")),x[1]))   
    # Spark DataFrame to be used wiht MLlib 
    test = spark.createDataFrame(keyAndTextt).toDF("id","label","text").cache()

    ############################################ My code(Test) #####################################################
    start_time_test = time.time()
    
    wordsData_test = tokenizer.transform(test)
    wordsData_test = remover.transform(wordsData_test)
    featurizedData_test = cv_model.transform(wordsData_test)  # Use the same CountVectorizerModel fitted on TRAIN data
    rescaledData_test = idfModel.transform(featurizedData_test)  # Use the same IDFModel fitted on TRAIN data
    
    rescaledData_test.cache()
    print("The vocabulary in TRAIN is reused in TEST, so the First 10 words in the vocabulary : ", vocab[:10])

    end_time_test = time.time()
    print("Total time to vectorize the TEST data: {:.2f} seconds".format(end_time_test - start_time_test))
    print("-----------------------------------------------------------------------------------")

    print("==============================================================================================================================")
    print()
    ###################################################################################################################

    ############################################################################################################################################


    ### Task 2
    print("=========================================================== Task 2 ===========================================================")
    print()
    ### Build your learning model using Logist
    # 1. Set the max number of iterations to 20
    lr = LogisticRegression(maxIter=20)

    # 2. Train the model on the train data
    start_train_time = time.time()
    lr_model = lr.fit(rescaledData)
    end_train_time = time.time()

    # 3. Evaluate the model on the test dataset and print the F1 measure and the confusion matrix.
    start_eval_time = time.time()
    predictions = lr_model.transform(rescaledData_test)

    # Ensure that the prediction and label columns are in the correct format
    predictions = predictions.withColumn("prediction", predictions["prediction"].cast("double"))
    predictions = predictions.withColumn("label", predictions["label"].cast("double"))
    end_eval_time = time.time()

    start_test_time = time.time()
    # Calculate TP, TN, FP, FN
    TP = predictions.filter((col('prediction') == 1) & (col('label') == 1)).count()
    TN = predictions.filter((col('prediction') == 0) & (col('label') == 0)).count()
    FP = predictions.filter((col('prediction') == 1) & (col('label') == 0)).count()
    FN = predictions.filter((col('prediction') == 0) & (col('label') == 1)).count()

    accuracy = (TP + TN)/(TP + TN + FP + FN)
    precision = TP / (TP + FP)
    recall = TP / (TP + FN)
    f1 = 2 * (precision * recall) / (precision + recall)

    # Display the values
    print("True Positives:", TP)
    print("True Negatives:", TN)
    print("False Positives:", FP)
    print("False Negatives:", FN)
    print("accuracy: ", accuracy * 100, "%")

    end_test_time = time.time()

    # Print the performance metrics
    print("\nPerformance Metrics: Logistic Regression")
    print("Precision:", precision)
    print("Recall:", recall)
    print("F1:", f1)
    print("Confusion Matrix:\n TP:", TP,  "FN: ", FN, "\n FP: ", FP, "TN: ", TN)
    print('The total time needed to train the model: {:.2f} secs\nEvaluate the model: {:.2f} secs\nTest the model: {:.2f}\nTotal Time: {:.2f} secs'.format(end_train_time - start_train_time, end_eval_time - start_eval_time, end_test_time - start_test_time,(end_train_time - start_train_time) + (end_test_time - start_test_time) +(end_eval_time - start_eval_time)))

    print("==============================================================================================================================")
    print()

    ### Task 3
    print("=========================================================== Task 3 ===========================================================")
    print()
    ### Build your learning model using SVM
  
    # 1. Set the max number of iterations to 20
    lsvc = LinearSVC(maxIter=20)

    # 2. Train the model on the train data
    start_train_time_svm = time.time()
    lsvc_model = lsvc.fit(rescaledData)
    end_train_time_svm = time.time()

    # 3. Evaluate the model on the test dataset and print the F1 measure and the confusion matrix.
    start_eval_time_svm = time.time()
    predictions_svm = lsvc_model.transform(rescaledData_test)

    # Ensure that the prediction and label columns are in the correct format
    predictions_svm = predictions_svm.withColumn("prediction", predictions_svm["prediction"].cast("double"))
    predictions_svm = predictions_svm.withColumn("label", predictions_svm["label"].cast("double"))
    end_eval_time_svm = time.time()

    start_test_time_svm = time.time()
    # Calculate TP, TN, FP, FN
    TP_svm = predictions_svm.filter((col('prediction') == 1) & (col('label') == 1)).count()
    TN_svm = predictions_svm.filter((col('prediction') == 0) & (col('label') == 0)).count()
    FP_svm = predictions_svm.filter((col('prediction') == 1) & (col('label') == 0)).count()
    FN_svm = predictions_svm.filter((col('prediction') == 0) & (col('label') == 1)).count()

    accuracy_svm = (TP_svm + TN_svm)/(TP_svm + TN_svm + FP_svm + FN_svm)
    precision_svm = TP_svm / (TP_svm + FP_svm)
    recall_svm = TP_svm / (TP_svm + FN_svm)
    f1_svm = 2 * (precision_svm * recall_svm) / (precision_svm + recall_svm)

    # Display the values
    print("True Positives:", TP_svm)
    print("True Negatives:", TN_svm)
    print("False Positives:", FP_svm)
    print("False Negatives:", FN_svm)
    print("accuracy: ", accuracy_svm * 100, "%")

    end_test_time_svm = time.time()

    # 4. Print out the total time needed to train the model, evaluate the model using the test dataset, and calculate the performance metrics.
    print("\nPerformance Metrics: SVM")
    print("Precision:", precision_svm)
    print("Recall:", recall_svm)
    print("F1:", f1_svm)
    print("Confusion Matrix:\n TP:", TP_svm,  "FN: ", FN_svm, "\n FP: ", FP_svm, "TN: ", TN_svm)
    print('The total time needed to train the model: {:.2f} secs\nEvaluate the model: {:.2f} secs\nTest the model: {:.2f} secs\nTotal Time: {:.2f} secs'.format(end_train_time_svm - start_train_time_svm, end_eval_time_svm - start_eval_time_svm, end_test_time_svm - start_test_time_svm,(end_train_time_svm - start_train_time_svm) + (end_eval_time_svm - start_eval_time_svm) + end_test_time_svm - start_test_time_svm))

    print("==============================================================================================================================")
    print()

    
    sc.stop()
    
