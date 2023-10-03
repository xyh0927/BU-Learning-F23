from __future__ import print_function

import re
import sys
import numpy as np
from operator import add

from pyspark import SparkContext
numTopWords = 10000
def freqArray(broadcastedDictionary, listOfIndices):
		global numTopWords
		returnVal = np.zeros(numTopWords)
		for word in listOfIndices:
			index = broadcastedDictionary.get(word)
			if index is not None:
				returnVal[index] += 1
		mysum = np.sum(returnVal)
		returnVal = np.divide(returnVal, mysum)
		return returnVal


if __name__ == "__main__":

	sc = SparkContext(appName="Assignment-4")

	### Task 1
	### Data Preparation
	corpus = sc.textFile(sys.argv[1])
	keyAndText = corpus.map(lambda x : (x[x.index('id="') + 4 : x.index('" url=')], x[x.index('">') + 2:][:-6]))
	regex = re.compile('[^a-zA-Z]')

	keyAndListOfWords = keyAndText.map(lambda x : (str(x[0]), regex.sub(' ', x[1]).lower().split()))


	########################################################## My Code for Task 1 ##########################################################
	print("======================================== Task 1 ========================================")
	allWords = keyAndListOfWords.flatMap(lambda x: x[1]).map(lambda x: (x, 1))
	wordCounts = allWords.reduceByKey(add)
    
    # Sorting by frequency and then creating a dictionary RDD
	topWords = wordCounts.sortBy(lambda x: x[1], ascending=False).take(numTopWords)
	dictionary = sc.parallelize(topWords).zipWithIndex().map(lambda x: (x[0][0], x[1]))

	########################################################################################################################################

	### Include the following results in your report:
	print("Index for 'applicant' is",dictionary.filter(lambda x: x[0]=='applicant').take(1)[0][1])
	print("Index for 'and' is",dictionary.filter(lambda x: x[0]=='and').take(1)[0][1])
	print("Index for 'attack' is",dictionary.filter(lambda x: x[0]=='attack').take(1)[0][1])
	print("Index for 'protein' is",dictionary.filter(lambda x: x[0]=='protein').take(1)[0][1])
	print("Index for 'car' is",dictionary.filter(lambda x: x[0]=='car').take(1)[0][1])
	print("Index for 'in' is",dictionary.filter(lambda x: x[0]=='in').take(1)[0][1])

	print("========================================================================================")
	print()
	
	### Task 2
	print("======================================== Task 2 ========================================")
	
	dictionaryBroadcast = sc.broadcast(dictionary.collectAsMap())
	allDocsAsNumpyArrays = keyAndListOfWords.map(lambda x: (x[0], freqArray(dictionaryBroadcast.value, x[1])))
	allDocsAsNumpyArrays.cache()
    
	def gradient(matrix, label, beta):
		Y = label
		X = matrix
		return -(Y - (1 / (1 + np.exp(-np.dot(X, beta))))) * X
    
	def loss(matrix, label, beta):
		Y = label
		X = matrix
		return np.log(1 + np.exp(-Y * np.dot(X, beta)))
    
	beta = np.zeros(numTopWords)

################################ Can change the value here to adjust the model ################################

	learningRate = 0.01
	regularizationParam = 12

###############################################################################################################

	# Run the gradient descent algorithm for 50 iterations
	numIterations = 50
	for i in range(numIterations):
		try:
			gradients = allDocsAsNumpyArrays.map(lambda x: gradient(x[1], int(x[0].startswith('AU')), beta))
			gradients.foreach(print)  # Print each gradient to debug
			gradientSum = gradients.reduce(add)
			lossSum = allDocsAsNumpyArrays.map(lambda x: loss(x[1], int(x[0].startswith('AU')), beta)).reduce(add)
        
			# Update the weights (beta) using the gradient
			beta -= learningRate * (gradientSum + regularizationParam * beta)
		
			print("Iteration", i, "Loss:", lossSum)
		except Exception as e:
			print("Error in iteration", i, ":", e)

    
	topIndices = np.argsort(beta)[-5:]
	invDictionary = dictionary.filter(lambda x: x[1] in topIndices).map(lambda x: x[0]).collect()

	print("Five words most strongly related to an Australian court case:", invDictionary)
	# for index in topIndices:
	# 	print(invDictionary[index])
    
	print("========================================================================================")
	print()


	### Task 3
	print("======================================== Task 3 ========================================")

	# Function to predict the label of a document
	def predictLabel(matrix, beta):
		prob = 1 / (1 + np.exp(-np.dot(matrix, beta)))
		return 1 if prob > 0.5 else 0

	# Predict the labels for all documents
	predictionsAndLabels = allDocsAsNumpyArrays.map(lambda x: (x[0], predictLabel(x[1], beta)))

	# Compute the confusion matrix
	truePositives = predictionsAndLabels.filter(lambda x: x[1] == 1 and x[0].startswith('AU')).count()
	falsePositives = predictionsAndLabels.filter(lambda x: x[1] == 1 and not x[0].startswith('AU')).count()
	trueNegatives = predictionsAndLabels.filter(lambda x: x[1] == 0 and not x[0].startswith('AU')).count()
	falseNegatives = predictionsAndLabels.filter(lambda x: x[1] == 0 and x[0].startswith('AU')).count()

	# Compute precision, recall, and F1 score
	precision = truePositives / (truePositives + falsePositives) if truePositives + falsePositives > 0 else 0
	recall = truePositives / (truePositives + falseNegatives) if truePositives + falseNegatives > 0 else 0
	f1Score = 2 * (precision * recall) / (precision + recall) if precision + recall > 0 else 0

	# print("Precision:", precision)
	# print("Recall:", recall)
	# print("F1 Score:", f1Score)
	print("\nPerformance Metrics: Logisitic Regression with Gradient Descent")
	print("Precision:", precision)
	print("Recall:", recall)
	print("F1:", f1Score)
	print("Confusion Matrix: \n", "TP: ",truePositives,",","FP: ",falsePositives,"\n", "TN: ",trueNegatives,",", "FN: ",falseNegatives)
	print("Accuracy: ",(truePositives + trueNegatives)/(truePositives + falsePositives + trueNegatives + falseNegatives))
	print("========================================================================================")
	print()


	

	sc.stop()