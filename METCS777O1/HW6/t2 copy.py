from __future__ import print_function
import sys

from operator import add
from string import ascii_lowercase as lowercase
import re
from re import sub, search
import numpy as np
from numpy.random.mtrand import dirichlet, multinomial
from string import punctuation
import random
from pyspark import SparkConf, SparkContext
import Task1 as t1

def count_top100_words(doc_content, top20000):
    words = doc_content.split()
    word_counts = {}
    for word in words:
        word = word.lower().strip(punctuation)
        if word in top20000[:100]:
            word_counts[word] = word_counts.get(word, 0) + 1
    return word_counts

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: task2 <trainingfile> <testingfile>", file=sys.stderr)
        exit(-1)
    sc = SparkContext(appName="A6")

    lines = sc.textFile(sys.argv[1])
    stripped = lines.map(lambda x: re.sub("<[^>]+>", "", x))

    # Count most frequent words (top 20000)
    counts = lines.map(lambda x: x[x.find(">"): x.find("</doc>")]) \
                  .flatMap(lambda x: x.split()).map(lambda x: x.lower().strip(".,<>()-[]:;?!")) \
                  .filter(lambda x: len(x) > 1) \
                  .map(lambda x: (x, 1)).reduceByKey(add)

    sortedwords = counts.takeOrdered(20000, key=lambda x: -x[1])
    top20000 = [pair[0] for pair in sortedwords]

    def countWords(d):
        try:
            header = re.search('(<[^>]+>)', d).group(1)
        except AttributeError:
            header = ''
        d = d[d.find(">"): d.find("</doc>")]
        words = d.split(' ')
        numwords = {}
        count = 0
        for w in words:
            if re.search("([A-Za-z])\w+", w) is not None:
                w = w.lower().strip(punctuation)
                if (len(w) > 2) and w in top20000:
                    count += 1
                    idx = top20000.index(w)
                    if idx in numwords:
                        numwords[idx] += 1
                    else:
                        numwords[idx] = 1
        return (header, numwords, count)

    def map_to_array(mapping):
        count_lst = [0] * 20000
        i = 0
        while i < 20000:
            if i in mapping:
                count_lst[i] = mapping[i]
            i+= 1
        return np.array(count_lst)
    
    # Calculate term frequency vector for each document
    result = lines.map(countWords)
    result.cache()

    # Function to filter and get counts of 100 most common words in the specific document

    # Extract the counts of the 100 most common words in a specific document
    def getSpecificDocCounts(header, numwords, count):
        if "20_newsgroups/comp.graphics/37261" in header:  # Adjusted the condition to match the document header
            return numwords
        return None

    specificDocCounts = result.map(lambda x: getSpecificDocCounts(*x)) \
                            .filter(lambda x: x is not None) \
                            .collect()

    if specificDocCounts:
        # Print the word counts for the 100 most common words in the specific document
        specificDocCounts = specificDocCounts[0]  # Extract the counts from the list
        for i in range(100):  # Adjusted to avoid IndexError
            word = top20000[i]
            count = specificDocCounts.get(i, 0)
            # print(f"Time {i+1},{word}")
    else:
        print("The specific document was not found in the dataset.")

    ###########################################################################################################################################
    ###########################################################################################################################################
    ########################################################### Task 3 ####################################################################

    alpha = [0.1] * 20
    beta = np.array([0.1] * 20000)

    pi = dirichlet(alpha).tolist()
    mu = np.array([dirichlet(beta) for j in range(20)])
    log_mu = np.log(mu)
    header = result.map(lambda x: x[0]).collect()
    n = result.count()
    l = result.map(lambda x: x[2]).collect()
    x = result.map(lambda x: x[1]).map(map_to_array).cache()

    def getProbs(checkParams, log_allMus, x, log_pi):
        if checkParams == True:
#            if x.shape [0] != log_allMus.shape [1]:
#                    raise Exception ('Number of words in doc does not match')
            if log_pi.shape[0] != log_allMus.shape [0]:
                    print (log_pi.shape[0])
                    raise Exception ('Number of document classes does not match')
            #if not (0.999 <= np.sum (log_pi) <= 1.001):
            #        raise Exception ('Pi is not a proper probability vector')
            for i in range(log_allMus.shape [0]):
                    if not (0.999 <= np.sum (np.exp (log_allMus[i])) <= 1.001):
                            raise Exception ('log_allMus[' + str(i) + '] is not a proper probability vector')
        allProbs = np.copy(log_pi)
        for i in range(log_allMus.shape [0]):
            product = np.multiply (x, log_allMus[i])
            allProbs[i] += np.sum(product)
        biggestProb = np.amax(allProbs)
        allProbs -= biggestProb
        allProbs = np.exp(allProbs)
        return allProbs / np.sum (allProbs)

    def addup(map1, map2):
        """
        Adds up the values of keys that appear in at least one of the mappings
        """
        map1 = dict(map1) #make a copy so the original dict isn't mutated
        for key in map2:
            if key in map1:
                map1[key] += map2[key]
            else:
                map1[key] = map2[key]
        return map1

    def assignCategory(x, i, c):
        if x[1] == i:
            return c
        else:
            return x[0]


    for num_iter in range(2): # 200 iterations
        print(num_iter)
        # update c
        logPi = np.log(pi)
        probs = x.map(lambda x_i: getProbs(False, log_mu, x_i, logPi)).collect()
        c_local = [np.nonzero(multinomial(1, prob))[0][0] for prob in probs]        
        c = x.zipWithIndex().map(lambda tup: c_local[tup[1]])
        c.cache()

        # update pi
        counts = c.map(lambda cat: (cat, 1)).reduceByKey(add)\
            .sortByKey(ascending=True).collectAsMap()

        new_alpha = [alpha[i] + counts.get(i, 0) for i in range(20)]
        pi = dirichlet(new_alpha)

        # update mu
        x_c = x.zip(c).cache()

        for j in range(20):
            word_counts = x_c.filter(lambda term: term[1] == j)\
                .map(lambda term: term[0])\
                .reduce(add)

            log_mu[j] = np.log(dirichlet(beta + word_counts))

    # Output results
    tosave = []
    for mu_j in log_mu:
        word_probs = zip(top20000, np.exp(mu_j))
        sorted_words = sorted(word_probs, key=lambda x: x[1], reverse=True)
        top_words = [word for word, _ in sorted_words[:50]]
        tosave.append(top_words)

    sc.parallelize(tosave, 1).saveAsTextFile(sys.argv[3])

    finalresult = sc.parallelize(zip(c.collect(), header)).sortByKey()
    finalresult.saveAsTextFile(sys.argv[2])

###########################################################################################################################################
###########################################################################################################################################
########################################################### Task 4 ####################################################################

    def analyze_clusters(cluster_assignments, headers):
        if len(cluster_assignments) != len(headers):
            raise ValueError("The length of cluster assignments and headers must be equal")

        # Create a dictionary to hold information about each cluster
        cluster_info = {}

        # Populate the cluster_info dictionary
        for c, h in zip(cluster_assignments, headers):
            if c not in cluster_info:
                cluster_info[c] = {'size': 0, 'categories': {}}
            cluster_info[c]['size'] += 1
            category = h.split('.')[0]  # Assuming the real category is the first part of the header

            if category not in cluster_info[c]['categories']:
                cluster_info[c]['categories'][category] = 0
            cluster_info[c]['categories'][category] += 1

        # Print information about each cluster
        for c, info in cluster_info.items():
            print(f"Cluster {c}:")
            
            # Check if the cluster is empty
            if info['size'] == 0:
                print("  This cluster is empty.")
                continue

            print(f"  Size: {info['size']}")
            sorted_categories = sorted(info['categories'].items(), key=lambda x: -x[1])
            for category, count in sorted_categories[:3]:  # Adjust the number to get more or fewer top categories
                percentage = count / info['size'] * 100
                print(f"  {category}: {percentage:.2f}%")
            print()

    # Get the cluster assignments and headers as lists or arrays
    c_values = c.collect() 
    header_values = header  

    # Call the function to analyze the clusters
    analyze_clusters(c_values, header_values)