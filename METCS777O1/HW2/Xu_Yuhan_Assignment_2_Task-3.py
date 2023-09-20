from __future__ import print_function
import sys
from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

def main():
    if len(sys.argv) != 3:
        print("Usage: wikipedia_category_analysis <wikiPages> <wikiCategoryLinks>", file=sys.stderr)
        exit(-1)

    # Initialize SparkContext
    sc = SparkContext(appName="Assignment2-Task3")

    # Initialize SparkSession
    spark = SparkSession(sc)

    # Read the input files into RDDs
    wikiPages = sc.textFile(sys.argv[1])
    wikiCategoryLinks = sc.textFile(sys.argv[2])

    # Convert the wikiCategoryLinks RDD to a DataFrame
    wikiCatsRDD = wikiCategoryLinks.map(lambda x: x.split(",")).map(lambda x: (x[0], x[1]))
    wikiCatsDF = spark.createDataFrame(wikiCatsRDD, ["docID", "category"])

    # Task 3.1: Provide summary statistics about the number of Wikipedia categories used for Wikipedia pages.

    # Group by docID and count the number of categories for each docID
    groupedWikiCats = wikiCatsDF.groupBy("docID").agg(func.count("category").alias("num_categories"))

    # Compute summary statistics
    summary_stats = groupedWikiCats.describe(["num_categories"])
    print("Task 3.1: Summary Statistics")
    summary_stats.show()

    # Calculate Max, Average, Median, and StdDev
    max_value = groupedWikiCats.agg({"num_categories": "max"}).collect()[0][0]
    avg_value = groupedWikiCats.agg({"num_categories": "avg"}).collect()[0][0]
    stddev_value = groupedWikiCats.agg({"num_categories": "stddev"}).collect()[0][0]

    # Calculating Median
    median_value = groupedWikiCats.approxQuantile("num_categories", [0.5], 0)[0]

    print(f"Max: {max_value}")
    print(f"Average: {avg_value}")
    print(f"Median: {median_value}")
    print(f"StdDev: {stddev_value}")

    # Task 3.2: Find the top 10 most used Wikipedia categories.

    # Group by category and count the number of occurrences for each category
    topCategories = wikiCatsDF.groupBy("category").agg(func.count("docID").alias("count"))

    # Sort by count in descending order and take the top 10
    top10Categories = topCategories.sort("count", ascending=False).limit(10)
    print("Task 3.2: Top 10 Most Used Wikipedia Categories")
    top10Categories.show()

    # Stop the SparkContext
    sc.stop()

if __name__ == "__main__":
    main()
