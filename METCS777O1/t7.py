from pyspark import SparkContext
from pyspark.sql import SparkSession
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.classification import LogisticRegression,LinearSVC
sc = SparkContext("local")
spark = SparkSession.builder.getOrCreate()

path="file:///content/"
wtrain = path+"wine-train.csv"
wtest=path+"wine-test.csv"
train = spark.read.csv(wtrain,header=True,inferSchema=True )
test = spark.read.csv(wtest,header=True,inferSchema=True )

trainRDD = train.rdd.map(tuple)
testRDD = test.rdd.map(tuple)
trainRDD.take(5)

from pyspark.ml.feature import VectorAssembler
feature_columns = ['fixed acidity', 'volatile acidity', 'citric acid', 'residual sugar','chlorides', 'free sulfur dioxide',
 'total sulfur dioxide','density','pH','sulphates','alcohol', 'red']
assembler = VectorAssembler(inputCols=feature_columns, outputCol='features')
train = assembler.transform(train).select("features","label")
test = assembler.transform(test).select("features","label")
train.show()
class_evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")
LogisticRegression().setLabelCol("label")
model = LogisticRegression().fit(train)
predictions = model.transform(test)

accuracy = class_evaluator.setMetricName("accuracy").setLabelCol("label").evaluate(predictions)
f1 = class_evaluator.setMetricName("f1").setLabelCol("label").evaluate(predictions)
        
    
precision = class_evaluator.setMetricName("Precision").setLabelCol("label").evaluate(predictions)
recall = class_evaluator.setMetricName("Recall").setLabelCol("label").evaluate(predictions)

LinearSVC().setLabelCol("label")
model = LogisticRegression().fit(train)
predictions = model.transform(test)

accuracy = class_evaluator.setMetricName("accuracy").setLabelCol("label").evaluate(predictions)
f1 = class_evaluator.setMetricName("f1").setLabelCol("label").evaluate(predictions)
        
    
precision = class_evaluator.setMetricName("Precision").setLabelCol("label").evaluate(predictions)
recall = class_evaluator.setMetricName("Recall").setLabelCol("label").evaluate(predictions)


test.show()