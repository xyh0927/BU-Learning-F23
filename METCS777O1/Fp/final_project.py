from pyspark.sql import SparkSession
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.evaluation import MulticlassClassificationEvaluator, BinaryClassificationEvaluator, RegressionEvaluator
from pyspark.ml.classification import LogisticRegression, DecisionTreeClassifier, RandomForestClassifier
from pyspark.ml.regression import LinearRegression
from pyspark.sql.functions import when, col, lit, cast
import sys
import matplotlib.pyplot as plt
import seaborn as sns
from sklearn.metrics import confusion_matrix
from pyspark.sql.functions import udf
from pyspark.sql.types import DoubleType
from sklearn.metrics import roc_curve, auc as sklearn_auc 
from sklearn.metrics import precision_recall_curve, average_precision_score

# Create Spark session
spark = SparkSession.builder.appName("Final_Project_Airline_Analysis").getOrCreate()

# Sample data path
data_path = sys.argv[1]

# Read data
try:
    df = spark.read.csv(data_path, header=True, inferSchema=True)
except Exception as e:
    print(f"Error reading data: {str(e)}")
    sys.exit(1)

# Create binary labels
df = df.withColumn("label_85_99", when(col("fatalities_per_fatal_accidents_85_99") > 0, 1).otherwise(0))
df = df.withColumn("label_00_14", when(col("fatalities_per_fatal_accidents_00_14") > 0, 1).otherwise(0))

# Define feature columns
feature_columns = ["avail_seat_km_per_week", "incidents_85_99", "fatal_accidents_85_99", "fatalities_85_99", "incidents_00_14", "fatal_accidents_00_14", "fatalities_00_14"]

# Assemble feature columns into a feature vector
assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")
data = assembler.transform(df)

# Split data into training and testing sets
train_data, test_data = data.randomSplit([0.8, 0.2], seed=123)

# Visualization 1: Bar chart of incident counts per airline
airline_incidents = df.groupBy("airline").agg({"incidents_85_99": "sum", "incidents_00_14": "sum"}).toPandas()
airline_incidents.plot(x="airline", kind="bar", figsize=(10, 6), title="Incident Counts per Airline (85_99 and 00_14)")
plt.ylabel("Incident Counts")
plt.xlabel("Airline")
plt.tight_layout()
plt.savefig("incident_counts_per_airline.png")
plt.show()

# Visualization 2: Line chart of fatalities over the years
fatalities_years = df.groupBy("airline").agg({"fatalities_85_99": "sum", "fatalities_00_14": "sum"}).toPandas()
fatalities_years.plot(x="airline", kind="line", marker="o", figsize=(10, 6), title="Fatalities Over the Years (85_99 and 00_14)")
plt.ylabel("Fatalities")
plt.xlabel("Airline")
plt.tight_layout()
plt.savefig("fatalities_over_years.png")
plt.show()

# Classification Models
classifiers = [
    LogisticRegression(),
    DecisionTreeClassifier(),
    RandomForestClassifier()
]

# Classification Metrics
class_evaluator = MulticlassClassificationEvaluator(predictionCol="prediction")

# Binary Classification Evaluator
bin_class_evaluator = BinaryClassificationEvaluator(rawPredictionCol="rawPrediction")

# Regression Model
lr = LinearRegression(featuresCol='features')

# Regression Metrics
reg_evaluator = RegressionEvaluator(predictionCol="prediction")

def extract_prob(probability):
    return float(probability[1])
    
extract_prob_udf = udf(extract_prob, DoubleType())

# Loop through periods
for period_label, incident_label in zip(["label_85_99", "label_00_14"], ["incidents_85_99", "incidents_00_14"]):
    print(f"\nPeriod: {period_label[-5:]}")
    
    # Supervised Learning
    for classifier in classifiers:
        classifier.setLabelCol(period_label)
        model = classifier.fit(train_data)
        predictions = model.transform(test_data)
        
        # Metrics
        accuracy = class_evaluator.setMetricName("accuracy").setLabelCol(period_label).evaluate(predictions)
        f1 = class_evaluator.setMetricName("f1").setLabelCol(period_label).evaluate(predictions)
        
        # Additional Metrics: Precision, Recall, AUC-ROC
        precision = class_evaluator.setMetricName("weightedPrecision").setLabelCol(period_label).evaluate(predictions)
        recall = class_evaluator.setMetricName("weightedRecall").setLabelCol(period_label).evaluate(predictions)
        auc = bin_class_evaluator.setLabelCol(period_label).evaluate(predictions)
        
        # Confusion Matrix
        y_true = predictions.select(period_label).toPandas()
        y_pred = predictions.select("prediction").toPandas()
        cm = confusion_matrix(y_true, y_pred)
        tn, fp, fn, tp = cm.ravel()
        
        # Output Metrics
        print(f"\nModel: {classifier.__class__.__name__}")
        print(f"Accuracy: {accuracy}\nF1 Score: {f1}\nPrecision: {precision}\nRecall: {recall}\nAUC: {auc}")
        print(f"TP: {tp}, TN: {tn}, FP: {fp}, FN: {fn}")
        
        # Plot Confusion Matrix
        plt.figure(figsize=(5,5))
        sns.heatmap(cm, annot=True, fmt=".0f", linewidths=.5, square=True, cmap='Blues')
        plt.ylabel('Actual label')
        plt.xlabel('Predicted label')
        plt.title(f'Confusion Matrix - {classifier.__class__.__name__} ({period_label[-5:]})', size=15)
        
        # Save Plot
        plt.savefig(f"confusion_matrix_{classifier.__class__.__name__}_{period_label[-5:]}.png")
        
        # ROC Curve 
        if isinstance(classifier, LogisticRegression):
            # Extract the probability of true class using the UDF
            predictions = predictions.withColumn("probability_of_true", extract_prob_udf("probability"))
    
            # Evaluate the model with BinaryClassificationEvaluator
            evaluator = BinaryClassificationEvaluator(labelCol=period_label, rawPredictionCol="probability_of_true", metricName="areaUnderROC")
            auc = evaluator.evaluate(predictions)
    
            # Extract the probability and label to Pandas dataframe
            prob_labels = predictions.select("probability_of_true", period_label).toPandas()
    
            # Compute ROC curve using sklearn
            fpr, tpr, _ = roc_curve(prob_labels[period_label], prob_labels['probability_of_true'])
            roc_auc = sklearn_auc(fpr, tpr)
    
            # Plot ROC Curve
            plt.figure()
            plt.plot(fpr, tpr, color='darkorange', lw=2, label='ROC curve (area = %0.2f)' % roc_auc)
            plt.plot([0, 1], [0, 1], color='navy', lw=2, linestyle='--')
            plt.xlim([0.0, 1.0])
            plt.ylim([0.0, 1.05])
            plt.xlabel('False Positive Rate')
            plt.ylabel('True Positive Rate')
            plt.title(f'ROC Curve - Logistic Regression ({period_label[-5:]})')
            plt.legend(loc="lower right")
    
            # Save Plot
            plt.savefig(f"roc_curve_logreg_{period_label[-5:]}.png")

            # Baseline Model: Predict all as the majority class
            majority_class = train_data.groupBy(period_label).count().orderBy("count", ascending=False).first()[0]
            baseline_predictions = test_data.withColumn("prediction", lit(majority_class).cast(DoubleType()))

    
            # Evaluate Baseline Model
            baseline_accuracy = class_evaluator.setMetricName("accuracy").setLabelCol(period_label).evaluate(baseline_predictions)
            print(f"\nBaseline Model (Predict all as {majority_class})")
            print(f"Accuracy: {baseline_accuracy}")
    
            # Residual Plots
            residuals = predictions.select(period_label, "prediction").withColumn("residual", col(period_label) - col("prediction"))
            residuals_pd = residuals.toPandas()
            plt.scatter(residuals_pd['prediction'], residuals_pd['residual'])
            plt.axhline(y=0, color='r', linestyle='--')
            plt.xlabel("Predicted Values")
            plt.ylabel("Residuals")
            plt.title(f"Residuals vs Predicted Values ({period_label[-5:]})")
            plt.savefig(f"residuals_vs_predicted_{period_label[-5:]}.png")
            plt.show()
    
            # Precision-Recall Curve
            prob_labels = predictions.select("probability_of_true", period_label).toPandas()
            precision, recall, _ = precision_recall_curve(prob_labels[period_label], prob_labels['probability_of_true'])
            average_precision = average_precision_score(prob_labels[period_label], prob_labels['probability_of_true'])
    
            plt.step(recall, precision, color='b', alpha=0.2, where='post')
            plt.fill_between(recall, precision, step='post', alpha=0.2, color='b')
            plt.xlabel('Recall')
            plt.ylabel('Precision')
            plt.ylim([0.0, 1.05])
            plt.xlim([0.0, 1.0])
            plt.title(f'Precision-Recall curve: AP={average_precision:0.2f}')
            plt.savefig(f"precision_recall_curve_{period_label[-5:]}.png")
    
        
    # Regression Analysis
    lr.setLabelCol(incident_label)
    lr_model = lr.fit(train_data)
    lr_predictions = lr_model.transform(test_data)
    
    # R-Squared Value
    r2 = reg_evaluator.setMetricName("r2").setLabelCol(incident_label).evaluate(lr_predictions)
    print(f"\nRegression Model: Linear Regression")
    print(f"R-Squared (R2) on test data = {r2}")

    # Root Mean Squared Error (RMSE)
    rmse = reg_evaluator.setMetricName("rmse").setLabelCol(incident_label).evaluate(lr_predictions)
    print(f"Root Mean Squared Error (RMSE) on test data = {rmse}")

# Stop Spark session
spark.stop()
