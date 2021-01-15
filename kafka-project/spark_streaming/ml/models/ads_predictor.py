from pyspark.ml import Pipeline
from pyspark.ml.regression import RandomForestRegressor
from pyspark.ml.feature import VectorIndexer
from pyspark.ml.evaluation import RegressionEvaluator


class ADSPredictor:
    def __init__(self, spark_):
        # Load and parse the data file, converting it to a DataFrame.
        data = spark_.read\
            .format("libsvm")\
            .option("numFeatures", "3")\
            .load("ml/datasets/Social_Network_Ads_libsvm_train.dat")

        # Automatically identify categorical features, and index them.
        # Set maxCategories so features with > 4 distinct values are treated as continuous.
        featureIndexer = \
            VectorIndexer(inputCol="features", outputCol="indexedFeatures", maxCategories=2).fit(data)

        # Split the data into training and test sets (30% held out for testing)
        # (trainingData, testData) = data.randomSplit([0.7, 0.3])

        # Train a RandomForest model.
        rf = RandomForestRegressor(featuresCol="indexedFeatures")

        # Chain indexer and forest in a Pipeline
        pipeline = Pipeline(stages=[featureIndexer, rf])

        # Train model.  This also runs the indexer.
        self.model = pipeline.fit(data)
        print("MODEL READY===================================")
        # # Make predictions.
        # predictions = self.model.transform(data)
        #
        # # Select example rows to display.
        # predictions.select("prediction", "label", "features").show(5)
        #
        # # Select (prediction, true label) and compute test error
        # evaluator = RegressionEvaluator(
        #     labelCol="label", predictionCol="prediction", metricName="rmse")
        # rmse = evaluator.evaluate(predictions)
        # print("Root Mean Squared Error (RMSE) on test data = %g" % rmse)
        #
        # rfModel = self.model.stages[1]
        # print(rfModel)  # summary only

    def predict(self, features):
        return self.model.transform(features)
