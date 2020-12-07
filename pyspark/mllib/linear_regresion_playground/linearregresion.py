# -*- coding: utf-8 -*-
"""LinearRegresion.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/16B-lIetHREsKPMeuG_sXoo-eJ30Rpl-r
"""

!pip install pyspark

from pyspark import SparkContext

from pyspark.sql import SparkSession

spark = SparkSession.builder.appName('reg').getOrCreate()

from pyspark.ml.regression import LinearRegression

train = spark.read.format("libsvm").load("sample_linear_regression_data.txt")

train.show()

lr = LinearRegression(featuresCol = 'features', labelCol = 'label', predictionCol =  'prediction')

modelo = lr.fit(train)

modelo.coefficients

modelo.intercept

summary = modelo.summary

summary.predictions.show()

summary.rootMeanSquaredError

summary.r2

## Split the data set

data = spark.read.format("libsvm").load("sample_linear_regression_data.txt")

split = data.randomSplit([0.7,0.3])

split

train, test = data.randomSplit([0.7,0.3]) # Tuple unpacking

train

test

train.describe().show()

test.describe().show()

correct_model = lr.fit(train)

results_test = correct_model.evaluate(test)

results_test.rootMeanSquaredError

data_without_label = test.select('features')

data_without_label.show()

## Evaluate our test data set

predictions = correct_model.transform(data_without_label)

predictions.show()
