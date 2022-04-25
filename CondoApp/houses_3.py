# -*- coding: utf-8 -*-
"""houses_3.ipynb

Automatically generated by Colaboratory.

Original file is located at
    https://colab.research.google.com/drive/1fJkOzPrh-1XFedK6WkKMo7nO6ep8s7FC
"""

from google.colab import drive
drive.mount('/content/drive')

# Commented out IPython magic to ensure Python compatibility.
# %%capture
# !pip install pyspark

# Commented out IPython magic to ensure Python compatibility.
# %%capture
# !pip install -q handyspark

"""# Import Dependencies"""

import json
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.ml.feature import *
from pyspark.ml import *
from pyspark.ml.evaluation import *
from handyspark import *
from matplotlib import pyplot as plt

spark = SparkSession.builder.appName('home_value_predict_project').getOrCreate()
spark.sparkContext.setLogLevel("ERROR")
spark

# with open("/content/drive/MyDrive/DSE_6000 project/data_housing/houses.csv", 'r') as content_file:
#   schema_json = content_file.read()
# new_schema = StructType.fromJson(json.loads(schema_json))
# print(new_schema

"""# ETL 
(Extract, Transform, Load)

## Load Data
"""

df_spark = spark.read.csv("/content/drive/MyDrive/DSE_6000 project/data_housing/houses.csv", inferSchema=True, header=True)

df_spark.show(2)

print(list(df_spark.columns))

"""## Select Features"""

cols_to_select = ("sq_mt_built","n_rooms","n_bathrooms","floor","buy_price")
df_spark = df_spark.select(*cols_to_select)
df_spark.show(2)

#df_spark.printSchema()

dim_all_data = (df_spark.count(), len(df_spark.columns))
dim_all_data

df_spark.select([count(when(col(c).isNull(), c)).alias(c) for c in df_spark.columns]).show()

"""## Reset df_etl_1"""

df_etl_1 = df_spark.na.drop(how="any")

dim_data_etl = (df_etl_1.count(), len(df_etl_1.columns))
dim_data_etl

"""Percent of data retained"""

int(100*dim_data_etl[0] / dim_all_data[0])

df_etl_1.select([count(when(col(c).isNull(), c)).alias(c) for c in df_etl_1.columns]).show()

#Replace string column value conditionally
df_etl_1 = df_etl_1.withColumn('floor', 
    when(df_etl_1.floor.endswith('1'),regexp_replace(df_etl_1.floor,'1','first_flr')) \
   .when(df_etl_1.floor.endswith('2'),regexp_replace(df_etl_1.floor,'2','second_flr')) \
   .when(df_etl_1.floor.endswith('3'),regexp_replace(df_etl_1.floor,'3','third_flr')) \
   .when(df_etl_1.floor.endswith('4'),regexp_replace(df_etl_1.floor,'4','fourth_flr')) \
   .when(df_etl_1.floor.endswith('5'),regexp_replace(df_etl_1.floor,'5','fifth_flr')) \
   .when(df_etl_1.floor.endswith('6'),regexp_replace(df_etl_1.floor,'6','sixth_flr')) \
   .when(df_etl_1.floor.endswith('7'),regexp_replace(df_etl_1.floor,'7','seventh_flr')) \
   .when(df_etl_1.floor.endswith('8'),regexp_replace(df_etl_1.floor,'8','eighth_flr')) \
   .when(df_etl_1.floor.endswith('9'),regexp_replace(df_etl_1.floor,'9','ninth_flr')) \
   .when(df_etl_1.floor.contains('Bajo'),regexp_replace(df_etl_1.floor,'Bajo', 'Null')) \
   .when(df_etl_1.floor.contains('Entreplanta'),regexp_replace(df_etl_1.floor,'Entreplanta', 'Null')) \
   .when(df_etl_1.floor.contains('Entreplanta exterior'),regexp_replace(df_etl_1.floor,'Entreplanta exterior', 'Null')) \
   .when(df_etl_1.floor.contains('Entreplanta interior'),regexp_replace(df_etl_1.floor,'Entreplanta interior', 'Null')) \
   .when(df_etl_1.floor.contains('Semi-sótano'),regexp_replace(df_etl_1.floor,'Semi-sótano', 'Null')) \
   .when(df_etl_1.floor.contains('Semi-sótano exterior'),regexp_replace(df_etl_1.floor,'Semi-sótano exterior', 'Null')) \
   .when(df_etl_1.floor.contains('Semi-sótano interior'),regexp_replace(df_etl_1.floor,'Semi-sótano interior', 'Null')) \
   .when(df_etl_1.floor.contains('Sótano'),regexp_replace(df_etl_1.floor,'Sótano', 'Null')) \
   .when(df_etl_1.floor.contains('Sótano exterior'),regexp_replace(df_etl_1.floor,'Sótano exterior', 'Null')) \
   .when(df_etl_1.floor.contains('Sótano interior'),regexp_replace(df_etl_1.floor,'Sótano interior', 'Null')) \
   .when(df_etl_1.floor.endswith('interior'),regexp_replace(df_etl_1.floor,'interior', 'Null')) \
   .when(df_etl_1.floor.endswith('exterior'),regexp_replace(df_etl_1.floor,'exterior', 'Null')) \
   .otherwise(df_etl_1.floor))

df_etl_1 = df_etl_1.withColumn('floor', 
    when(df_etl_1.floor.endswith('interior'),regexp_replace(df_etl_1.floor,'interior', 'Null')) \
   .when(df_etl_1.floor.endswith('exterior'),regexp_replace(df_etl_1.floor,'exterior', 'Null')) \
   .otherwise(df_etl_1.floor))

df_floor = df_etl_1.groupBy('floor').count().orderBy('count')
df_floor.show()

df_etl_1 = df_etl_1.where(df_etl_1.floor !='Null')
df_etl_1.show()

dim_data_etl = (df_etl_1.count(), len(df_etl_1.columns))
dim_data_etl

int(100*dim_data_etl[0] / dim_all_data[0])

"""## EDA, viz
(Exploratory Data Analysis)
"""

hdf_etl_1 = df_etl_1.toHandy()

hdf_etl_1.show(3)

hdf_etl_1.cols['floor'].value_counts(dropna=False)

print(hdf_etl_1.cols['floor'].nunique())
print()

# hdf_etl_1.cols['operation'].nunique()

fig, axs = plt.subplots(1, 4, figsize=(30, 4))
hdf_etl_1.cols['sq_mt_built'].hist(ax=axs[0])
hdf_etl_1.cols['n_rooms'].hist(ax=axs[1])
hdf_etl_1.cols['n_bathrooms'].hist(ax=axs[2])
hdf_etl_1.cols['floor'].hist(ax=axs[3])

# boxplot doesn't work with buy_price
# hdf_etl_1.cols['buy_price'].boxplot()
fig, axs = plt.subplots(1, 3, figsize=(30, 4))
hdf_etl_1.cols[['n_bathrooms','buy_price']].scatterplot(ax=axs[0])
hdf_etl_1.cols[['n_rooms','buy_price']].scatterplot(ax=axs[1])
hdf_etl_1.cols[['sq_mt_built','buy_price']].scatterplot(ax=axs[2])

"""Future work, explore variable transformation e.g. log(var)

## Correlation
"""

from pyspark.mllib.stat import Statistics
# select variables to check correlation
df_features = df_etl_1.select("n_bathrooms","n_rooms","sq_mt_built","buy_price") 
# create RDD table for correlation calculation
rdd_table = df_features.rdd.map(lambda row: row[0:])
# get the correlation matrix
corr_mat=Statistics.corr(rdd_table, method="pearson")

print(corr_mat)

"""## Write (overwrite) to json, Clean Data

### RESET ML input data
"""

df_etl_1\
  .coalesce(1)\
  .write\
  .mode('Overwrite') \
  .format('json')\
  .save('/content/drive/MyDrive/DSE_6000 project/df_etl_1.json')

"""# ETL Done

# Load data, json
"""

df_etl_2 = []
file_path = "/content/drive/MyDrive/DSE_6000 project/df_etl_1.json"
df_etl_2 = spark.read.json(file_path)

df_etl_2.show(5)

df_etl_2.printSchema()

"""### Save Schema"""

col_list_to_drop = ["buy_price"]
df_for_schema = df_etl_2.drop(*col_list_to_drop)

with open("/content/drive/MyDrive/DSE_6000 project/df_etl_2_schema.json", "w") as f:
  f.write(df_for_schema.schema.json())

"""# Define Stages of Pipeline

### comments
"""

# # Assemble pipeline
# pipeline_1 = Pipeline(stages=stages_list_1)
# # Estimator fit , Train the model
# pipeline_model_1 = pipeline_1.fit(df_etl_2)

# df_etl_3 = pipeline_model_1.transform(df_etl_2)

# col_list_to_drop = ("floor", "floor_str_ix")
# df_etl_3 = df_etl_3.drop(*col_list_to_drop)

# df_etl_3.printSchema()

# df_etl_3.show(3)

"""## One Hot Encoding

## Linear Regression
"""

# reset stages list
stages_list_1 = []
# specify columns to encode
cols_to_encode = ["floor"]
# define stages in pipeline
for col in cols_to_encode:
    # recast to string_index type from original type. ### Convert to String first.
    # NOTE: It really converts to string_type that is indexed by frequency, max_frequency is given index 0.
    stages_list_1.append(StringIndexer(inputCol=col, outputCol=col + '_str_ix', handleInvalid='skip'))
    # OneHotEncoder
    stages_list_1.append(OneHotEncoder(inputCol=col + '_str_ix', outputCol=col + '_encd_vec'))
    # recast to categorical variable from string_index
    # stages_list.append(IndexToString(inputCol=col + '_str_ix', outputCol=col + '_catg'))
  
# train-test split
train_data, test_data = df_etl_2.randomSplit([0.80, 0.20], seed=2022)
print("train_data= ")
train_data.show(2)
print("test_data= ")
test_data.show(2)

# specify predictor_cols
predictor_cols = ['n_bathrooms', 'n_rooms', 'sq_mt_built', "floor_encd_vec"]    
# set response/target column name
target_col_name = "buy_price"

# append VectorAssembler to stages list
stages_list_1.append(VectorAssembler(inputCols=[col for col in predictor_cols], 
                                     outputCol='features'
                                    )
                    )

# define model
from pyspark.ml.regression import LinearRegression
lm_model = LinearRegression(featuresCol='features', labelCol= target_col_name)
# append model to stages list
stages_list_1.append(lm_model)
print("Stages of Pipeline", stages_list_1,"\n")

# Assemble pipeline
pipeline_1 = Pipeline(stages=stages_list_1)

# Estimator fit , Train the model
pipeline_model_1 = pipeline_1.fit(train_data)

# Transformer fit, Make Predictions
# predictions on train data are for reference
df_train_preds = pipeline_model_1.transform(train_data)
# predictions on test data are of main interest
df_test_preds = pipeline_model_1.transform(test_data)

# drop predictor cols since they are in the "features" vector
col_list_to_drop = ['n_bathrooms', 'n_rooms', 'sq_mt_built', "floor_encd_vec",
                    "floor", "floor_str_ix"]
# drop other cols
df_train_preds = df_train_preds.drop(*col_list_to_drop)
df_test_preds = df_test_preds.drop(*col_list_to_drop)

print("df_train_preds= ")
df_train_preds.show(2)

print("df_test_preds= ")
df_test_preds.show(2)

# print features used
print("Features: ", predictor_cols)
print("Example Features vector: ", df_train_preds.collect()[0])

# Model evaluator
evaluator = RegressionEvaluator(predictionCol="prediction", labelCol=target_col_name)
# Make predictions using model
dataset = df_train_preds.select(target_col_name, "features", "prediction")
train_r2 = evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
# Make predictions using model
dataset = df_test_preds.select(target_col_name, "features", "prediction")
test_r2 = evaluator.evaluate(dataset, {evaluator.metricName: "r2"})

# R2 for train_data
dataset = df_train_preds.select(target_col_name, "features", "prediction")
train_r2 = evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
print("\n", "train_r2= ", train_r2)
# R2 for test_data
dataset = df_test_preds.select(target_col_name, "features", "prediction")
test_r2 = evaluator.evaluate(dataset, {evaluator.metricName: "r2"})
print(" test_r2= ", test_r2)
# RMSE for train_data
dataset = df_train_preds.select(target_col_name, "features", "prediction")
train_rmse = evaluator.evaluate(dataset, {evaluator.metricName: "rmse"})
print(" train_rmse= ", train_rmse)
# RMSE for test_data
dataset = df_test_preds.select(target_col_name, "features", "prediction")
test_rmse = evaluator.evaluate(dataset, {evaluator.metricName: "rmse"})
print(" test_rmse= ", test_rmse)

"""# Productionize/Persist/Save model"""

file_path = "/content/drive/MyDrive/DSE_6000 project/model_saved"
#pipeline_model_2.toPMML("/content/drive/MyDrive/DSE_6000 project")
sc = spark.sparkContext
pipeline_model_1.write().overwrite().save(file_path)

"""# Local Testing, Import model"""

# file_path = "/content/drive/MyDrive/DSE_6000 project/model_saved"
# persistedModel = PipelineModel.load(file_path)

# print(persistedModel)

"""## Never seen test data"""

# df = []
# file_path = "/content/drive/MyDrive/DSE_6000 project/unseen_test_data.json"
# df = spark.read.json(file_path)
# # df.show(3)
# # predict on new test data
# df_new_test_data = df
# df_preds = persistedModel.transform(df_new_test_data)
# print("Predictions: \n")
# col_list_to_drop = ["floor_str_ix", "floor_encd_vec", "features"]
# df_preds = df_preds.drop(*col_list_to_drop)
# df_preds.show(5)

# df = []
# file_path = '/content/drive/MyDrive/DSE_6000 project/user_test_data.json'
# df = spark.read.json(file_path)
# # df.show()
# # predict on new test data
# df_new_test_data = df
# df_preds = persistedModel.transform(df_new_test_data)
# print("Predictions: \n")
# col_list_to_drop = ["floor_str_ix", "floor_encd_vec", "features"]
# df_preds = df_preds.drop(*col_list_to_drop)
# df_preds.show()