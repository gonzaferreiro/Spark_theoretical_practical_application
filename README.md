# Spark_theoretical_practical_application

In this repository I'll be exploring in deep three labs from my Immersive Course in Data Science about Spark including some basic map reduce and SQL-Spark operations, as well as a bit of modelling through Spark

## What folders you'll find in this repository

### 1. Spark_map_reduce_ and_SQL_practice

In this lab, we will use Spark to dig into the Bay Area Bike Share data. 

The main goals of this notebooks are:

* Practice some map reduce operations

MapReduce is a programming model designed for processing large volumes of data in parallel by dividing the work into a set of independent tasks. It works based on two concepts:

Map: Breaks down a problem into simple pieces
Reduce: Collates the broken problems into a single solution.

In between Map and Reduce, there is small phase called Shuffle and Sort in MapReduce.

The following picture from https://www.guru99.com explains the process better:

<img src="https://www.guru99.com/images/Big_Data/061114_0930_Introductio1.png" height="500" />

In this picture, a job of mapping phase is to count a number of occurrences of each word from input splits and prepare a list in the form of <word, frequency>. This is the very first phase in the execution of map-reduce program. In this phase data in each split is passed to a mapping function to produce output values.

The shuffiling phase consumes the output of Mapping phase. Its task is to consolidate the relevant records from Mapping phase output. In our example, the same words are clubed together along with their respective frequency.

Finally, output values from the Shuffling phase are aggregated when reducing. This phase combines values from Shuffling phase and returns a single output value. In short, this phase summarizes the complete dataset. In our example, this phase aggregates the values from Shuffling phase i.e., calculates total occurrences of each word.

* Use Spark SQL context

Besides the SparkContext, Spark also exposes a sqlContext that allows us to perform SQL queries on an RDD object. In this notebook we'll run a query using a sqlContext to obtain the average duration of a trip originating from the Caltrain station.

This notebook includes:

1. Loading a csv file into a sqlContext
2. Printing the data, schema and columns
3. Renaming columns
4. Some basic SQL operations through Spark

### 2. Spark_simple_regression_and_classification

This notebook will go from zero to building a classification and regression models with spark. Understanding how features and labels have to be encoded in spark and how should we construct a model pipeline.

The following points will be done step by step:

1. Creating the Spark context
2. Loading data (the Boston Housing dataset) in a regular format
3. Putting the data into a Spark dataframe
4. Working with Spark StandardScaler and VectorAssembler for feature engineering
5. Rescaling the data
6. Creating a train-test split
7. Using grid search to improve results
6. Fitting and evaluate models

### 3. Spark_application_with_categorical_features

Often we have categorical features with values given as strings which we would like to transform to numerical values. In this notebook we'll fit a classification model for a dataset with categorical features, using first the StringIndexer, then the OneHotEncoderEstimator tools from pyrspark library, to create the dummified variables. 
