# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 12:22:14 2021

@author: Aaron Alvarez
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MovieRatings").getOrCreate()

# creating schema out of a csv file
schema = StructType([\
                   StructField("userID",IntegerType(), True),\
                   StructField("movieID", IntegerType(), True),\
                   StructField("rating", IntegerType(), True),\
                   StructField("time_stamp", StringType(), True)])
    
df = spark.read.option("sep", "\t").schema(schema).csv("u.data")
df.printSchema()

topMoviesIDs = df.groupBy("movieID").count().orderBy(func.desc("count"))
topMoviesIDs.show(10)

spark.stop()
