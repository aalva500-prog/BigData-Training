# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 08:43:55 2021

@author: Aaron Alvarez
"""

from pyspark.sql import SparkSession #
#from pyspark.sql import row # to read the row

# Create a spark session

# Note: Spark session is a combination of multiple entities and internally it creates a SparkContext
# Spark session is the new way of creating SparkContext
spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

friends = spark.read.option("header", "true").option("inferSchema", "True").csv("fakefriends-header.csv")

print("infer our schema")
friends.printSchema()

friends.select("name").show()

friends.filter(friends.age < 21).show()

friends.groupBy("age").count().show()

# Don't forget to stop the session, otherwise a lot of memory will be wasted
spark.stop()