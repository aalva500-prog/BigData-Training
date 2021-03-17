# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 09:06:43 2021

@author: Aaron Alvarez
"""

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Country With Most Goals").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("world_cups.csv")

countries = df.select("Year", "Country", "GoalsScored")

result = countries.sort(func.desc("GoalsScored"))

result.show()
