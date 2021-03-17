# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 11:13:32 2021

@author: Aaron Alvarez
"""

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("Matches in each World Cup").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("world_cup_matches.csv")

# Count the number of times each year appears and group by year
year_count = df.groupBy("Year").count() 

# Sort the list by Year                                              
sorted_years = year_count.sort("Year")

# Show the output
sorted_years.show(sorted_years.count())

# Stop the app
spark.stop()
