# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 11:43:56 2021

@author: Aaron Alvarez
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as func

spark = SparkSession.builder.appName("Units Sold in Andorra").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("10000 Sales Records.csv")

# Get the total of units sold in Andorra
totalByCountry = df.groupBy("Country").agg(func.sum("Units Sold").alias("Total_Units_Sold")) 

# Filter to getAndorra
andorra  = totalByCountry.filter("Country == 'Andorra'") # this could also be totalByCountry.filter(totalByCountry.Country == 'Andorra')

# Show the output
andorra.show()

# Stop the app
spark.stop()