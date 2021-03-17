# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 10:26:57 2021

@author: Aaron Alvarez

@purpose: Use DataFrame to get total sales of each region based on based on
         total unit sold sales and revenue - 2 lists both separately
"""
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create Spark Session
spark = SparkSession.builder.appName("Sales per Region").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("sales.csv")

# Get Units sold per region
unitsSoldPerRegion = df.groupBy("Region").agg(func.sum("Units Sold").alias("Total Sold"))

# Sort the regions by units sold
sortedUnitsSoldPerRegion = unitsSoldPerRegion.sort(func.desc("Total Sold"))

# Get total revenue per region
revenuePerRegion = df.groupBy("Region").agg(func.round(func.sum("Total Revenue"), 2).alias("Total_Revenue"))

# Sort the regions by Total Revenue
sortedRegionsByRevenue = revenuePerRegion.sort(func.desc("Total_Revenue"))

# Show Units Sold Per Region
print("The Units Sold Per Region are shown below:\n")
sortedUnitsSoldPerRegion.show()

# Show Total Revenue Per Region
print("The Total Revenue per Region is shown below:\n")
sortedRegionsByRevenue.show()

# Stop spark App
spark.stop()