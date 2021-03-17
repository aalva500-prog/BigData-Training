# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 11:41:33 2021

@author: Aaron Alvarez

@purpose: Use DataFrame to get the item that has the most 
        sales across all regions based on list based on 
        total unit sold sales and revenue - 2 list s both separately
"""

# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create Spark Session
spark = SparkSession.builder.appName("Item with most sales").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("sales.csv")

# Get Units sold per Item Type
unitsSoldPerRegion = df.groupBy("Item Type").agg(func.sum("Units Sold").alias("Total Sold"))

# Sort the Items by units sold
sortedUnitsSoldPerRegion = unitsSoldPerRegion.sort(func.desc("Total Sold"))

# Get total revenue per Item
revenuePerRegion = df.groupBy("Item Type").agg(func.round(func.sum("Total Revenue"), 2).alias("Total_Revenue"))

# Sort the regions by Total Revenue
sortedRegionsByRevenue = revenuePerRegion.sort(func.desc("Total_Revenue"))

# Show Units Sold Per Item Type
print("The Units Sold Per Item Type are shown below:\n")
sortedUnitsSoldPerRegion.show()

# Show Total Revenue Per Item Type
print("The Total Revenue per Item Type is shown below:\n")
sortedRegionsByRevenue.show()

# Stop spark App
spark.stop()