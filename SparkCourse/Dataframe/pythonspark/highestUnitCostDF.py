# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 09:52:16 2021

@author: Aaron Alvarez

@purpose: Use DataFrame to get highest unit cost of an item per region
"""
# Import libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

# Create Spark Session
spark = SparkSession.builder.appName("Highest Unit Cost").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("sales.csv")

# Get max Unit Cost per Region
maxUnitCostPerRegion = df.groupBy("Region", "Item Type").max("Unit Cost")

# Sort List by max(Unit Cost)
sortedList = maxUnitCostPerRegion.sort(func.desc("max(Unit Cost)"))

# Show the highest unit cost per region
print("The highest Unit Cost of an Item per Region is shown below:\n")
sortedList.show(7)

# Stop Spark app
spark.stop()
