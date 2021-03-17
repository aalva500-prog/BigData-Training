# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 09:38:55 2021

@author: Aaron Alvarez

Purpose: The purpose of this program is to obtain the product that was most ordered from a list
"""
# Import neccesary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Most Ordered Product").getOrCreate()

# creating schema out of a csv file
schema = StructType([\
                   StructField("serialNum",IntegerType(), True),\
                   StructField("orderID", IntegerType(), True),\
                   StructField("productID", IntegerType(), True),\
                   StructField("quantity", IntegerType(), True),\
                   StructField("totalCost", IntegerType(), True),\
                   StructField("unitCost", FloatType(), True)])
    
# Read csv file and create dataframe  
df = spark.read.schema(schema).csv("order_items.csv")

# Group by productID, sum the quantities sold for each product, and create an alias for the sum
quantities_product = df.groupBy("productID").agg(func.sum("quantity").alias("total_quantities"))         

# Sort the list of products by quantity                                               
totalByCustomerSorted = quantities_product.sort(func.desc("total_quantities"))

# Show the output
#totalByCustomerSorted.show(totalByCustomerSorted.count())
totalByCustomerSorted.show(1)

# Stop the app
spark.stop()
