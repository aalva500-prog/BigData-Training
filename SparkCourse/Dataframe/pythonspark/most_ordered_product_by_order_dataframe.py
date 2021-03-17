# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 09:38:55 2021

@author: Aaron Alvarez

Purpose: The purpose of this program is to obtain the product that was most ordered from a list
"""
# Import necessary libraries
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

# Count the productsID and group by the column named "productID"
product_count = df.groupBy("productID").count()  

# Sort the list of products by count                                               
sorted_products = product_count.sort(func.desc("count"))

# Show the output
#sorted_products.show(sorted_products.count())
sorted_products.show(1)

# Stop the app
spark.stop()