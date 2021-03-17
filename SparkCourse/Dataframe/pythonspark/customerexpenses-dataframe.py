# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 13:06:27 2021

@author: Aaron Alvarez
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Expenses By Customer").getOrCreate()

# creating schema out of a csv file
schema = StructType([\
                   StructField("customerID",IntegerType(), True),\
                   StructField("articleID", IntegerType(), True),\
                   StructField("moneySpent", FloatType(), True)])
    
df = spark.read.schema(schema).csv("customer-orders.csv")
df.printSchema()

customer_expenses = df.groupBy("customerID").agg(func.round(func.sum("moneySpent"), 2).alias("total_spent"))
                                                        
totalByCustomerSorted = customer_expenses.sort("total_spent")

totalByCustomerSorted.show(totalByCustomerSorted.count())

spark.stop()
