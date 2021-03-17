# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 13:47:46 2021

@author: Aaron Alvarez
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Expenses By Customer").getOrCreate()

# creating schema out of a csv file
schema = StructType([\
                   StructField("personID",IntegerType(), True),\
                   StructField("name", StringType(), True),\
                   StructField("age", IntegerType(), True),\
                   StructField("numberFriends", IntegerType(), True)])
    
    
df = spark.read.schema(schema).csv("fakefriends.csv")

# Solution #1
averageFriendsByAge1 = df.groupBy("age").agg(func.mean("numberFriends")).orderBy("age")
averageFriendsByAge1.show()

# Solution #2
averageFriendsByAge2 = df.groupBy("age").agg(func.round(func.avg("numberFriends"), 2).alias("average_friends"))
averageFriendsByAgeSorted = averageFriendsByAge2.sort("average_friends")
averageFriendsByAgeSorted.show(averageFriendsByAgeSorted.count())

spark.stop()
