# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 11:57:41 2021

@author: Aaron Alvarez
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("MinTemp").getOrCreate()

# creating schema out of a csv file
schema = StructType([\
                   StructField("stationID",StringType(), True),\
                   StructField("date", IntegerType(), True),\
                   StructField("measure_type", StringType(), True),\
                   StructField("temperature", FloatType(), True)])

df = spark.read.schema(schema).csv("1800.csv")
df.printSchema()

# Filter the measure type to get only TMIN
minTemps = df.filter(df.measure_type == "TMIN")

# Filter the measure type to get only TMIN
maxTemps = df.filter(df.measure_type == "TMAX")

stationTemps = minTemps.select("stationID", "temperature")

minTempByStation = stationTemps.groupBy("stationID").min("temperature")
print("Show Min Temps:\n")
minTempByStation.show()

maxTempByStation = stationTemps.groupBy("stationID").max("temperature")
print("Show Max Temps:\n")
maxTempByStation.show()

spark.stop()