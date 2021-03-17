# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 14:39:55 2021

@author: Aaron Alvarez
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType


spark = SparkSession.builder.appName("Never Won Country").getOrCreate()    
    
ds = spark.read.option("header", "true").option("inferSchema", "true").csv("world_cup_matches.csv")

noFinalCountries = ds.filter(ds.Stage !="Final")

homeTeams = noFinalCountries.select("Home Team Name")
    
awayTeams = noFinalCountries.select("Away Team Name")

print("Teams that have never won the cup:")
    
distinctTeams = homeTeams.unionAll(awayTeams).distinct()    
    
distinctTeams.show()