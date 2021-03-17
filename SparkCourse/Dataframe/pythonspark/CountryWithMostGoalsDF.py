# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 09:06:43 2021

@author: Aaron Alvarez
"""

# Import necessary libraries
from pyspark.sql import SparkSession
from pyspark.sql import functions as func
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType

spark = SparkSession.builder.appName("Country With Most Goals").getOrCreate()

#Create a dataframe, taking in the header row to create the columns
df = spark.read.option("header","true").option("inferSchema","true").csv("world_cup_matches.csv")

# Filter to get only the teams who have played in smei-finals
semifinalsCountries = df.filter(df.Stage == "Semi-finals")

# Filter by between years: 1950 and 2005
yearFilter1 = semifinalsCountries.filter(df.Year > 1949)
yearFilter2 = yearFilter1.filter(df.Year < 2006)

# Get the lists of Home and Away Teams
homeTeamList = yearFilter2.select("Home Team Goals", "Home Team Name", "Stage", "Year")
awayTeamList = yearFilter2.select("Away Team Goals", "Away Team Name", "Stage", "Year")

# Sort the lists of Home and Away Teams
sortedHomeTeamMostGoals = homeTeamList.sort(func.desc("Home Team Goals"))
sortedawayTeamMostGoals = awayTeamList.sort(func.desc("Away Team Goals"))

goalsByCountry = sortedHomeTeamMostGoals.unionAll(sortedawayTeamMostGoals)

goalsByCountry.show(1)

# Teams with most goals in semi-finals
# print("The Home Team that has scored most goals in semi-finals between 1950 and 2005:\n")
# sortedHomeTeamMostGoals.show(1)

# print("The Away Team that has scored most goals in semi-finals between 1950 and 2005:\n")
# sortedawayTeamMostGoals.show(1)

spark.stop()
