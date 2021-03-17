# -*- coding: utf-8 -*-
"""
Created on Fri Feb 26 07:25:08 2021

@author: Aaron Alvarez
"""
from pyspark import SparkConf, SparkContext
import collections

# Configure app and provide name
conf = SparkConf().setMaster("local").setAppName("Product Most Ordered")
sc= SparkContext(conf = conf)

# Helper function to parse the lines of the csv file and get the neccesary fields for the exercise
def parseLine(line):
    fields = line.split(',')
    city = fields[4]
    return (city)

# Read csv file
lines = sc.textFile("file:///SparkCourse/world_cup_matches.csv")

# Parse lines
parsedLines = lines.map(parseLine)

# Get totals by city
cityCount = parsedLines.map(lambda x: (x,1)).reduceByKey(lambda x,y: x+y)

# Sort results by count
sortedMovies = cityCount.sortBy(lambda x: x[1])

# Collect the result
results = sortedMovies.collect()

print(str(results[-1][0]) + "has hosted the most number of world cup matches, " + str(results[-1][1]) + " times.")