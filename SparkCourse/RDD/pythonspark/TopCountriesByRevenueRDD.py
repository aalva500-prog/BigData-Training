# -*- coding: utf-8 -*-
"""
Created on Fri Mar 12 09:24:34 2021

@author: Aaron Alvarez
"""
from pyspark import SparkConf, SparkContext

# Configure app and provide name
conf = SparkConf().setMaster("local").setAppName("Top Five Countries By Revenue")
sc= SparkContext(conf = conf)

# Helper function to parse the lines of the csv file and get the neccesary fields for the exercise
def parseLine(line):
    fields = line.split(',')
    country = fields[1]
    revenue = fields[11]
    return (country, revenue)

# Read csv file
lines = sc.textFile("file:///SparkCourse/10000 Sales Records.csv")

# Parse lines
parsedLines = lines.map(parseLine)

# Get rid of the first line
result = parsedLines.filter(lambda x: (x[0]!= 'Country'))

# Get totals by country
revenueByCountry = result.reduceByKey(lambda x,y:(float(x) + float(y)))

# Sort results by total revenue by country
sortedByRevenue = revenueByCountry.sortBy(lambda x: x[1])

# Collect the result and get the top Five Countries
topFive = sortedByRevenue.collect()[-5:]

# Print out the result
print("\nThe top five countries by revenue are listed below:\n")
for key, value in topFive:
    print(key + " with a total of $" + str(value))