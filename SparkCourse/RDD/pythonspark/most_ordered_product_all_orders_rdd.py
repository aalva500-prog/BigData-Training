# -*- coding: utf-8 -*-
"""
Created on Wed Feb 24 08:35:05 2021

@author: Aaron Alvarez

Purpose: The purpose of this program is to obtain the product that was most ordered from a list
"""
from pyspark import SparkConf, SparkContext
import collections

# Configure app and provide name
conf = SparkConf().setMaster("local").setAppName("Product Most Ordered")
sc= SparkContext(conf = conf)

# Helper function to parse the lines of the csv file and get the neccesary fields for the exercise
def parseLine(line):
    fields = line.split(',')
    productID = int(fields[2])
    quantity = int(fields[3])
    return (productID, quantity)

# Read csv file
lines = sc.textFile("file:///SparkCourse/order_items.csv")

# Parse lines
parsedLines = lines.map(parseLine)

# Get totals by product
totalsByProduct = parsedLines.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))

# Sort results by total quantity ordered by product
sortedByquantity = totalsByProduct.sortBy(lambda x: x[1])

# Collect the result
results = sortedByquantity.collect()

# Print out the results
for result in results:
    print(result)