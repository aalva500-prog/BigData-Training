# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 09:12:13 2021

@author: Administrator
"""

from pyspark import SparkConf, SparkContext
import collections

# Configuration and set the Spark cluster and App Name
conf = SparkConf().setMaster("local").setAppName("Temperature")
sc= SparkContext(conf = conf)

# Whenever you read a csv file and split it by rows
def parseLine(line):
    fields = line.split(',')
    stationID = fields[0]
    entryType = fields[2]
    temperature = float(fields[3]) * 0.1 * (9.0/5.0) + 32.0
    return (stationID, entryType, temperature)
    

lines = sc.textFile("file:///SparkCourse/1800.csv")
parsedLines = lines.map(parseLine) # parsing the lines with the help of the parsLine function
#minTemps = parsedLines.filter(lambda x:"TMIN" in x[1])
maxTemps = parsedLines.filter(lambda x:"TMAX" in x[1])
stationTemps = maxTemps.map(lambda x:(x[0], x[2]))
#minTemps = stationTemps.reduceByKey(lambda x,y : min(x,y))
maxTemps = stationTemps.reduceByKey(lambda x,y : max(x,y))
results = maxTemps.collect();

for result in results:
    print(result[0] + "\t{:.2f}F".format(result[1]))