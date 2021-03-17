# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 13:40:44 2021

@author: Aaron Alvarez

Purpose: From the ages 40 to 60 determine which age group has the least number of friends
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Teenagers Friends")
sc= SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
parsedLines = lines.map(parseLine)
adults = parsedLines.filter(lambda x: (x[0] >= 40 and x[0] <= 60))
totalsByAge = adults.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))
results = totalsByAge.collect()

for result in results:
    print(result)