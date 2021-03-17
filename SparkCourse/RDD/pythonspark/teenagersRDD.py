# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 13:21:40 2021

@author: Aaron Alvarez

Purpose: From the age 13-19 get the number of friends of each age group
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
teenagers = parsedLines.filter(lambda x: (x[0] >= 13 and x[0]<=19))
totalsByAge = teenagers.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))
results = totalsByAge.collect()

for result in results:
    print(result)