# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 12:49:57 2021

@author: Aaron Alvarez

Purpose: Get the number og friends by age
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Friends")
sc= SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    age = int(fields[2])
    friends = int(fields[3])
    return (age, friends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
parsedLines = lines.map(parseLine)
totalsByAge = parsedLines.mapValues(lambda x:(x,1)).reduceByKey(lambda x,y:(x[0]+y[0], x[1]+y[1]))
results = totalsByAge.collect()

for result in results:
    print(result)

