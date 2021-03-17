# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 12:49:57 2021

@author: Aaron Alvarez

Purpose: Get the number og friends by age
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Most Friends")
sc= SparkContext(conf = conf)

def parseLine(line):
    fields = line.split(',')
    name = fields[1]
    age = int(fields[2])
    friends = int(fields[3])
    return (name, age, friends)

lines = sc.textFile("file:///SparkCourse/fakefriends.csv")
parsedLines = lines.map(parseLine)
persons = parsedLines.map(lambda x:(x[2], (x[0], x[1])))
sortedbyFriends = persons.sortByKey()
results = sortedbyFriends.collect()

for result in results:
    print(result)

