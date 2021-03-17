# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 08:10:25 2021

@author: Administrator
"""

# Count the number of words in text file (using flatmap)
# Difference between map and flatmap: map repeats and flatmaps no
# Flatmap is trying to get the dataset in a sequence ( it flattens the elements to whatever you wish to)
# Flatmap desintegrate the elements and produces an arbitrary number

from pyspark import SparkConf, SparkContext
import collections

# Configuration and set the Spark cluster and App Name
conf = SparkConf().setMaster("local").setAppName("Word Count")
sc= SparkContext(conf = conf)

rddone = sc.textFile("file:///SparkCourse/book.txt")
words = rddone.flatMap(lambda x: x.split()) # Split each line into words (transform)
result = words.countByValue() # action

for word, count in result.items():
    cleanword = word.encode('ascii', 'ignore')
    if(cleanword):
        print(cleanword.decode() + " " + str(count))

print(result)
