# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 07:22:05 2021

@author: Administrator
"""

from pyspark import SparkConf, SparkContext
import collections

# Configuration and set the Spark cluster
conf = SparkConf().setMaster("local").setAppName("Ratings")
sc= SparkContext(conf = conf)

# First job is to create an rdd (an rdd is a dataset)
rddtolines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")

# As soon as you hve an rdd yo can start applying transformation
# Apply a map (lambda) function and split the values (iterating over the entire file and get the 3rd column)
# ratings = rddtolines.map(lambda x: x.split()[2]) # transform function
movieIDs = rddtolines.map(lambda y: (y.split()[1], y.split()[2]))

# Apply actions to the data collected
# Collect the data (the collect method prints the data)
# Without the collect method it will be imposible to print the data
# results = ratings.collect() # action
results = movieIDs.countByValue() # action

# Ordering the data, which is obtained as a dictionary
sortedResult = collections.OrderedDict(sorted(results.items()))
# print the data as keys and values
for key, value in sortedResult.items():
    print("%s %i" %(key, value))

#print(results)
