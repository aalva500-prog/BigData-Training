# -*- coding: utf-8 -*-
"""
Created on Mon Feb 22 09:46:29 2021

@author: Aaron Alvarez
"""

from pyspark import SparkConf, SparkContext
import collections

conf = SparkConf().setMaster("local").setAppName("Movie Ratings")
sc= SparkContext(conf = conf)

lines = sc.textFile("file:///SparkCourse/ml-100k/ml-100k/u.data")

movies = lines.map(lambda y: (int(y.split()[1]),1)) # get MovieID

movieCount = movies.reduceByKey(lambda x,y: x+y)

flipped = movieCount.map(lambda xy: (xy[1], xy[0]))
sortedMovies = flipped.sortByKey()

results = sortedMovies.collect()

for result in results:
    print(result)



