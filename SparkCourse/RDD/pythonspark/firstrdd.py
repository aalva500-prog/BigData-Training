# -*- coding: utf-8 -*-
"""
Created on Thu Feb 18 12:15:07 2021

@author: Administrator
"""

from pyspark import SparkContext, SparkConf
import collections

conf = SparkConf().setMaster("local").setAppName("creting an RDD")
sc =  SparkContext(conf=conf)
    
# Input data
#data = [1,2,3,4,5]
    
# Create RDD
# Te first thing is to use a parallelize method
#rdd1 = sc.parallelize(data)

# Collect data from RDD
#newrdd = rdd1.collect()

# Print RDD
#print(newrdd)


rdd = sc.textFile("book.txt")

list_elem = rdd.collect()

for elem in list_elem:
    cleanword = elem.encode('ascii', 'ignore')
    if(cleanword):
        print(cleanword.decode())

    
    