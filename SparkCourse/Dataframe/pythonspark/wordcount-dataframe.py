# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 11:38:03 2021

@author: Aaron Alvarez
"""



from pyspark.sql import SparkSession
from pyspark.sql import functions as func #import functions as func

spark = SparkSession.builder.appName("WordCount").getOrCreate()

inputDF = spark.read.text("book.txt") # this is a dataframe

# select function is similar to split
# alias functions helps you to create columns out of an RDD
# Use regular expression to split each line with the column named word
# explode explore all the file and splits each line into columns
words = inputDF.select(func.explode(func.split(inputDF.value, "\\W+")).alias("word"))
words.filter(words.word != "") # get rid of white spaces

#convert all words to lowercase
lowercasewords = words.select(func.lower(words.word).alias("word"))
 
# Count the words and group by the column named "word"
wordCounts = lowercasewords.groupBy("word").count()

# Sort the words by count
sortedwords = wordCounts.sort("count")

# Show the output
sortedwords.show(sortedwords.count())