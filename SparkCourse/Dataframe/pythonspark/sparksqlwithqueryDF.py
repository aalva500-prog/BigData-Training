# -*- coding: utf-8 -*-
"""
Created on Tue Feb 23 09:25:41 2021

@author: Aaron Alvarez
"""
from pyspark.sql import SparkSession
from pyspark.sql import Row

spark = SparkSession.builder.appName("SparkSQL").getOrCreate()

def mapper(line):
    fields = line.split(',')
    return Row(ID=int(fields[0]), name= str(fields[1].encode("utf-8")), age=int(fields[2]), numFriends=int(fields[3]))

lines = spark.sparkContext.textFile("fakefriends.csv")
people = lines.map(mapper) # This is an RDD

schemaPeople = spark.createDataFrame(people).cache() # Create a schema out of an RDD. The cache function is an optimization technique.
schemaPeople.createOrReplaceTempView("people") # Displays a dataframe in a form of a table. It's a temporary view that terminates when the session stops, meaning that is session scope.

# Create SQL query
teenagers = spark.sql("SELECT * FROM people WHERE age >= 13 AND age <= 19") # This can be done only in a dataframe, it won't work with an rdd

# Get people above the age of 40
adults = spark.sql("SELECT * FROM people WHERE age > 41")

# Get people (all age groups) with friends less than 50
friends50 = spark.sql("SELECT * FROM people WHERE numFriends < 50")

# Get people (all age groups) with friends more than 300
friends300 = spark.sql("SELECT * FROM people WHERE numFriends > 300")

#Get people (all age groups) with the most number of friends
maxFriends = spark.sql("SELECT name, numFriends FROM people ORDER BY numFriends DESC LIMIT 5")

#Collect data from the sql query
print("Teenagers:\n")
for teen in teenagers.collect():
    print(teen)
    
print("People above the age of 40:\n")
for adult in adults.collect():
    print(adult)
    
print("People with less than 50 friends:\n")
for person in friends50.collect():
    print(person)
    
print("People with more than 300 friends:\n")
for person in friends300.collect():
    print(person)
    
print("People with the most friends:\n")
for persons in maxFriends.collect():
    print(persons)
    
schemaPeople.groupBy("age").count().orderBy("age").show()

spark.stop()