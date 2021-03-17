# -*- coding: utf-8 -*-
"""
Created on Mon Mar  1 09:03:15 2021

@author: Aaron Alvarez
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract


# Exclusively for windows OS
spark = SparkSession.builder.config("spark.sql.warehouse.dir", "file:///c:/temp").appName("StructuredStreaming").getOrCreate()

accessLine = spark.readStream.text("logs") #monitor the logs and reads each access line

hostExp = r'(^\S+\.[\S+\.]+\S+)\s'
timeExp= r'\[(\d{2}/\w{3}/\d{4}:\d{2}:\d{2}:\d{2} -\d{4})]'
statusExp = r'\s(\d{3})\s'
generalExp = r'\"(\S+)\s(\S+)\s*(\S*)\"'
contentSizeExp =r'\s(\d+)$'

logsDF= accessLine.select(regexp_extract('value', hostExp, 1).alias('host'),
                          regexp_extract('value', timeExp, 1).alias('timestamp'),
                          regexp_extract('value', generalExp, 1).alias('method'),
                          regexp_extract('value', generalExp, 2).alias('endpoint'),
                          regexp_extract('value', generalExp, 3).alias('protocol'),
                          regexp_extract('value', statusExp, 1).cast('integer').alias('status'),
                          regexp_extract('value', contentSizeExp, 1).cast('integer').alias('content_size'))
                          
                          
statusCountsDF = logsDF.groupBy(logsDF.method).count()
# keeps a running count of every access by status code
query =(statusCountsDF.writeStream.outputMode("complete").format("console").queryName("counts").start())


query.awaitTermination()


spark.stop()