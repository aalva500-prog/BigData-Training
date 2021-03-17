# -*- coding: utf-8 -*-
"""
Created on Wed Mar  3 08:15:27 2021

@author: Aaron Alvarez
"""

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.sql import Row, SparkSession

from pyspark.sql.functions import regexp_extract
import pyspark.sql.functions as func


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

logsDF2 = logsDF.withColumn("eventTime", func.current_timestamp())

# Keeping a runing count of end points

endpointsCounts = logsDF2.groupBy(func.window(func.col("eventTime"), "30 seconds", "10 seconds"), func.col("endpoint")).count()

sortedEndPointCounts = endpointsCounts.orderBy(func.col("count").desc())


#display the stream to the console

query = sortedEndPointCounts.writeStream.outputMode("complete").format("console").queryName("counts").start()


#wait until we terminate
query.awaitTermination()

spark.stop()