package structuredstreaming

import org.apache.log4j._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

object StructuredStreaming {
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession
        .builder
        .appName("StructuredStreamin")
        .master("local[*]")
        .getOrCreate()
        
    val accessLines = spark.readStream.text("data/logs")
    
    val hostExp = "(^\\S+\\.[\\S+\\.]+\\S+)\\s"
    val timeExp = "\\[(\\d{2}/\\w{3}/\\d{4}:\\d{2}:\\d{2}:\\d{2} - \\d{4})]"
    val statusExp = "\\s(\\d{3})\\s"
    val generalExp = "\"(\\S+)\\s(\\S+)\\s*(\\s*)\""
    val contentSize = "\\s(\\d+)$"
    
    
    val logsDF = accessLines.select(regexp_extract(col("value"), hostExp, 1).alias("host"),
     regexp_extract(col("value"), timeExp, 1).alias("timeStamp"),
     regexp_extract(col("value"), generalExp, 1).alias("method"),
     regexp_extract(col("value"), generalExp, 2).alias("endpoint"),
     regexp_extract(col("value"), generalExp, 3).alias("protocol"),
     regexp_extract(col("value"), statusExp, 1).cast("Integer").alias("status"),
     regexp_extract(col("value"), contentSize, 1).cast("Integer").alias("contentSize"))
                 
    val statusCountsDF = logsDF.groupBy("status").count()
      
    val query = statusCountsDF .writeStream.outputMode("complete").format("console").queryName("counts").start()

    query.awaitTermination()
     
    spark.stop()
     
        
  }
  
}