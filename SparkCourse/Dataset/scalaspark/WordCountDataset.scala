package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object WordCountDataset {
  
  case class Line(line: String)
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
    .appName("WordCount")
    .master("local[*]")
    .getOrCreate()
    
    val wordSchema = new StructType().add("line", StringType, nullable=true)
    
    import spark.implicits._
    
    val ds = spark.read
      .schema(wordSchema)
      .csv("data/book.txt")
      .as[Line]
    
    val lines = ds.select("line")
    
    val wordsPerLine = lines.withColumn("line", split(col("line"), "\\s+"))
    
    val words = wordsPerLine.select(explode(col("line")) as "word")
    
    val wordCount = words.groupBy("word").count()
    
    val wordsSorted = wordCount.orderBy(col("count").desc)
    
    wordsSorted.show()
  }
  
}