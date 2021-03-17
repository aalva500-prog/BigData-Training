package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object CountryWhoWonTheMost {
  
  def main(args: Array[String]){
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder
    .appName("Countries with most World Cups")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/world_cups.csv")
             
   val winners = ds.select("Winner")
   
   val countWinners = winners.groupBy("Winner").count()
   
   val sortWinners = countWinners.orderBy(col("count").desc)
   
   println("The country who has won the most world cups:")
   sortWinners.show(1)
             
  }
  
}