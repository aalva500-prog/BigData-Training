package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}


object MostAttendees {
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder
    .appName("Match with most attendees")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/world_cup_matches.csv")
             
    val matches = ds.select("Year", "Stadium", "City", "Attendance")
    
    val sortedMatches = matches.orderBy(col("Attendance").desc)
    
    println("The match with the biggest attendance was:")
    sortedMatches.show(1)
  }
  
}