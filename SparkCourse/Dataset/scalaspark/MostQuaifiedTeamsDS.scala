package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}


object MostQualifiedTeams {
  
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
             
   val mostGoalCountry = ds.select("QualifiedTeams", "Country", "Year")
   
   val sortedList = mostGoalCountry.orderBy(col("QualifiedTeams").desc)
   
      
   println("The world cup with the most qualified teams:")
   sortedList.show(1)
             
  }  
}