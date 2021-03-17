package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}


object HomeTeamMostWorldCups {
  
  def main(args: Array[String]){
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder
    .appName("Countries with most World Cups")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/world_cup_matches.csv")  
   
   val semiFilter = ds.filter($"Stage" === "Final")
   
   val filterHomeTeamVictory = semiFilter.filter($"Home Team Won" === "HomeTeam")
   
   val countHomeTeamVictories = filterHomeTeamVictory.count()
   
   println(s"The Home Team has won the World Cup: $countHomeTeamVictories times")
   
   val yearsHomeTeamWon = filterHomeTeamVictory.select("Year", "Home Team Name")
   
   println("The years the Home Team Won:")
   yearsHomeTeamWon.show()
  
  
  }
  
}