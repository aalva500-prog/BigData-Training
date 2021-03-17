package scalaspark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,LongType, StructType,StringType,FloatType}

  

object NeverQuaterFinals {
  
  def main(args: Array[String]){
        Logger.getLogger("org").setLevel(Level.ERROR)
        var sc = new SparkContext("local[*]", "Never QuaterFinals")
        val spark = SparkSession.builder.appName("Never Quater Finals Teams").master("local[*]").getOrCreate()
       
       import spark.implicits._
       val ds = spark.read.option("header", "true")
                 .option("inferSchema", "true")
                 .csv("data/world_cup_matches.csv")
                 
       val teams = ds.select("Stage","Home Team Name","Away Team Name")
       
       // Filter No Quarter Finals Teams
       val noQuarterFinalsTeams = teams.filter(teams("Stage") !== "Quarter-finals")
       
       // Filter No Finals Teams
       val noFinals = noQuarterFinalsTeams.filter(teams("Stage") !== "Final")
       
       // Filter No Semi-Finals
       val noSemiFinals = noFinals.filter(teams("Stage") !== "Semi-finals")
       
       // No Match for third place
       val noThirdPlace = noSemiFinals.filter(teams("Stage") !== "Match for third place")
       
       // Select home teams
       val homeTeams = noThirdPlace.select("Home Team Name")
       
       // Select away teams
       val awayTeams = noThirdPlace.select("Away Team Name")       
       
       // Get the distinct values from Home and Away teams
       val differentTeams = homeTeams.union(awayTeams).distinct().collect()      
       
       differentTeams.foreach(println)    
    
  }
  
}