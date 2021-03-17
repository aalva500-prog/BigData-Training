package scalaspark

import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,LongType, StructType,StringType,FloatType}

object QuaterFinalTeams {
  
  def main(args: Array[String]){
        Logger.getLogger("org").setLevel(Level.ERROR)
        var sc = new SparkContext("local[*]", "QuaterFinals")
        val spark = SparkSession.builder.appName("Quater Finals Teams").master("local[*]").getOrCreate()
       
       import spark.implicits._
       val ds = spark.read.option("header", "true")
                 .option("inferSchema", "true")
                 .csv("data/world_cup_matches.csv")
                 
       val teams = ds.select("Stage","Home Team Name","Away Team Name")
       
       // Filter Quarter Finals Teams
       val quaterFinalsTemas = teams.filter(teams("Stage") === "Quarter-finals")
       
       // Select home teams
       val homeTeams = quaterFinalsTemas.select("Home Team Name", "Stage")
       
       // Select away teams
       val awayTemas = quaterFinalsTemas.select("Away Team Name", "Stage")       
       
       // Get the distinct values from Home and Away teams
       val diffrentTeams = homeTeams.union(awayTemas).distinct().collect()
       
       diffrentTeams.foreach(println)    
    
  }
}