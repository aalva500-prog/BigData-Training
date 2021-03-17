package scalaspark
import org.apache.spark._
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{IntegerType,LongType, StructType,StringType,FloatType}


object CitiesWithMostGames {
  
  def main(args: Array[String]){
        Logger.getLogger("org").setLevel(Level.ERROR)       
        
        var sc = new SparkContext("local[*]", "City")
        
        val spark = SparkSession.builder.appName("City with Most GAmes").master("local[*]").getOrCreate()
       
       import spark.implicits._
       
       val ds = spark.read.option("header", "true")
                     .option("inferSchema", "true")
                     .csv("data/world_cup_matches.csv")
       
       val gamesPerCity = ds.select("City").groupBy("City").count()
       
       val gamesSorted = gamesPerCity.sort(desc("count"))
       
       gamesSorted.show()       
    
  }
}