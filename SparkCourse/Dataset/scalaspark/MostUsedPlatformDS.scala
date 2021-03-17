package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object MostUsedPlatform {
  
  def main(args: Array[String]){
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
    
    val spark = SparkSession.builder
    .appName("Persons")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/hrdataset.csv")    
   
   
    val platforms = ds.select("RecruitmentSource")
    
    val results = platforms.groupBy("RecruitmentSource").count()
    
    val sortedResults = results.sort("count")
    
    sortedResults.show()
    
  }
  
}