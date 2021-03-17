package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}


object MostRunnerUp {
  def main(args: Array[String]){
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val spark = SparkSession.builder
    .appName("Countries runners-up")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/world_cups.csv")
             
   val runnersUp = ds.select("Runners-Up")
   
   val countRunnersUp = runnersUp.groupBy("Runners-Up").count()
   
   val sortedRunnersUp = countRunnersUp.orderBy(col("count").desc)
   
   println("The country who has been the most runner up:")
   sortedRunnersUp.show(1)
             
  }
  
}