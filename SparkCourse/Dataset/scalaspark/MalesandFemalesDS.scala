package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object MalesandFemales {
  
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
   
   val malesFilter = ds.filter($"GenderID" === 1)
   
   val males = malesFilter.select("Employee_Name", "Sex")
   
   males.show()
             
   val femalesFilter = ds.filter($"GenderID" === 0)
   
   val females = femalesFilter.select("Employee_Name", "Sex")
   
   females.show()
   
   val malesCount = males.count()
   
   val femalesCount = females.count()
   
   println(s"The total of men: $malesCount")
   println(s"The total of women: $femalesCount")
    
  }
  
}