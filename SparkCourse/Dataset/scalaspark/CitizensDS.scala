package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object Citizens {
  
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
   
   val citizensFilter = ds.filter($"CitizenDesc" === "US Citizen")
   
   val citizens = citizensFilter.select("Employee_Name", "CitizenDesc")
   
   citizens.show()
             
   val nonCitizensFilter = ds.filter($"CitizenDesc" === "Eligible NonCitizen")
   
   val nonCitizens = nonCitizensFilter.select("Employee_Name", "CitizenDesc")
   
   nonCitizens.show()
   
   val citizensCount = citizens.count()
   
   val nonCitizensCount = nonCitizens.count()
   
   println(s"The total of citizens: $citizensCount")
   println(s"The total of non-citizens: $nonCitizensCount")
    
  }
  
}