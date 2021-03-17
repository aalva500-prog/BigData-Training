package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object ActiveEmployees {
  
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
   
   val employeesFilter = ds.filter($"EmploymentStatus" === "Active")
   
   val activeEmployees = employeesFilter.select("Employee_Name", "EmploymentStatus")
   
   activeEmployees.show()
   
   val activeEmployeesCount = activeEmployees.count()
   
   println(s"The total of active employees: $activeEmployeesCount")
    
  }
  
}