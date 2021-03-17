package scalaspark
import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object CountryWithMostSemifinals {
  
  def main(args: Array[String]){
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
    
    val spark = SparkSession.builder
    .appName("Countries in most Semifinals")
    .master("local[*]")
    .getOrCreate()
    
    import spark.implicits._
    
    val ds = spark.read.option("header","true")
             .option("inferSchema","true")      
             .csv("data/world_cup_matches.csv")  
   
   val semiFilter = ds.filter($"Stage" === "Semi-finals")   
     
   val countCountriesAway = semiFilter.groupBy("Away Team Name").count()
   
   val countCountriesHome = semiFilter.groupBy("Home Team Name").count()
   
//   val sortedHomeCountries = countCountriesHome.orderBy(col("count").desc)
//   
//   val sortedAwayCountries = countCountriesAway.orderBy(col("count").desc)
   
   val dataset = countCountriesHome.union(countCountriesAway)
   
   val result = dataset.groupBy("Home Team Name")
   
   val suma = result.sum()
   
   val resultSorted = suma.orderBy(col("sum(count)").desc)
   
   println("The country with most semi-finals is:")
   
   resultSorted.show(1)
   
//   println("Country with most semi-finals as Home Team:")
//   sortedHomeCountries.show(1)
//   
//   println("Country with most semi-finals as Away Team:")
//   sortedAwayCountries.show(1)
    
  }
  
}