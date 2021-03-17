package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.min
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object FriendsDataset {
  
  case class Friend(id:Int, name:String, age:Int, numFriends:Int)
  
  def main(args: Array[String]){
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
    .appName("Friends")
    .master("local[*]")
    .getOrCreate()
    
    val friendSchema = new StructType()
        .add("id", IntegerType, nullable=true)
        .add("name", StringType, nullable=true)
        .add("age", IntegerType, nullable=true)
        .add("numFriends", IntegerType, nullable=true)
        
    import spark.implicits._
    
    val ds = spark.read
      .schema(friendSchema)
      .csv("data/fakefriends.csv")
      .as[Friend]
    
    val persons = ds.select("age","numFriends")
    
    val results = persons.groupBy("age").avg("numFriends")
    
    val sortedResults = results.sort("age")
    
    sortedResults.show()   
        
  }
}