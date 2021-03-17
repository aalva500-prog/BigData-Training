package scalaspark

import org.apache.spark.sql._
import org.apache.log4j._
import org.apache.spark.sql.types.{IntegerType, LongType, StringType, StructType}  

object RatingsDataset {
  
  case class UserRatings(userID: Int, movieID: Int, rating: Int, timestamp: Long)  
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    // Create a Spark Session
    val spark =SparkSession.builder.appName("Ratings").master("local[*]").getOrCreate()
    
    //Create a schema
    val userRatingsSchema = new StructType()
    .add("userID", IntegerType, nullable=true)
    .add("movieID", IntegerType, nullable=true)
    .add("rating", IntegerType, nullable=true)
    .add("timestamp", LongType, nullable=true)
    
    import spark.implicits._
    val ratingsDS = spark.read
        .option("sep", "\t").schema(userRatingsSchema)
        .csv("data/ml-100k/u.data")
        .as[UserRatings]
    
    val ratings = ratingsDS.select("rating")
    
    val results = ratings.groupBy("rating").count()
    
    val sortedResults = results.sort("count")
    
    sortedResults.show()
        
  }
  
}