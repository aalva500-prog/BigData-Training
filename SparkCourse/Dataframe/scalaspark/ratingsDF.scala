package scalaspark

object ratings {
  import org.apache.spark._
  import org.apache.log4j._
  
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var sc = new SparkContext("local[*]", "MovieRatings")
    
    val input = sc.textFile("data/ml-100k/u.data")
    
    val ratings = input.map(x => x.split("\t")(1))
    
    val results = ratings.countByValue()
    
    val sortedResults = results.toSeq.sortBy(_._2)
    
    sortedResults.foreach(println)    
  }
}