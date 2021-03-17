package scalaspark

object wordcount {
  
  import org.apache.spark._
  import org.apache.log4j._
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var sc = new SparkContext("local[*]", "WordCount")
    
    var input = sc.textFile("data/book.txt")
    
    val words = input.flatMap(x => x.split(" "))
    
    val wordCounts = words.countByValue()
    
    wordCounts.foreach(println) 
    
  }
  
}