package scalaspark

object CustomerExpenses {
   import org.apache.spark._
  import org.apache.log4j._  
  
  def parseLine(line: String):(Int, Int, Float) = {
    val fields = line.split(",")
    val customerID = fields(0).toInt
    val articleID = fields(1).toInt
    val moneySpent = fields(2).toFloat
    (customerID, articleID, moneySpent)
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var sc = new SparkContext("local[*]", "Customer Expenses")
    
    val input = sc.textFile("data/customer-orders.csv")
    
    val parsedLines = input.map(parseLine)
    
    val customerMoney = parsedLines.map(x => (x._1.toInt, x._3.toFloat))
    
    val moneyByCustomer = customerMoney.reduceByKey((x,y) => x+y)
    
    val results = moneyByCustomer.collect()
    
    results.sorted.foreach(println)
  }
}