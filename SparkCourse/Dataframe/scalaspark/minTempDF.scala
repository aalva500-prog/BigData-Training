package scalaspark

object minTemp {
  import org.apache.spark._
  import org.apache.log4j._
  import scala.math.min
  
  def parseLine(line: String):(String, String, Float) = {
    val fields = line.split(",")
    val stationID = fields(0)
    val entryType = fields(2)
    val temperature = fields(3).toFloat
    (stationID, entryType, temperature)
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var sc = new SparkContext("local[*]", "MinTemp")
    
    val input = sc.textFile("data/1800.csv")
    
    val parsedLines = input.map(parseLine)
    
    val minTemps = parsedLines.filter(x => x._2 == "TMIN")
    
    val stationTemps = minTemps.map(x => (x._1, x._3.toFloat))
        
    val minTempsByStation = stationTemps.reduceByKey((x,y) => min(x,y))
    
    val results = minTempsByStation.collect()
    
    for( result <- results.sorted) {
      val station = result._1
      val temp = result._2
      println(s"$station minimum temperature: $temp")
    }
    
    
  }  
}