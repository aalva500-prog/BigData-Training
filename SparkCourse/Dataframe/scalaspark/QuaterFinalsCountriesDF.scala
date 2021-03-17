package scalaspark

import org.apache.spark._
import org.apache.log4j._
  

object QuaterFinalsCountries {
  
  def parseLine(line: String):(String, String, String, String) = {
    val fields = line.split(",")
    val year = fields(0)
    val stage = fields(2)
    val hTeam = fields(5)
    val aTeam = fields(8)
    (year, stage, hTeam, aTeam)
  }
  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    var sc = new SparkContext("local[*]", "Quaterfinal Countries")
    
    val input = sc.textFile("data/world_cup_matches.csv")
    
    val parsedLines = input.map(parseLine)
    
    val quaterFinalsTeams = parsedLines.filter(x => x._2 == "Quarter-finals")
    
    val result = quaterFinalsTeams.collect()
    
    println("The teams that have reached the quarter-final stage are:")
    
    result.foreach(println)
   
  }
  
}