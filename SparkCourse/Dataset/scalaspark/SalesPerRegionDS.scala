package scalaspark

import org.apache.spark._
import org.apache.log4j._
import scala.math.max
import org.apache.spark.sql.functions._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{IntegerType, StringType, LongType, StructType, FloatType}

object SalesPerRegion {
  
  case class Sale(region:String, country:String, item_type:String, channel:String,
                  priority:String, order_date:String, order_ID:String,
                  shipDate:String, unitsSold:Int, unitPrice:Float,
                  unitCost:Float, t_revenue:Float, t_cost:Float, t_profit:Float)
                  
  def main(args: Array[String]){
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = SparkSession.builder
    .appName("Highest Unit Cost")
    .master("local[*]")
    .getOrCreate()
    
    val salesSchema = new StructType()
        .add("region", StringType, nullable=true)
        .add("country", StringType, nullable=true)
        .add("item_type", StringType, nullable=true)
        .add("channel", StringType, nullable=true)
        .add("priority", StringType, nullable=true)
        .add("order_date", StringType, nullable=true)
        .add("order_ID", StringType, nullable=true)
        .add("shipDate", StringType, nullable=true)
        .add("unitsSold", IntegerType, nullable=true)
        .add("unitPrice", FloatType, nullable=true)
        .add("unitCost", FloatType, nullable=true)
        .add("t_revenue", FloatType, nullable=true)
        .add("t_cost", FloatType, nullable=true)
        .add("t_profit", FloatType, nullable=true)
        
    import spark.implicits._    
        
   // Generate dataset
    val ds = spark.read.option("header","true")
      .schema(salesSchema)
      .csv("data/sales.csv")
      .as[Sale]
    
    // Select the columns needed in this exercise
    val sales = ds.select("region", "unitsSold", "t_revenue")
    
    //Get Units sold per region
    val unitsSoldPerRegion = ds.groupBy("region").sum("unitsSold")
    
    // Sort the regions by units sold
    val sortedUnitsSoldPerRegion = unitsSoldPerRegion.sort(col("sum(unitsSold)").desc)
    
    // Get total revenue per region
    val revenuePerRegion = ds.groupBy("region").sum("t_revenue")
    
    // Sort the regions by total revenue
    val sortedRegionsByRevenue = revenuePerRegion.sort(col("sum(t_revenue)").desc)
    
    //Show Units Sold Per Region
    println("The Units Sold Per Region are shown below:")
    sortedUnitsSoldPerRegion.show()
    
    //Show Units Sold Per Region
    println("The Total Revenue Per Region is shown below:")
    sortedRegionsByRevenue.show()
  }
}