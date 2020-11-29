package playground

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types._

object Playground extends App {

  /**
   * Creates a SparkSession to operate on DataFrames
   */
  val spark = SparkSession.builder()
    .appName("Spark Lab Playground")
    .config("spark.master", "local")
    .getOrCreate()

  val sparkContext = spark.sparkContext
}