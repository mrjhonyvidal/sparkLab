import org.apache.spark.sql.SparkSession

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
