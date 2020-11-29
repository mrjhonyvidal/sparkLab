import org.apache.spark.sql.SparkSession

object DBOperations {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", PostgresDBConnection.getDriver)
    .option("url", PostgresDBConnection.getUrl)
    .option("user", PostgresDBConnection.getUser)
    .option("password", PostgresDBConnection.getPassword)
    .option("dbtable", s"public.$tableName")
    .load()
}
