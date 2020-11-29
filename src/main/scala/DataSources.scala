import DataFramesBasics.spark
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object DataSources extends App {

  val spark = SparkSession.builder()
    .appName("Data Source and Formts")
    .config("spark.master", "local")
    .getOrCreate()

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  /**
   * Reading a DF:
   *  - format
   *  - schema or inferSchema = true
   *  - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(carsSchema)
    .option("mode", "failFast") //failFast ThrowException, dropMalformed, permissive(default)
    .option("path", "src/main/resources/data/cars.json")
    .load()

  // alternative reading with options map
  val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))

  /**
   * Writing DFs
   * - format
   * - save mode = overwrite, append, ignore, errorIfExists
   * - path
   * - zero or more options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .option("path", "src/main/resources/data/cars_spark_writing.json")
    .save()

  // JSON flags
  spark.read
    .format("json")
    .schema(carsSchema)
    .option("dateFormat", "YYYY-MM--dd") //note: match with schema; if Spark fails parsing, it will put null
    .option("allowSingleQuotes", "true")
    .option("compression", "uncompressed") //bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")
}
