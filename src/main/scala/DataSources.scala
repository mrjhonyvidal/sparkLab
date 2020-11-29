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

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  spark.read
    .format("csv")
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", "true") // Does the file contain a header? So we can skip it
    .option("sep", ",") // Separators
    .option("nullValue", "") // There is no notion of null values en CSV, parse "" as null
    .load("src/main/resources/data/stocks.csv") // .csv() also works

  // Parquet
  // Open source compress binary data storage format optimized for fast reading calls
  // Default storage format for dataframes
  carsDF.write
    .format("parquet") // not necessary to specify as it is the default
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars.parquet")

  // Text files
  spark.read.text("src/main/resources/data/sampleTextFile.txt").show()

  // Reading from a remote DB (Postgres DB can be run after `docker-compose up` in this project)
  val employeesDF = spark.read
    .format("jdbc")
    .option("driver", PostgresDBConnection.getDriver)
    .option("url", PostgresDBConnection.getUrl)
    .option("user", PostgresDBConnection.getUser)
    .option("password", PostgresDBConnection.getPassword)
    .option("dbtable", "public.employees")
    .load()

  employeesDF.show()

  /**
   * Read movies DF and write it as:
   *  - tab-separated value file
   *  - snappy Parquet (compression: snappy)
   *  - table "public.movies" in the PostgresDB
   */
  val moviesDF = spark.read.json("src/main/resources/data/movies.json")

  // TSV
  moviesDF.write
    .format("csv")
    .option("header", "true")
    .option("sep", "\t")
    .save("src/main/resources/data/movies.csv")

  // Parquet
  moviesDF.write.save("src/main/resources/data/movies.parquet")

  // save to DF
  moviesDF.write
    .format("jdbc")
    .option("driver", PostgresDBConnection.getDriver)
    .option("url", PostgresDBConnection.getUrl)
    .option("user", PostgresDBConnection.getUser)
    .option("password", PostgresDBConnection.getPassword)
    .option("dbtable", "public.movies")
    .save()
}
