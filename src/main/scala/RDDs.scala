import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

import scala.io.Source

object RDDs extends App {

 val spark = SparkSession.builder()
   .appName("SparkLab into RDDs")
   .config("spark.master", "local")
   .getOrCreate()

 // SparkContext is the entry point to low-level APIs, including RDDs
 val sc = spark.sparkContext

 // 1 - parallelize an existing collection
 val numbers = 1 to 1000000
 val numbersRDD = sc.parallelize(numbers)

 // 2 - reading from files
 case class StockValue(symbol: String, date: String, price: Double)

 def readStocks(filename: String) =
  Source.fromFile(filename)
    .getLines()
    .drop(1)
    .map(line => line.split(','))
    .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))
    .toList

 val stocksRDD = sc.parallelize(readStocks("src/main/resources/data/stocks.csv"))

 // 2b - reading from files
 val stocksRDD2 = sc.textFile("src/main/resources/data/stocks.csv")
   .map(line => line.split(","))
   .filter(tokens => tokens(0).toUpperCase() == tokens(0))
   .map(tokens => StockValue(tokens(0), tokens(1), tokens(2).toDouble))

 // 3 - read from a DataFrame
 val stocksDF = spark.read
   .option("header", "true")
   .option("inferSchema", "true")
   .csv("src/main/resources/data/stocks.csv")

 // DataSet

 import spark.implicits._

 val stocksDS = stocksDF.as[StockValue]
 val stocksRDD3 = stocksDS.rdd

 // RDD -> DataFrame
 val numbersDF = numbersRDD.toDF("numbers") // this way we don't have the type info

 // RDD -> DataSet
 val numbersDS = spark.createDataset(numbersRDD) // Include TypeInfo

 // Transformations

 // distinct
 val msftRDD = stocksRDD.filter(_.symbol == "MSFT") // lazy transformation
 val msCount = msftRDD.count() // eager ACTION - trigger computation

 // counting
 val companyNamesRDD = stocksRDD.map(_.symbol).distinct() // Lazy Evaluation

 // min and max
 implicit val stockOrdering: Ordering[StockValue] =
  Ordering.fromLessThan[StockValue]((stockA: StockValue, stockB: StockValue) => stockA.price < stockB.price)
 val minMicrosoftStocks = msftRDD.min() // ACTION - trigger computation

 // reduce
 numbersRDD.reduce(_ + _)

 // grouping
 val groupedStocksRDD = stocksRDD.groupBy(_.symbol)
 // Note: very expensive operation

 // Partitioning

 val repartitionedStocksRDD = stocksRDD.repartition(30)
 repartitionedStocksRDD.toDF.write
   .mode(SaveMode.Overwrite)
   .parquet("src/main/resources/data/stocks30")
 /**
  * Notes: Repartitioning is EXPENSIVE. Involves Shuffling.
  * Best pracites: partition EARLY, then process that.
  * Size of a partition 10-100MB.
  */

 // coalesce
 val coalesceRDD = repartitionedStocksRDD.coalesce(15) // does not involve shuffling
 coalesceRDD.toDF.write
   .mode(SaveMode.Overwrite)
   .parquet("src/main/resources/data/stocks15")


 case class Movie(title: String, genre: String, rating: Double)
 // Read movies.json as an RDD.
 val moviesDF = spark.read
   .option("inferschema", "true")
   .json("src/main/resources/data/movies.json")

 val moviesRDD = moviesDF
   .select(col("Title").as("Title"), col("Major_Genre").as("Genre"), col("IMDB_Rating").as("rating"))
   .where(col("genre").isNotNull and col("rating").isNotNull)
   .as[Movie]
   .rdd

 // Show the distinct genres as an RDD
 val genresRDD = moviesRDD.map(_.genre).distinct()

 // Select all the movies in the Drama genre with IMDB rating > 6.
 val goodDramaRDD = moviesRDD.filter(movie => movie.genre == "Drama" && movie.rating > 6)

 // Show the average rating of movies by genre.
 case class GenreAvgRating(genre: String, rating: Double)

 val avgRatingByGenreRDD = moviesRDD.groupBy(_.genre).map {
  case (genre, movies) => GenreAvgRating(genre, movies.map(_.rating).sum / movies.size)
 }

 avgRatingByGenreRDD.toDF.show
 moviesRDD.toDF.groupBy(col("genre")).avg("rating").show

 /*
  reference:
    +-------------------+------------------+
    |              genre|       avg(rating)|
    +-------------------+------------------+
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |        Documentary| 6.997297297297298|
    |       Black Comedy|6.8187500000000005|
    |  Thriller/Suspense| 6.360944206008582|
    |            Musical|             6.448|
    |    Romantic Comedy| 5.873076923076922|
    |Concert/Performance|             6.325|
    |             Horror|5.6760765550239185|
    |            Western| 6.842857142857142|
    |             Comedy| 5.853858267716529|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
  RDD:
    +-------------------+------------------+
    |              genre|            rating|
    +-------------------+------------------+
    |Concert/Performance|             6.325|
    |            Western| 6.842857142857142|
    |            Musical|             6.448|
    |             Horror|5.6760765550239185|
    |    Romantic Comedy| 5.873076923076922|
    |             Comedy| 5.853858267716529|
    |       Black Comedy|6.8187500000000005|
    |        Documentary| 6.997297297297298|
    |          Adventure| 6.345019920318729|
    |              Drama| 6.773441734417339|
    |  Thriller/Suspense| 6.360944206008582|
    |             Action| 6.114795918367349|
    +-------------------+------------------+
 */
}
