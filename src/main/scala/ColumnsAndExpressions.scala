import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{avg, col, column, count, countDistinct, expr, mean, stddev, sum}

object ColumnsAndExpressions extends App {

  val spark = SparkSession.builder()
    .appName("DF Columns and Expressions")
    .config("spark.master", "local")
    .getOrCreate()

  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // Columns
  val firstColumn = carsDF.col("Name")

  // Selecting
  val carNamesDF = carsDF.select(firstColumn)

  // various select methods
  import spark.implicits._
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // Scala Symbol, auto-converted to column
    $"Horsepower", // fancier interpolated string, returns a Column Object
    expr("Origin") // EXPRESSION
  )

  // Select with plain column names
  carsDF.select("Name", "Year")

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2

  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2").as("Weigh_in_kg_2")
  )

  // selectExpr
  val carsWithSelectExprWeightDF = carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2"
  )

  // DF processing

  // adding a column
  val carsWithKg3DF = carsDF.withColumn("Weight_in_kg_3", col("Weight_in_lbs") / 2.2)

  // Renaming a column
  val carsWithColumnRenamed = carsDF.withColumnRenamed("Weight_in_lbs", "Weight in pounds")

  // Careful with column names
  carsWithColumnRenamed.selectExpr("`Weight in pounds`") // Add expression wrapped on backticks `column name with spaces`

  // Remove a column - Retuns a new DataSet with the columns dropped
  carsWithColumnRenamed.drop("Cylinders", "Displacement")

  // Filtering
  val europeanCarsDF = carsDF.filter(col("Origin") =!= "USA")
  val europeanCarsDF2 = carsDF.where(col("Origin") =!= "USA")

  // filtering with expression strings
  val americanCarsDF = carsDF.filter("Origin = 'USA'")

  // chain filters
  val americanPowerfulCarsDF = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val americanPowerfilCarsDF2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCarsDF3 = carsDF.filter("Origin = 'USA' and Horsepower > 150")

  // unioning = adding more rows
  val moreCarsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) // works if the DFs have the same schema

  // distinct values
  val allContriesDF = carsDF.select("Origin").distinct()

  // 1.Read movies DF and select 2 columns.
  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  moviesDF.show()
  val moviesReleaseDF = moviesDF.select("Title", "Release_Date")
  val moviesReleaseDF2 = moviesDF.select(
    moviesDF.col("Title"),
    col("Release_Date"),
    $"Major_Genre",
    expr("IMDB_Rating")
  )
  val moviesReleaseDF3 = moviesDF.selectExpr(
    "Title", "Release_Date"
  )

  //Create another column summing up the total profit or the movie = US_Gross + Worldwide_Gross + DVD sales
  val moviesProfitDF = moviesDF.select(
    col("Title"),
    col("US_Gross"),
    col("Worldwide_Gross"),
    col("US_DVD_Sales"),
    (col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross")
  )

  val moviesProfitDF2 = moviesDF.selectExpr(
    "Title",
    "US_Gross",
    "Worldwide_Gross",
    "US_Gross + Worldwide_Gross + US_DVD_Sales as Total_Gross"
  )

  /**
   * Example of result
   * +--------------------+-----------------+--------------+-----------+----------+-----------+-----------+-----------------+------------+----------------------+----------------+-------------------+--------------------+------------+--------+---------------+
   * |       Creative_Type|         Director|   Distributor|IMDB_Rating|IMDB_Votes|MPAA_Rating|Major_Genre|Production_Budget|Release_Date|Rotten_Tomatoes_Rating|Running_Time_min|             Source|               Title|US_DVD_Sales|US_Gross|Worldwide_Gross|
   * +--------------------+-----------------+--------------+-----------+----------+-----------+-----------+-----------------+------------+----------------------+----------------+-------------------+--------------------+------------+--------+---------------+
   * |                null|             null|      Gramercy|        6.1|      1071|          R|       null|          8000000|   12-Jun-98|                  null|            null|               null|      The Land Girls|        null|  146083|         146083|
   * |                null|             null|        Strand|        6.9|       207|          R|      Drama|           300000|    7-Aug-98|                  null|            null|               null|First Love, Last ...|        null|   10876|          10876|
   * |                null|             null|     Lionsgate|        6.8|       865|       null|     Comedy|           250000|   28-Aug-98|                  null|            null|               null|I Married a Stran...|        null|  203134|         203134|
   * |                null|             null|     Fine Line|       null|      null|       null|     Comedy|           300000|   11-Sep-98|                    13|            null|               null|Let's Talk About Sex|        null|  373615|         373615|
   * |Contemporary Fiction|             null|       Trimark|        3.4|       165|          R|      Drama|          1000000|    9-Oct-98|                    62|            null|Original Screenplay|                Slam|        null| 1009819|        1087521|
   *
  */

  val moviesProfitDF3 = moviesDF.select("Title", "US_Gross", "Worldwide_Gross")
    .withColumn("Total_Gross", col("US_Gross") + col("Worldwide_Gross"))

  // 3
  val atLeastMediocreComediesDF = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy" and col("IMDB_Rating") > 6)

  val comediesDF2 = moviesDF.select("Title", "IMDB_Rating")
    .where(col("Major_Genre") === "Comedy")
    .where(col("IMDB_Rating") > 6)

  val comediesDF3 = moviesDF.select("Title", "IMDB_Rating")
    .where("Major_Genre = 'Comedy' and IMDB_Rating > 6")

  comediesDF3.show
  /***
   * Result example
   * +--------------------+-----------+
   * |               Title|IMDB_Rating|
   * +--------------------+-----------+
   * |I Married a Stran...|        6.8|
   * |24 7: Twenty Four...|        6.9|
   * |          Four Rooms|        6.4|
   * |    The Four Seasons|        7.0|
   * |Ace Ventura: Pet ...|        6.6|
   * |   American Graffiti|        7.6|
   * |          Annie Hall|        8.2|
   * |    Bon Cop, Bad Cop|        6.9|
   * |                 Big|        7.2|
   * |             Bananas|        7.1|
   * |     Blazing Saddles|        7.8|
   * |Bill & Ted's Exce...|        6.7|
   * |        Beetle Juice|        7.3|
   * |Bienvenue chez le...|        7.0|
   * |          Caddyshack|        7.3|
   * |       Casino Royale|        8.0|
   * |   A Christmas Story|        8.0|
   * |            Crooklyn|        6.5|
   * |              Festen|        8.1|
   * |            Clueless|        6.7|
   * +--------------------+-----------+
   *
   */

  /**
   * Data Science
   * In statistics, the standard deviation is a measure of the amount of variation or dispersion of a set of values.
   * A low standard deviation indicates that the values tend to be close to the mean
   * (also called the expected value) of the set, while a high standard deviation indicates that the values
   * are spread out over a wider range.
   *
   * In probability and statistics, the population mean, or expected value, is a measure of the central
   * tendency either of a probability distribution or of the random variable characterized by that distribution.
   * In a discrete probability distribution of a random variable X,
   * the mean is equal to the sum over every possible value weighted by the probability of that value;
   * that is, it is computed by taking the product of each possible value x of X and its probability p(x),
   * and then adding all these products together, giving
   * {\displaystyle \mu =\sum xp(x)....}{\displaystyle \mu =\sum xp(x)....}
   *
   * https://en.wikipedia.org/wiki/Mean
   * https://en.wikipedia.org/wiki/Standard_deviation
   */
  moviesDF.select(
    mean(col("Rotten_Tomatoes_Rating")), // Average or Mean
    stddev(col("Rotten_Tomatoes_Rating")) // Standard Deviation on a scale of 0 to 100
  ).show()

  /**
   * Result
   * +---------------------------+-----------------------------------+
   * |avg(Rotten_Tomatoes_Rating)|stddev_samp(Rotten_Tomatoes_Rating)|
   * +---------------------------+-----------------------------------+
   * |          54.33692373976734|                  28.07659263787602|
   * +---------------------------+-----------------------------------+
   */

  // Grouping and Aggregation
  val countByGenreDF = moviesDF.groupBy(col("Major_Genre")) // includes null
    .avg("IMDB_Rating") // Aggregation
    //.count() // select count(*) from moviesDF group by Major_Genre

  val aggregationsByGenreDF = moviesDF
    .groupBy(col("Major_Genre"))
    .agg(
      count("*").as("N_Movies"),
      avg("IMDB_Rating").as("Avg_Rating")
    )
    .orderBy(col("Avg_Rating"))


  // sum up all the profits
  moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + col("US_DVD_Sales")).as("Total_Gross"))
    .select(sum("Total_Gross"))

  // Count how many distinct directors we have
  moviesDF
    .select(countDistinct(col("Director")))
    //.show()

  // Show the mean and standard deviations of US gross revenue for the movies
  moviesDF
    .select(
      mean("US_Gross"),
      stddev("US_Gross")
    )
    //.show() // Result x.E7 (10 ^ 7)

  // Compute the average IMDB rating and the average US gross revenue PER DIRECTOR
  moviesDF
    .groupBy("Director")
    .agg(
      avg("IMDB_Rating").as("Avg_Rating"),
      sum("US_Gross").as("Total_US_Gross")
    )
    .orderBy(col("Avg_Rating").desc_nulls_last)
    .show()
}
