import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DoubleType, LongType, StringType, StructField, StructType}

/**
 * DataFrames - Distributed spreadsheets with rows and columns
 * Immutable
 *  - can't be changed once created
 *  - create other DFs via transformation
 *
 *  Transformations
 *  - narrow = one input partition contributes to at most one output partition(example: map)
 *  - wide = input partitions (one or more) create many output partitions (sort)
 *
 *  Shuffle = data exchange between cluster nodes
 *  - occurs in wide transformation
 *  - massive perf topic
 *
 * Computing Data Frames
 * - Lazy Evaluation
 *  Spark waits until the last moment to execute the DF Transformation
 *
 * Planning
 * - Spark compiles the DF transformation into a graph before running any code
 *  - Logical plan = DF dependency graph + narrow/wide transformations sequence
 *  - physical plan = optimized sequence of steps for nodes in the cluster
 *  optimizations
 *
 *  Transformations vs Actions
 *  - Transformations describe how new DFs are obtained
 *  - actions actually start executing Spark code (example: .show(), .take() -> Spark evaluates)
 *
 * Schema = list describing the column names and types
 *  - types known to Spark, not at compile time (it can be set with TypeSafe types if desired
 *  arbitrary number of columns
 *  all rows have the same structure
 *
 *  Need to be distributed
 *  - data too big for a single computer
 *  - too long to process the entire data on a single CPU
 *
 *  Partitioning
 *  - splits the data into files, distributed between nodes in the cluster
 *  - impacts the processing parallelism
 */
object DataFramesBasics extends App{

  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master","local")
    .getOrCreate()

  val firstDataFrame = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")

  firstDataFrame.show()
  firstDataFrame.printSchema()

  //Get rows
  firstDataFrame.take(10).foreach(println)

  // Spark types
  val longType = LongType

  val carsSchema = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", StringType),
    StructField("Origin", StringType)
  ))

  val carsDFSSchema = firstDataFrame.schema

  // Read a DataFrame with our schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSSchema)
    .load("src/main/resources/data/cars.json")

  val carRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // Create a DataFrame from tuples
  val cars = Seq(
    ("buick skylark 320",15.0,8L,350.0,165L,3693L,11.5,"1970-01-01","USA"),
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("plymouth satellite",18.0,8L,318.0,150L,3436L,11.0,"1970-01-01","USA"),
    ("amc rebel sst",16.0,8L,304.0,150L,3433L,12.0,"1970-01-01","USA"),
    ("ford torino",17.0,8L,302.0,140L,3449L,10.5,"1970-01-01","USA"),
    ("ford galaxie 500",15.0,8L,429.0,198L,4341L,10.0,"1970-01-01","USA"),
    ("chevrolet impala",14.0,8L,454.0,220L,4354L,9.0,"1970-01-01","USA"),
    ("plymouth fury iii",14.0,8L,440.0,215L,4312L,8.5,"1970-01-01","USA"),
    ("pontiac catalina",14.0,8L,455.0,225L,4425L,10.0,"1970-01-01","USA"),
    ("amc ambassador dpl",15.0,8L,390.0,190L,3850L,8.5,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred

  // note: DFs have schemas, rows do not

  // create DFs with implicits
  import spark.implicits._
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylinders", "Displacements", "HP", "Weight", "Acceleration", "Year", "CountryOrigin")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()
  manualCarsDFWithImplicits.show()
  println(s"Total of rows in Data Frame: ${manualCarsDFWithImplicits.count()}")
}
