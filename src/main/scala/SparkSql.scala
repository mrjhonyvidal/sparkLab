import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.functions._

object SparkSql extends App{

  val spark = SparkSession.builder()
    .appName("Spark SQL Practice")
    .config("spark.master", "local")
    //.config("spark.sql.warehouse.dir", "src/main/resources/warehouse")
    //.config("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation", "true")
    .getOrCreate()


  val carsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/cars.json")

  // regular Data Frame API
  carsDF.select(col("Name")).where(col("Origin") === "USA")

  // use Spark SQL
  carsDF.createOrReplaceTempView("cars")
  val americanCarsDF = spark.sql(
    """
      |select Name from cars where Origin = 'USA'
      |""".stripMargin
  )

  // we acn run ANY SQL statemetn
  spark.sql("create database scalalab")
  spark.sql("use scalalab")
  val databaseDF = spark.sql("show databases")

  // transfer tables from a DB to Spark tables
  def readTable(tableName: String) = spark.read
    .format("jdbc")
    .option("driver", PostgresDBConnection.getDriver)
    .option("url", PostgresDBConnection.getUrl)
    .option("user", PostgresDBConnection.getUser)
    .option("password", PostgresDBConnection.getPassword)
    .option("dbtable", s"public.$tableName")
    .load()

  def transferTables(tableNames: List[String], shouldWriteToWarehouse: Boolean = false) = tableNames.foreach { tableName =>
    val tableDF = readTable(tableName)
    tableDF.createOrReplaceTempView(tableName)

    if ( shouldWriteToWarehouse ){
      tableDF.write
        .mode(SaveMode.Overwrite)
        .saveAsTable(tableName)
    }
  }

  transferTables(List(
    "employees",
    "departments",
    "titles",
    "dept_emp",
    "salaries",
    "dept_manager")
  )

  // Read DF from loaded Spark tables
  val employeesDF2 = spark.read.table("employees")

  // Read movies DF and store it as Spark table in the scalalab database
  val moviesDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/movies.json")

  moviesDF.write
    .mode(SaveMode.Overwrite)
    .saveAsTable("movies")

  // Count how many employees were hired in between Jan 1 1999 and Jan 1 2000
  spark.sql(
    """
      |select count(*)
      |from employees
      |where hire_date > '1999-01-01' and hire_date < '2000-01-01'
      |""".stripMargin
  )

  // Show the average salaries for employees hired in between those dates, grouped by department.
  spark.sql(
    """
      |select de.dept_no, avg(s.salary)
      |from employees e, dept_emp de, salaries s
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      |group by de.dept_no
      |""".stripMargin
  )

  // Show the name of best-paying department for employees hired between those dates
  spark.sql(
    """
      |select avg(s.salary) payments, d.dept_name
      |from employees e, dept_emp de, salaries s, departments d
      |where e.hire_date > '1999-01-01' and e.hire_date < '2000-01-01'
      | and e.emp_no = de.emp_no
      | and e.emp_no = s.emp_no
      | and de.dept_no = d.dept_no
      |group by d.dept_name
      |order by payments desc
      |limit 1
    """.stripMargin
  ).show()
}
