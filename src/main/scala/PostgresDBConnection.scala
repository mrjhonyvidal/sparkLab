trait DBConnection {
  def getDriver: String
  def getUrl: String
  def getUser: String
  def getPassword: String
}

object PostgresDBConnection extends DBConnection {

  private val DB_DRIVER = "org.postgresql.Driver"
  private val DB_URL = "jdbc:postgresql://localhost:5432/sparklab"
  private val DB_USER = "docker"
  private val DB_PASSWORD = "docker"

  override def getDriver: String = DB_DRIVER
  override def getUrl: String = DB_URL
  override def getUser: String = DB_USER
  override def getPassword: String = DB_PASSWORD
}
