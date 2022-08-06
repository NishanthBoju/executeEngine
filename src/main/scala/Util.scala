import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession

object Util {

  val spark = SparkSession.builder()
    .master("local[1]")
    .appName("ExecutionEngine")
    .getOrCreate()

  def LogLevel(logType:Level): Unit ={
    Logger.getLogger("org").setLevel(logType)
  }

  // Create the case classes for our domain
  case class Department(id: String, name: String)
  case class Employee(firstName: String, lastName: String, email: String, salary: Int)
  case class DepartmentWithEmployees(department: Department, employees: Seq[Employee])


}
