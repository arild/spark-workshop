package workshop.part2

import org.apache.spark.sql.DataFrame
import org.scalatest._
import workshop.util.SparkTestUtils

class LogAnalyzerSqlTest extends SparkTestUtils with Matchers {


  val APP_LOG_1 = "src/test/resources/application-1.log"
  val APP_LOG_2 = "src/test/resources/application-2.log"

  sparkTest("count number of error lines") {
    LogAnalyzerSql.countNumberOfErrors(openLogFile(APP_LOG_1)) should be (2)
  }

  sparkTest("collect distinct info lines") {
    val logLines = LogAnalyzerSql.collectDistinctInfoLines(openLogFile(APP_LOG_1))

    logLines.size should be(6)
    logLines contains "INFO - User id=23 successful login"
  }

  def openLogFile(file: String): DataFrame = {
    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.implicits._

    val df = sc.textFile("src/test/resources/application-1.log")
      .flatMap(LogAnalyzerSql.parseLogLine)
      .toDF()
    df.registerTempTable("loglines")
    df
  }
}
