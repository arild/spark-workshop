package workshop.part2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest._
import workshop.util.SparkTestUtils
import workshop.util.parser.{AccessLogParser, HttpStatusCode}

class LogAnalyzerSqlTest extends SparkTestUtils with Matchers {

  sparkTest("count stuff") {
    val file: RDD[String] = sc.textFile("access_log")
    val dataFrame: DataFrame = openLogFile(file)
    LogAnalyzerSql.countStatus200Loglines(dataFrame) shouldBe 1274
    println(LogAnalyzerSql.countLoglinesByStatuscodes(dataFrame))
    println(LogAnalyzerSql.sumBytesPerRequest(dataFrame))
  }

  def openLogFile(rdd: RDD[String]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    openHttpStatusFile(sqlContext)

    val df = rdd
      .map(AccessLogParser.parseRecord)
      .toDF()
    df.registerTempTable("logs")
    df
  }

  def openHttpStatusFile(sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    val lines: RDD[Array[String]] = sc.textFile("src/test/resources/http_status_codes.csv")
      .map(line => line.split(",").map(_.trim))

    val header: Array[String] = lines.first()
    val data: RDD[Array[String]] = lines.filter(_(0) != header(0))
    data
      .map(x => HttpStatusCode(x(0).toInt, x(1), x(2)))
      .toDF()
      .registerTempTable("http_status")
  }
}
