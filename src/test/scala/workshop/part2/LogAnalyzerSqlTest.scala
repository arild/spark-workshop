package workshop.part2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest._
import workshop.util.SparkTestUtils
import workshop.util.parser.{AccessLogParser, HttpStatusCode}

class LogAnalyzerSqlTest extends SparkTestUtils with Matchers {

  val ACCESS_LOG_1 = "src/test/resources/access_log-1"

  sparkTest("count status 200 log lines") {
    val dataFrame: DataFrame = openLogFile()
    LogAnalyzerSql.countStatus200Loglines(dataFrame) shouldBe 6
  }

  sparkTest("find three most frequent ip addresses") {
    val dataFrame: DataFrame = openLogFile()
    val ips = LogAnalyzerSql.findThreeMostFrequentIpAddresses(dataFrame)

    ips.length shouldBe 3
    ips should contain ("77.241.224.111")
    ips should contain ("2.148.3.1")
  }

  sparkTest("find request with largest average response size") {
    val dataFrame: DataFrame = openLogFile()
    val (request, avgResponseSize) = LogAnalyzerSql.findRequestWithLargestAverageResponseSize(dataFrame)

    request shouldBe "PUT /api/users HTTP/1.1"
    avgResponseSize shouldBe 9658.0
  }

  def openLogFile(): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    openHttpStatusFile(sqlContext)

    val df = sc.textFile(ACCESS_LOG_1)
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
