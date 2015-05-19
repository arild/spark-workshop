package workshop.part2

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.scalatest._
import workshop.util.SparkTestUtils
import workshop.util.parser.{AccessLogParser, HttpStatusCode}

class LogAnalyzerSqlTest extends SparkTestUtils with Matchers {

  val ACCESS_LOG_1 = "src/test/resources/access_log-1"

  sparkTest("count status 200 log lines") {
    LogAnalyzerSql.countStatus200Loglines(openLogFile()) shouldBe 6
  }

  sparkTest("find three most frequent ip addresses") {
    val ips = LogAnalyzerSql.findThreeMostFrequentIpAddresses(openLogFile())

    ips.length shouldBe 3
    ips should contain ("77.241.224.111")
    ips should contain ("2.148.3.1")
  }

  sparkTest("find request with largest average response size") {
    val (request, avgResponseSize) = LogAnalyzerSql.findRequestWithLargestAverageResponseSize(openLogFile())

    request shouldBe "PUT /api/users HTTP/1.1"
    avgResponseSize shouldBe 9658.0
  }

  sparkTest("find description for all unique status codes") {
    val res = LogAnalyzerSql.findDescriptionForAllUniqueStatusCodes(openLogFile())

    res.get(200) shouldBe Some("OK")
    res.get(302) shouldBe Some("Found")
    res.get(404) shouldBe Some("Not Found")
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
