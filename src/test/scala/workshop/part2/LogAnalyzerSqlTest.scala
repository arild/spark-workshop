package workshop.part2

import org.apache.spark.sql.DataFrame
import org.scalatest._
import workshop.util.SparkTestUtils

class LogAnalyzerSqlTest extends SparkTestUtils with Matchers {


  sparkTest("creates two tables") {
    val numTables = tables().sqlContext.sql("SHOW TABLES").collect().length
    numTables shouldBe 2
  }

  sparkTest("count status 200 log lines") {
    LogAnalyzerSql.countStatus200Loglines(tables()) shouldBe 6
  }

  sparkTest("find three most frequent ip addresses") {
    val ips = LogAnalyzerSql.findThreeMostFrequentIpAddresses(tables())

    ips.length shouldBe 3
    ips should contain ("77.241.224.111")
    ips should contain ("2.148.3.1")
    ips should contain ("2.148.3.42")
  }

  sparkTest("find request with largest average response size") {
    val (request, avgResponseSize) = LogAnalyzerSql.findRequestWithLargestAverageResponseSize(tables())

    request shouldBe "PUT /api/users HTTP/1.1"
    avgResponseSize shouldBe 9658.0
  }

  sparkTest("find description for all unique status codes") {
    val res = LogAnalyzerSql.findDescriptionForAllUniqueStatusCodes(tables())

    res.get(200) shouldBe Some("OK")
    res.get(302) shouldBe Some("Found")
    res.get(404) shouldBe Some("Not Found")
  }

  def tables(): DataFrame = {
    LogAnalyzerSql.createTables(sc, "src/test/resources/access_log-1", "src/test/resources/http_status_codes.csv")
  }
}
