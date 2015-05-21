package workshop.part1

import org.apache.spark.rdd.RDD
import org.scalatest._
import workshop.util.SparkTestUtils
import workshop.util.parser.{AccessLogParser, AccessLogRecord}

class LogAnalyzerTest extends SparkTestUtils with Matchers {

  val ACCESS_LOG_1 = "src/test/resources/access_log-1"
  val ACCESS_LOG_2 = "src/test/resources/access_log-2"

  sparkTest("count number of status codes") {
    LogAnalyzer.countNumberOfStatusCode(openAccessLog(ACCESS_LOG_1), 200) should be (6)
    LogAnalyzer.countNumberOfStatusCode(openAccessLog(ACCESS_LOG_1), 404) should be (5)
  }

  sparkTest("collect distinct ip addresses for status code") {
    val ips200 = LogAnalyzer.collectDistinctIpAddressesForStatusCode(openAccessLog(ACCESS_LOG_1), 200)
    ips200.length shouldBe 4
    ips200 should contain("77.241.224.111")
    ips200 should contain("77.241.192.12")
    ips200 should contain("77.241.208.95")
    ips200 should contain("77.241.176.53")

    val ips404 = LogAnalyzer.collectDistinctIpAddressesForStatusCode(openAccessLog(ACCESS_LOG_1), 404)
    ips404.length shouldBe 5
    ips404 should contain("77.241.224.111")
    ips404 should contain("77.241.192.89")
    ips404 should contain("2.148.3.42")
    ips404 should contain("77.241.224.11")
    ips404 should contain("77.241.208.6")
  }

  sparkTest("count all distinct ip addresses for status code") {
    val num200 = LogAnalyzer.countAllDistinctIpAddressesForStatusCode(
      openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2), 200)
    num200 shouldBe 6

    val num404 = LogAnalyzer.countAllDistinctIpAddressesForStatusCode(
      openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2), 404)
    num404 shouldBe 8
  }

  sparkTest("find num ip addresses occurring in both logs") {
    val num = LogAnalyzer.findNumIpAddressesOccurringInBothLogs(openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2))

    num shouldBe 9
  }

  sparkTest("find num ip addresses occurring more than once") {
    val num = LogAnalyzer.findNumIpAddressesOccurringMoreThanOnce(openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2))

    num shouldBe 10
  }

  sparkTest("find three most frequent ip addresses") {
    val ips = LogAnalyzer.findThreeMostFrequentIpAddresses(openAccessLog(ACCESS_LOG_1))

    ips.length shouldBe 3
    ips should contain ("77.241.224.111")
    ips should contain ("2.148.3.1")
    ips should contain ("2.148.3.42")
  }

  def openAccessLog(path: String): RDD[AccessLogRecord] = {
    sc.textFile(path).map(AccessLogParser.parseRecord)
  }
}
