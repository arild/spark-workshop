package workshop.part1

import org.apache.spark.rdd.RDD
import org.scalatest._
import workshop.parser.{AccessLogParser, AccessLogRecord}
import workshop.util.SparkTestUtils

class LogAnalyzerTest extends SparkTestUtils with Matchers {

  val ACCESS_LOG_1 = "src/test/resources/access_log-1"
  val ACCESS_LOG_2 = "src/test/resources/access_log-2"

  sparkTest("count number of status codes") {
    LogAnalyzer.countNumberOfStatusCode(openAccessLog(ACCESS_LOG_1), "200") should be (6)
    LogAnalyzer.countNumberOfStatusCode(openAccessLog(ACCESS_LOG_1), "404") should be (5)
  }

  sparkTest("collect distinct ip addresses for status code") {
    val logLines = LogAnalyzer.collectDistinctIpAddressesForStatusCode(openAccessLog(ACCESS_LOG_1), "200")
    
    logLines.length should be (4)
    logLines contains "64.242.88.10"
  }
  
  sparkTest("count all distinct ip addresses for status code") {
    val numLines = LogAnalyzer.countAllDistinctIpAddressesForStatusCode(
      openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2), "200")

    numLines should be (6)
  }

  sparkTest("find num ip addresses occurring in both logs") {
    val num = LogAnalyzer.findNumIpAddressesOccurringInBothLogs(openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2))

    num should be (3)
  }

  sparkTest("find num ip addresses occurring more than once") {
    val num = LogAnalyzer.findNumIpAddressesOccurringMoreThanOnce(openAccessLog(ACCESS_LOG_1), openAccessLog(ACCESS_LOG_2))

    num should be (10)
  }

  sparkTest("find three most frequent ip addresses") {
    val ips = LogAnalyzer.findThreeMostFrequentIpAddresses(openAccessLog(ACCESS_LOG_1))

    ips.length should be (3)
    ips should contain ("77.241.224.111")
    ips should contain ("2.148.3.1")
    ips should contain ("2.148.3.42")
  }

  def openAccessLog(path: String): RDD[AccessLogRecord] = {
    val parser: AccessLogParser = new AccessLogParser()
    sc.textFile(path).map( line => {
      parser.parseRecordReturningNullObjectOnFailure(line)
    })
  }
}
