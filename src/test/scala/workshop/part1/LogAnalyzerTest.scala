package workshop.part1

import org.apache.spark.rdd.RDD
import org.scalatest._
import workshop.util.SparkTestUtils

class LogAnalyzerTest extends SparkTestUtils with Matchers {

  val APP_LOG_1 = "src/test/resources/application-1.log"
  val APP_LOG_2 = "src/test/resources/application-2.log"
   
  sparkTest("count number of error lines") {
    LogAnalyzer.countNumberOfErrors(openAppLog(APP_LOG_1)) should be (2)
  }

  sparkTest("collect distinct info lines") {
    val logLines = LogAnalyzer.collectDistinctInfoLines(openAppLog(APP_LOG_1))
    
    logLines.size should be(6)
    logLines contains "INFO - User id=23 successful login"
  }
  
  sparkTest("count all distinct info lines") {
    val numLines = LogAnalyzer.countAllDistinctInfoLines(openAppLog(APP_LOG_1), openAppLog(APP_LOG_2))

    numLines should be (7)
  }

  sparkTest("find num words occurring in both files") {
    val num = LogAnalyzer.findNumWordsOccurringInBothFiles(openAppLog(APP_LOG_1), openAppLog(APP_LOG_2))

    num should be (10)
  }

  sparkTest("find num words occurring more than once") {
    val num = LogAnalyzer.findNumWordsOccurringMoreThanOnce(openAppLog(APP_LOG_1), openAppLog(APP_LOG_2))

    num should be (16)
  }

  sparkTest("find three most frequent words") {
    val words = LogAnalyzer.findThreeMostFrequentWords(openAppLog(APP_LOG_1))

    words should contain ("-")
    words should contain ("INFO")
    words should contain ("User")
  }

  sparkTest("find first line of longest exception") {
    val line = LogAnalyzer.findFirstLineOfLongestException(openAppLog(APP_LOG_1), openAppLog(APP_LOG_2))
    
    line should be ("ERROR - java.sql.SQLException: Io exception: The Network Adapter could not establish the connection")
  }

  def openAppLog(path: String): RDD[String] = {
    sc.textFile(path)
  }
}
