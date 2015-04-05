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

  sparkTest("collect unique info lines") {
    val logLines = LogAnalyzer.collectUniqueInfoLines(openAppLog(APP_LOG_1))
    
    logLines.size should be(6)
    logLines contains "INFO - User id=23 successful login"
  }
  
  sparkTest("count all unique info lines") {
    val numLines = LogAnalyzer.countAllUniqueInfoLines(openAppLog(APP_LOG_1), openAppLog(APP_LOG_2))

    numLines should be (7)
  }
  
  sparkTest("find theree most frequent words") {
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
