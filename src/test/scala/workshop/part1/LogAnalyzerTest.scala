package workshop.part1

import org.apache.spark.rdd.RDD
import org.scalatest._
import workshop.util.SparkTestUtils

class LogAnalyzerTest extends SparkTestUtils with Matchers {

  sparkTest("count number of error lines") {
    LogAnalyzer.countNumberOfErrorLines(openLogFile) should be (2)
  }

  def openLogFile: RDD[String] = {
    sc.textFile("src/test/resources/application.log")
  }
}
