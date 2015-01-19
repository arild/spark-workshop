package workshop.part1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
import org.scalatest._

class LogAnalyzerTest extends FlatSpec with Matchers {

  it should "count number of error lines" in {
    LogAnalyzer.countNumberOfErrorLines(openLogFile) should be (2)
  }

  def openLogFile: RDD[String] = {
    val sparkConf = new SparkConf().setAppName("LogAnalyzer").setMaster("local[2]")
    new SparkContext(sparkConf).textFile("src/test/resources/application.log")
  }
}
