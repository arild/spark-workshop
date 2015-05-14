package workshop

import org.apache.spark.rdd.RDD
import org.scalatest.Matchers
import workshop.part1.TestTask
import workshop.util.SparkTestUtils

class Experiment1 extends SparkTestUtils with Matchers {

  sparkTest("something") {
    val file: RDD[String] = sc.textFile("access_log")
    println(TestTask.countNumberOfErrors(file))
  }

}
