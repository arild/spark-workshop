package workshop.part1

import org.apache.spark.rdd.RDD

object LogAnalyzer {
  
  def countNumberOfErrorLines(lines: RDD[String]) = {
    lines.filter(line => line.contains("ERROR")).count()
  }

}
