package workshop.part1

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext._

object LogAnalyzer {

  def countNumberOfErrors(lines: RDD[String]): Long = {
    lines.filter(isError).count()
  }

  def collectUniqueInfoLines(lines: RDD[String]): Array[String] = {
    lines.filter(isInfo).distinct().collect()
  }

  def countAllUniqueInfoLines(lines1: RDD[String], lines2: RDD[String]): Long = {
    lines1.union(lines2).distinct().filter(isInfo).count()
  }

  def findThreeMostFrequentWords(lines: RDD[String]): Array[String] = {
    lines.flatMap(line => line.split(" "))
      .filter(!_.isEmpty)
      .map(name => (name, 1))
      .reduceByKey(_ + _)
      .map { case (word, count) => (count, word) }
      .sortByKey(ascending = false)
      .take(3)
      .map { case (count, word) => word }
  }

  def findFirstLineOfLongestException(lines1: RDD[String], lines2: RDD[String]): String = {
    // Assume line starts with INFO or ERROR, otherwise it is part of stack trace
    val exceptions = lines1.union(lines2).filter(!isInfo(_))

    def partitionErrors(lines: List[String], result: List[List[String]] = List()): List[List[String]] = {
      lines match {
        case Nil => result
        case head :: tail => {
          val (ex, remaining) = tail.span(!isError(_))
          partitionErrors(remaining, (head :: ex) :: result)
        }
      }
    }

    val sorted = partitionErrors(exceptions.collect().toList).sortWith { (a, b) =>
      def numChars(lines: List[String]): Int = {
        lines.map(_.length).sum
      }
      numChars(a) > numChars(b)
    }

    sorted.head.head
  }

  private def isError(line: String) = {
    line.startsWith("ERROR - ")
  }

  private def isInfo(line: String) = {
    line.startsWith("INFO - ")
  }
}
