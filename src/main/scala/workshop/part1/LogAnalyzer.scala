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

  def countNumberOfInfoAndErrorLines(lines: RDD[String]): (Long, Long) = {
    val counted = lines.filter(l => isInfo(l) || isError(l))
      .map(l => {
        if (isInfo(l)) ("INFO", 1)
        else ("ERROR", 1)
       })
      .reduceByKey((r, c) => r + c)
      .sortByKey(ascending = false)
      .map(_._2)
      .collect()
    
    counted match {
      case Array(countInfo, countError) => (countInfo, countError)
    }
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
