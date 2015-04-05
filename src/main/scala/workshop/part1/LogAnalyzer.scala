package workshop.part1

import org.apache.spark.rdd.RDD


object LogAnalyzer {

  def countNumberOfErrors(lines: RDD[String]): Long = {
    lines.filter(isError).count()
  }

  def collectUniqueInfoLines(lines: RDD[String]): Array[String] = {
    lines.filter(isInfo).distinct().collect()
  }

  def countAllUniqueInfoLines(lines1: RDD[String], lines2: RDD[String]): Long = {
    lines1.union(lines2)
      .distinct()
      .filter(isInfo)
      .count()
  }

  def findNumWordsOccurringInBothFiles(lines1: RDD[String], lines2: RDD[String]): Long = {
    toWords(lines1)
      .intersection(toWords(lines2))
      .count()
  }

  def findNumWordsOccurringMoreThanOnce(lines1: RDD[String], lines2: RDD[String]): Long = {
    toWords(lines1.union(lines2))
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .filter { case (word, count) => count > 1 }
      .count()
  }

  def findThreeMostFrequentWords(lines: RDD[String]): Array[String] = {
    toWords(lines)
      .map(word => (word, 1))
      .reduceByKey(_ + _)
      .map { tuple => tuple.swap }
      .sortByKey(ascending = false)
      .take(3)
      .map { case (_, word) => word }
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

  private def isInfo(line: String) = {
    line.startsWith("INFO - ")
  }

  private def isError(line: String) = {
    line.startsWith("ERROR - ")
  }

  private def toWords(lines: RDD[String]) = {
    lines.flatMap(line => line.split(" ")).filter(!_.isEmpty)
  }
}
