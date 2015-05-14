package workshop.part1

import org.apache.spark.rdd.RDD
import workshop.util.parser.AccessLogRecord


object LogAnalyzer {

  def countNumberOfStatusCode(log: RDD[AccessLogRecord], statusCode: String): Long = {
    log.filter(_.status == statusCode).count()
  }

  def collectDistinctIpAddressesForStatusCode(log: RDD[AccessLogRecord], statusCode: String): Array[String] = {
    log.filter(_.status == statusCode)
      .map(_.ipAddress)
      .distinct()
      .collect()
  }

  def countAllDistinctIpAddressesForStatusCode(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord], statusCode: String): Long = {
    log1.union(log2)
      .filter(_.status == statusCode)
      .map(_.ipAddress)
      .distinct()
      .count()
  }

  def findNumIpAddressesOccurringInBothLogs(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord]): Long = {
    log1.map(_.status)
      .intersection(log2.map(_.status))
      .count()
  }

  def findNumIpAddressesOccurringMoreThanOnce(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord]): Long = {
    log1.union(log2)
      .map(_.ipAddress)
      .map(ip => (ip, 1))
      .reduceByKey(_ + _)
      .filter { case (ip, count) => count > 1 }
      .count()
  }

  def findThreeMostFrequentIpAddresses(log: RDD[AccessLogRecord]): Array[String] = {
    log.map(_.ipAddress)
      .map(ip => (ip, 1))
      .reduceByKey(_ + _)
      .map { tuple => tuple.swap }
      .sortByKey(ascending = false)
      .take(3)
      .map { case (_, ip) => ip }
  }
}
