package workshop.part1

import org.apache.spark.rdd.RDD
import workshop.util.parser.AccessLogRecord


object LogAnalyzer {

  def countNumberOfStatusCode(log: RDD[AccessLogRecord], statusCode: Int): Long = {
    log.filter(_.status == statusCode).count()
  }

  def collectDistinctIpAddressesForStatusCode(log: RDD[AccessLogRecord], statusCode: Int): Array[String] = {
    log.filter(_.status == statusCode)
      .map(_.ipAddress)
      .distinct()
      .collect()
  }

  def countAllDistinctIpAddressesForStatusCode(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord], statusCode: Int): Long = {
    log1.union(log2)
      .filter(_.status == statusCode)
      .map(_.ipAddress)
      .distinct()
      .count()
  }

  def findNumIpAddressesOccurringInBothLogs(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord]): Long = {
    log1.map(_.ipAddress)
      .intersection(log2.map(_.ipAddress))
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

  def findThreeMostFrequentIpAddresses(log1: RDD[AccessLogRecord]): Array[String] = {
    log1.map(_.ipAddress)
      .map(ip => (ip, 1))
      .reduceByKey(_ + _)
      .map { tuple => tuple.swap }
      .sortByKey(ascending = false)
      .take(3)
      .map { case (_, ip) => ip }
  }
}
