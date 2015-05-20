package workshop.part1

import org.apache.spark.rdd.RDD
import workshop.util.parser.AccessLogRecord


object LogAnalyzer {

  def countNumberOfStatusCode(log: RDD[AccessLogRecord], statusCode: Int): Long = ???

  def collectDistinctIpAddressesForStatusCode(log: RDD[AccessLogRecord], statusCode: Int): Array[String] = ???

  def countAllDistinctIpAddressesForStatusCode(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord], statusCode: Int): Long = ???

  def findNumIpAddressesOccurringInBothLogs(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord]): Long = ???

  def findNumIpAddressesOccurringMoreThanOnce(log1: RDD[AccessLogRecord], log2: RDD[AccessLogRecord]): Long = ???

  def findThreeMostFrequentIpAddresses(log1: RDD[AccessLogRecord]): Array[String] = ???
}
