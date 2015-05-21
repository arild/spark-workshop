package workshop.part2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import workshop.util.parser.{AccessLogParser, HttpStatusParser}


object LogAnalyzerSql {

  def createTables(sc: SparkContext, accessLog: String, httpStatus: String): DataFrame = ???

  def countStatus200Loglines(df: DataFrame): Long = ???

  def findThreeMostFrequentIpAddresses(df: DataFrame): Array[String] = ???

  def findRequestWithLargestAverageResponseSize(df: DataFrame): (String, Double) = ???

  def findDescriptionForAllUniqueStatusCodes(df: DataFrame): Map[Int, String] = ???
}
