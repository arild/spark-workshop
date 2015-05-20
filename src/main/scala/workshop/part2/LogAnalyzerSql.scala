package workshop.part2

import org.apache.spark.SparkContext
import org.apache.spark.sql.{DataFrame, Row, SQLContext}
import workshop.util.parser.{AccessLogParser, HttpStatusParser}

object LogAnalyzerSql {

  def createTables(sc: SparkContext, accessLog: String, httpStatus: String): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    sc.textFile(accessLog)
      .map(AccessLogParser.parseRecord)
      .toDF()
      .registerTempTable("logs")

    sc.textFile(httpStatus)
      .map(HttpStatusParser.parseRecord)
      .toDF()
      .registerTempTable("http_status")

    sqlContext.tables()
  }

  def countStatus200Loglines(df: DataFrame): Long = {
    val res: Array[Row] = df.sqlContext.sql(
      """SELECT count(*) AS antall
         FROM logs
         where status = 200
      """).collect()
    res.head.getLong(0)
  }

  def findThreeMostFrequentIpAddresses(df: DataFrame): Array[String] = {
    df.sqlContext.sql(
      """SELECT ipAddress, count(ipAddress) numIpAddresses
         FROM logs
         GROUP BY ipAddress
         ORDER BY numIpAddresses desc
      """.stripMargin).collect()
      .take(3)
      .map(row => row.getString(0))
  }

  def findRequestWithLargestAverageResponseSize(df: DataFrame): (String, Double) = {
    val res = df.sqlContext.sql(
      """SELECT request, avg(bytesSent) avg_bytes
         FROM logs
         GROUP BY request
         ORDER BY avg_bytes desc
      """)

    (res.head.getString(0), res.head.getDouble(1))
  }

  def findDescriptionForAllUniqueStatusCodes(df: DataFrame): Map[Int, String] = {
    df.sqlContext.sql(
      """SELECT DISTINCT l.status, s.description
         FROM logs l
         JOIN http_status s ON s.status = l.status
      """)
      .collect()
      .map(row => (row.getInt(0), row.getString(1)))
      .toMap
  }
}
