package workshop.part2

import org.apache.spark.sql.{DataFrame, Row}

object LogAnalyzerSql {
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
      """.stripMargin)

    (res.head.getString(0), res.head.getDouble(1))
  }

  def countLoglinesByStatuscodes(df: DataFrame): List[(Int, String, Long)] = {
    val res: Array[Row] = df.sqlContext.sql(
      """SELECT l.status, s.description, count(*) AS antall
         FROM logs l
         JOIN http_status s ON s.status = l.status
         GROUP BY l.status, s.description
    """).collect()
    res
      .map(x => (x.getString(0).toInt, x.getString(1), x.getLong(2)))
      .toList
  }
}
