package workshop.part2

import org.apache.spark.sql.{DataFrame, Row}

object LogAnalyzerSql {
  def sumBytesPerRequest(df: DataFrame): Map[String, Double] = {

    df.sqlContext.sql(
      """select request, avg(bytesSent), sum(bytesSent) sum_bytes from logs group by request order by sum_bytes desc
      """.stripMargin)
      .take(10)
      .foreach(println)
    df.show()

    Map.empty
  }

  def countStatus200Loglines(df: DataFrame): Long = {
    val res: Array[Row] = df.sqlContext.sql(
      """SELECT count(*) AS antall
         FROM logs
         where status = 200
      """).collect()
    res.head.getLong(0)
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

  def collectDistinctInfoLines(df: DataFrame): Array[String] = {
    df.sqlContext.sql("SELECT DISTINCT message FROM loglines WHERE level='INFO'")
      .collect()
      .map(row => row.toString())
  }

}
