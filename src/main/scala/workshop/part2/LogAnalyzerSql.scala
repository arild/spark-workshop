package workshop.part2

import org.apache.spark.sql.{DataFrame, Row}

object LogAnalyzerSql {

  case class LogLine(level: String, message: String)
  val logPattern = """(\w+) - (.*)""".r

  def parseLogLine(logLine: String): Option[LogLine] = logLine match {
    case logPattern(level, msg) => Some(LogLine(level, msg))
    case _ => None
  }

  def countNumberOfErrors(df: DataFrame): Long = {
    val res: Array[Row] = df.sqlContext.sql("SELECT count(*) FROM loglines WHERE level='ERROR'").collect()
    res.head.getLong(0)
  }

  def collectDistinctInfoLines(df: DataFrame): Array[String] = {
    df.sqlContext.sql("SELECT DISTINCT message FROM loglines WHERE level='INFO'")
      .collect()
      .map(row => row.toString())
  }
}
