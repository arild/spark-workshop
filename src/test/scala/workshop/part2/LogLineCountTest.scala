package workshop.part2

import org.apache.spark.sql.SchemaRDD
import org.scalatest._
import workshop.util.SparkTestUtils

class LogLineCountTest extends SparkTestUtils with Matchers {

  sparkTest("count number of lines grouped by log level") {
    openLogFile should be (2)
  }

  def openLogFile: Long = {

    val sqlContext = new org.apache.spark.sql.SQLContext(sc)
    import sqlContext.createSchemaRDD

    val logLines = sc.textFile("src/test/resources/application.log")
      .flatMap(LogLineCount.parseLogLine)
    logLines.registerTempTable("loglines")

    val sql: SchemaRDD = sqlContext.sql("select level, count(*) from loglines group by level order by 2 desc")
    val a: Array[Long] = sql.filter(x => x.getString(0) == "ERROR").map(x => x.getLong(1)).collect()
    a(0)
  }

}
