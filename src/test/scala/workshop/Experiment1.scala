package workshop

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{SQLContext, DataFrame}
import org.scalatest.Matchers
import workshop.part2.LogAnalyzerSql2
import workshop.util.SparkTestUtils
import workshop.util.parser.{HttpStatusCode, AccessLogParser}

class Experiment1 extends SparkTestUtils with Matchers {

  sparkTest("something") {
    val file: RDD[String] = sc.textFile("access_log")
    val dataFrame: DataFrame = openLogFile(file)
    println(LogAnalyzerSql2.countStatus200Loglines(dataFrame))
    println(LogAnalyzerSql2.countLoglinesByStatuscodes(dataFrame))
    println(LogAnalyzerSql2.sumBytesPerRequest(dataFrame))
  }

  def openLogFile(rdd: RDD[String]): DataFrame = {
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._

    val parser: AccessLogParser = new AccessLogParser()

    openHttpStatusFile(sqlContext)

    val df = rdd
      .map(line => parser.parseRecordReturningNullObjectOnFailure(line))
      .toDF()
    df.registerTempTable("logs")
    df
  }

  def openHttpStatusFile(sqlContext: SQLContext): Unit = {
    import sqlContext.implicits._

    val lines: RDD[Array[String]] = sc.textFile("src/test/resources/http_status_codes.csv")
      .map(line => line.split(",").map(_.trim))

    val header: Array[String] = lines.first()
    val data: RDD[Array[String]] = lines.filter(_(0) != header(0))
    data
      .map(x => HttpStatusCode(x(0).toInt, x(1), x(2)))
      .toDF()
      .registerTempTable("http_status")
  }
}
