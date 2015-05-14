package workshop.part3


import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{StreamingContext, Seconds}
import org.apache.spark.{SparkConf, SparkContext}
import workshop.util.parser.AccessLogParser

object StreamingLogAnalyzer extends App {

  val conf = new SparkConf().setAppName("Streaming log analyzer").setMaster("local[2]")
  val sc = new SparkContext(conf)

  val ssc = new StreamingContext(sc, Seconds(10))

  val stream = ssc.socketTextStream("localhost", 1337, StorageLevel.MEMORY_AND_DISK)

  val parser: AccessLogParser = new AccessLogParser()
  val logEntries = stream.map(parser.parseRecordReturningNullObjectOnFailure)


  val window = logEntries.window(Seconds(10), Seconds(10))
  window.foreachRDD(lr => {
    if (lr.count() == 0) {
      println("No data")
    } else {
      val http200 = lr.filter(_.status == 200).count()
      val http500 = lr.filter(_.status == 500).count()
      val http404 = lr.filter(_.status == 404).count()

      println(s"Data: ${lr.count()}\n200: $http200\n404: $http404\n500: $http500")
    }
  })
  Logger.getRootLogger.setLevel(Level.WARN)
  ssc.start()
  ssc.awaitTermination()
}
