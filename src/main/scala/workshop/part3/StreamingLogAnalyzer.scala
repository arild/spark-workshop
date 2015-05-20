package workshop.part3


import org.apache.log4j.{Level, Logger}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import workshop.util.parser.AccessLogParser

object StreamingLogAnalyzer extends App {

  val conf = new SparkConf().setAppName("Streaming log analyzer").setMaster("local[2]")
  val ssc = new StreamingContext(new SparkContext(conf), Seconds(5))

  ssc.socketTextStream("localhost", 8000, StorageLevel.MEMORY_AND_DISK)
    .map(AccessLogParser.parseRecord)
    .window(Seconds(60), Seconds(5))
    .foreachRDD(lr => {
      if (lr.count() == 0) {
        println("No data")
      } else {
        val http200 = lr.filter(_.status == 200).count()
        val http500 = lr.filter(_.status == 500).count()
        val http404 = lr.filter(_.status == 404).count()

        println(s"Data: ${lr.count()}\n200: $http200\n404: $http404\n500: $http500")
        println("---------------")
    }
  })
  Logger.getRootLogger.setLevel(Level.WARN)
  ssc.start()
  ssc.awaitTermination()
}
