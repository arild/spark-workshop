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
    // TODO: do something cool!

  Logger.getRootLogger.setLevel(Level.WARN)
  ssc.start()
  ssc.awaitTermination()
}
