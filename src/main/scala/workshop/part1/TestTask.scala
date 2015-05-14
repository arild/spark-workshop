package workshop.part1

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import workshop.util.IpLookup


object TestTask {

  def countNumberOfErrors(file: RDD[String]): Long = {
    val woot: RDD[(String, Option[String])] = file.map(x => parseFromLogLine(x)).map(x => (x, IpLookup.getCountryForIp(x)))
    val map: RDD[String] = woot.map(x => x._2)
      .flatMap(x => x)
    map.map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .foreach(println)
0
  }

  private val patternStrng: String = "^(\\S+)"

  private val pattern = Pattern.compile(patternStrng)

  def parseFromLogLine(logline: String): String = {
    val m: Matcher = pattern.matcher(logline)

    if (!m.find()) {
      throw new RuntimeException("Error parsing logline" + logline)
    }

    m.group(1)
  }

}
