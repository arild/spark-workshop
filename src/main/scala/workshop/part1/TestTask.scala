package workshop.part1

import java.util.regex.{Matcher, Pattern}

import org.apache.spark.rdd.RDD
import workshop.util.IpLookup

object TestTask {

  def countNumberOfErrors(file: RDD[String]): Map[String, Int] = {
    file.map(x => parseIpFromLogline(x))
      .map(x => (x, IpLookup.getCountryForIp(x)))
      .map(x => x._2)
      .flatMap(x => x)
      .map(x => (x, 1))
      .reduceByKey((x, y) => x + y)
      .collect()
      .toMap
  }

  private val patternStrng: String = "^(\\S+)"

  private val pattern = Pattern.compile(patternStrng)

  def parseIpFromLogline(logline: String): String = {
    val m: Matcher = pattern.matcher(logline)

    if (!m.find()) {
      throw new RuntimeException("Error parsing logline" + logline)
    }

    m.group(1)
  }

}
