package workshop

import java.io.FileWriter

import org.joda.time.DateTime

import scala.io.Source
import scala.util.Random

case class LogEntry(ip:String, method: String, responseCode: Int, path:String, length: Int) {
  val date  = DateTime.now().toString("dd/MMM/yyyy:HH:mm:ss Z")
  override def toString:String = {
    s"""$ip - - [$date] "$method $path HTTP/1.1" $responseCode $length """
  }
}

object AccessLogGenerator extends App {

  val rand = new Random()

  val outputFile: String = "target/access.log"
  val ipAddresses = Source.fromInputStream(getClass.getResourceAsStream("/ip_addresses.txt")).getLines()
    .toSeq.filter(s => !s.startsWith("#") && s.trim.nonEmpty)
  val methods = Seq("GET", "POST", "PUT")
  val paths = Seq("/index.html", "/api/users", "/api/posts")

  var run = true
  println(s"Generation logs to: $outputFile ")

  while (run) {
    val res = createResponse()
    val logFile = new FileWriter(outputFile, true)
    logFile.append( res.toString + "\n")
    logFile.close()
    Thread.sleep(rand.nextInt(1000))
  }

  def randomInSeq(elements:Seq[String]): String = {
    elements(rand.nextInt(elements.length))
  }

  def createResponse():LogEntry = {
    val path: String = randomInSeq(paths)
    val method = if (path == paths.head) methods.head else randomInSeq(methods)
    LogEntry(randomInSeq(ipAddresses), method, 200, path, rand.nextInt(12000))
  }

}

