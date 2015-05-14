package workshop

import java.io.OutputStream
import java.net.{SocketException, ServerSocket, Socket}

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

  val serverSocket: ServerSocket = new ServerSocket(1337)
  var accept: Socket = serverSocket.accept()
  println("Connected to " + accept.getInetAddress)
  val os: OutputStream = accept.getOutputStream
  while (run) {
    val res = createResponse()
    val logEntry = res.toString + "\n"
    try {
      os.write(logEntry.getBytes("UTF-8"))
    } catch {
      case se: SocketException => {
        println("Waiting for new connection: " + se.getMessage)
        accept = serverSocket.accept()
        println("Connected to " + accept.getInetAddress)
      }
    }
    Thread.sleep(rand.nextInt(100))
  }

  def randomInSeq[T](elements:Seq[T]): T = {
    elements(rand.nextInt(elements.length))
  }

  def createStatusCode(): Int = {
    rand.nextInt(100) match {
      case r if 0 until 70 contains r => 200
      case r if 70 until 80 contains r => 302
      case r if 80 until 95 contains r => 404
      case _ => 500
    }
  }

  def createResponse():LogEntry = {
    val path: String = randomInSeq(paths)
    val method = if (path == paths.head) methods.head else randomInSeq(methods)

    LogEntry(randomInSeq(ipAddresses), method, createStatusCode(), path, rand.nextInt(12000))
  }

}

