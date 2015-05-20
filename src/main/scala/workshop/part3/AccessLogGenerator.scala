package workshop.part3

import java.net.{ServerSocket, Socket, SocketException}

import org.joda.time.DateTime

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent._
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
  val ipAddresses = Source.fromInputStream(getClass.getResourceAsStream("/ip_addresses.txt")).getLines()
    .toSeq.filter(s => !s.startsWith("#") && s.trim.nonEmpty)
  val methods = Seq("GET", "POST", "PUT")
  val paths = Seq("/index.html", "/api/users", "/api/posts")

  val serverSocket: ServerSocket = new ServerSocket(8000)
  while (true) {
    val socket: Socket = serverSocket.accept()
    Future {
      handleConnection(socket)
    }
  }

  def handleConnection(socket: Socket) = {
    println("Connected to " + socket.getInetAddress)
    var running = true
    while (running) {
      val logEntry = createResponse().toString + "\n"
      try {
        socket.getOutputStream.write(logEntry.getBytes("UTF-8"))
      } catch {
        case se: SocketException => {
          socket.close()
          running = false
        }
      }
      Thread.sleep(rand.nextInt(100))
    }
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

