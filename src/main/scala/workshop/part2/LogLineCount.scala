package workshop.part2

object LogLineCount {

  case class LogLine(level: String, message: String)
  val logPattern = """\[(\w+)\](.*)""".r

  def parseLogLine(logLine: String): Option[LogLine] = logLine match {
    case logPattern(level, msg) => Some(LogLine(level, msg))
    case _ => None
  }

}
