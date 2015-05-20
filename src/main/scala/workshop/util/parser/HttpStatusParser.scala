package workshop.util.parser

object HttpStatusParser {
  def parseRecord(record: String): HttpStatusCode = {
    val x = record.split(",").map(_.trim)
    HttpStatusCode(x(0).toInt, x(1), x(2))
  }
}
