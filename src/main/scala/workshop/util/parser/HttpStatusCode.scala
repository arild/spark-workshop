package workshop.util.parser

case class HttpStatusCode(
    status: Int,         // 200, 404, etc.
    description: String, // "OK", "Not Found", etc.
    status_type: String  // "Successful", "Client Error", etc.
)
