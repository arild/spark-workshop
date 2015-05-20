package workshop.util.parser

case class AccessLogRecord (
    ipAddress: String,
    rfc1413ClientIdentity: String,  // typically `-`
    remoteUser: String,             // typically `-`
    dateTime: String,               // [day/month/year:hour:minute:second zone]
    request: String,                // `GET /foo ...`
    status: Int,                    // 200, 404, etc.
    bytesSent: Int
)















