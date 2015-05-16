package workshop.util.parser

/**
 * @see http://httpd.apache.org/docs/2.2/logs.html for details
 */
case class AccessLogRecord (
    ipAddress: String,         // should be an ip address, but may also be the hostname if hostname-lookups are enabled
    rfc1413ClientIdentity: String,   // typically `-`
    remoteUser: String,              // typically `-`
    dateTime: String,                // [day/month/year:hour:minute:second zone]
    request: String,                 // `GET /foo ...`
    status: Int,                     // 200, 404, etc.
    bytesSent: Int                // may be `-`
)















