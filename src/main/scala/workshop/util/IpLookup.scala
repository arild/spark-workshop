package workshop.util

import java.io.FileInputStream
import java.net.InetAddress

import com.maxmind.geoip2.DatabaseReader
import com.maxmind.geoip2.DatabaseReader.Builder
import com.maxmind.geoip2.model.CountryResponse

import scala.util.Try

object IpLookup extends App {

  lazy val geoip: DatabaseReader = createDb

  def getCountryForIp(ip: String): Option[String] = {
    val inetAddress: Option[InetAddress] = getInetAddress(ip)
    inetAddress.flatMap(x => test(geoip, x)).map(x => x.getCountry.getName)
  }

  private def createDb: DatabaseReader = {
    val stream: FileInputStream = new FileInputStream("GeoLite2-Country.mmdb")
    val geoip: DatabaseReader = new Builder(stream).build()
    geoip
  }

  private def test(geoip: DatabaseReader, ip: InetAddress): Option[CountryResponse] = {
    Try{ geoip.country(ip) }.toOption
  }

  private def getInetAddress(address: String) = {
    // Some people, when confronted with a problem, think "I know, I'll use regular expressions." Now they have two problems.
    val validNum = """(25[0-5]|2[0-4][0-9]|1[0-9]{2}|[1-9][0-9]|[0-9])"""
    val dot = """\."""
    val validIP = (validNum + dot + validNum + dot + validNum + dot + validNum).r

    try {
      address match {
        case validIP(_, _, _, _) => Some(InetAddress.getByAddress(address.split('.').map(_.toInt.toByte)))
        case _                   => Some(InetAddress.getByName(address))
      }
    } catch { // if all fails...
      case _ : Exception => None
    }
  }
}
